# //// Neoffice — added file (no upstream equivalent in version-15): backport of the frappe
# //// develop (v16) Chrome PDF generator, unchanged from upstream. Lineage (cherry-picked
# //// with -x): 964dd6c034 "feat: Chrome PDF generator" (ours c64ffb849d) + d870caf6c4 (#35098,
# //// ours 153d09abb5). Why the backport: see the browser.py header (wkhtmltopdf cannot render
# //// the Oslo print formats; ADR 2026-05-26). Neoffice changes in this file: wait_for_event()
# //// and wait_for_event_or_throw() — see the markers on each.
# //// v16 note: upstream moved this module to frappe/utils/chromium/cdp_connection.py
# //// (182e127732 "refactor: extract generic headless-Chromium stack", 2026-06-18).
import asyncio

import websockets

import frappe


class CDPSocketClient:
	"""
	Manages WebSocket communications with Chrome DevTools Protocol.
	Ensures robust error handling and consistent logging.
	"""

	def __init__(self, websocket_url):
		self.websocket_url = websocket_url
		self.connection = None
		self.message_id = 0
		self.pending_messages = {}
		self.listeners = {}
		self.listen_task = None
		self.loop = asyncio.new_event_loop()
		asyncio.set_event_loop(self.loop)

	def connect(self):
		"""Open the WebSocket connection and start listening for messages."""
		self.loop.run_until_complete(self._connect())
		self.listen_task = self.loop.create_task(self._listen())

	async def _connect(self):
		try:
			self.connection = await websockets.connect(self.websocket_url)
		except Exception:
			frappe.log_error(title="Failed to connect to WebSocket:", message=f"{frappe.get_traceback()}")
			raise

	async def _listen(self):
		try:
			async for message in self.connection:
				self._handle_message(frappe.json.loads(message))
		except Exception:
			frappe.log_error(title="WebSocket listening error:", message=f"{frappe.get_traceback()}")

	def _handle_message(self, response):
		method = response.get("method")
		params = response.get("params", {})
		session_id = response.get("sessionId")
		target_id = params.get("targetId")
		frame_id = params.get("frameId")
		message_id = response.get("id")

		composite_key = (method, session_id, target_id, frame_id)

		# Handle responses with `id`
		if message_id and message_id in self.pending_messages:
			future = self.pending_messages.pop(message_id)
			if composite_key in self.pending_messages:
				self.pending_messages.pop(composite_key)
			future.set_result(response)

		# Handle responses without `id` using a composite key
		elif method:
			if composite_key in self.pending_messages:
				# print("matched using composite_key", composite_key)
				future = self.pending_messages.pop(composite_key)
				future.set_result(response)

			if method in self.listeners:
				for callback, future, filters in self.listeners[method]:
					# added not filters["key"] might cause cross talk between different sessions
					if (
						(not session_id or not filters["sessionId"] or filters["sessionId"] == session_id)
						and (not target_id or not filters["targetId"] or filters["targetId"] == target_id)
						and (not frame_id or not filters["frameId"] or filters["frameId"] == frame_id)
					):
						callback(future, response)

	def disconnect(self):
		try:
			if self.listen_task:
				self.listen_task.cancel()
			self.loop.run_until_complete(self._disconnect())
			# Cancel all pending tasks before stopping the loop was causing degrading performance over time to not cancelled properly
			pending_tasks = [task for task in asyncio.all_tasks(self.loop) if not task.done()]
			for task in pending_tasks:
				task.cancel()
				try:
					self.loop.run_until_complete(task)  # Ensure tasks finish before loop stops
				except asyncio.CancelledError:
					pass  # Ignore cancellation errors
		except Exception:
			frappe.log_error(title="Error while disconnecting:", message=f"{frappe.get_traceback()}")
			raise

	async def _disconnect(self):
		try:
			if self.connection:
				await self.connection.close()
			self.connection = None
		except Exception:
			frappe.log_error(
				title="Error during WebSocket disconnection:", message=f"{frappe.get_traceback()}"
			)

	def send(self, method, params=None, session_id=None, return_future=False):
		if return_future:
			return asyncio.ensure_future(
				self._send(method, params, session_id, wait_future_fulfill=False), loop=self.loop
			)
		future = self.loop.run_until_complete(self._send(method, params, session_id))
		return self._destructure_response(future.result())

	async def _send(self, method, params=None, session_id=None, wait_future_fulfill=True):
		self.message_id += 1
		message_id = self.message_id
		message = {
			"id": message_id,
			"method": method,
			"params": params or {},
		}

		if session_id:
			message["sessionId"] = session_id

		if self.connection is None:
			raise RuntimeError("WebSocket connection is not open.")

		future = asyncio.Future()
		self.pending_messages[message_id] = future

		# Dynamically create the composite key
		if any(
			[
				method,
				session_id,
				params.get("targetId") if params else None,
				params.get("frameId") if params else None,
			]
		):
			composite_key = (
				method,
				session_id,
				params.get("targetId") if params else None,
				params.get("frameId") if params else None,
			)
			self.pending_messages[composite_key] = future

		await self.connection.send(frappe.json.dumps(message))
		if wait_future_fulfill:
			await future
		return future

	def _destructure_response(self, response):
		"""Destructure the response to extract useful information."""
		result = response.get("result", None)
		error = response.get("error", None)
		return result, error

	def start_listener(self, method, callback, session_id=None, target_id=None, frame_id=None):
		"""Register a listener for a specific CDP event with optional filtering."""
		if method not in self.listeners:
			self.listeners[method] = []
		future = self.loop.create_future()

		event = (callback, future, {"sessionId": session_id, "targetId": target_id, "frameId": frame_id})
		if event not in self.listeners[method]:
			self.listeners[method].append(event)
		return event

	# //// Neoffice — returns a bool, and the default timeout goes 3 s -> 15 s.
	# //// Upstream returned nothing and swallowed the TimeoutError, but asyncio.wait_for
	# //// CANCELS the future when it expires: the caller then called .result() on that
	# //// cancelled future and a bare CancelledError surfaced as an HTTP 500 on the print
	# //// preview, with a traceback naming asyncio instead of the real cause. 3 s was also
	# //// below the measured p95 of a render on a loaded 2-vCPU instance (3.58 s on
	# //// a loaded client instance), so roughly 5 % of previews failed by construction.
	# //// Callers now branch on the return value instead of touching a cancelled future.
	# //// Remove when upstream ships the Chrome generator on v15 with its own fix.
	def wait_for_event(self, event, timeout=15):
		"""Wait for a CDP event. Returns True if it arrived, False if it timed out."""
		if type(event) is tuple:
			event = event[1]
		try:
			self.loop.run_until_complete(asyncio.wait_for(event, timeout))
			# //// Neoffice — see block marker above: return bool instead of raising
			return True
		except (asyncio.TimeoutError, asyncio.CancelledError):
			frappe.log_error(title="Timeout waiting for event", message=f"{frappe.get_traceback()}")
			# //// Neoffice — see block marker above: caller branches on False, no cancelled future
			return False

	# //// Neoffice — added. wait_for_event() reports a timeout by returning False, and by
	# //// then asyncio.wait_for has CANCELLED the future: any caller that reads the value
	# //// afterwards gets a bare CancelledError, surfaced as an HTTP 500 with a traceback
	# //// naming asyncio and not the real cause. Every caller that needs the value goes
	# //// through here, so there is ONE way to wait and ONE message — a guard copied per
	# //// call site is how the first fix left get_pdf_stream_id open (see #291).
	# //// Remove when upstream ships the Chrome generator on v15 with its own fix.
	def wait_for_event_or_throw(self, event, timeout=15, cleanup=None):
		"""Wait for a CDP event; raise a user-facing error naming the timeout if it never came."""
		if self.wait_for_event(event, timeout):
			return
		if cleanup:
			cleanup()
		frappe.throw(
			frappe._("The PDF engine did not respond in time. Please try again."),
			title=frappe._("PDF generation timed out"),
		)

	def remove_listener(self, method, event):
		"""Remove a listener for a specific CDP event."""
		self.listeners[method].remove(event)
