import atexit
import os
import platform
import subprocess
import time
from pathlib import Path
from typing import ClassVar

import requests

import frappe
from frappe import _
from frappe.utils.data import cint
from frappe.utils.print_utils import find_or_download_chromium_executable


class ChromePDFGenerator:
	_instance = None

	_browsers: ClassVar[list] = []

	def add_browser(self, browser):
		self._browsers.append(browser)

	def remove_browser(self, browser):
		self._browsers.remove(browser)

	def __new__(cls):
		inst = cls._instance
		# Shared external Chromium (systemd / remote): no owned process to check,
		# keep reusing the singleton.
		if inst is not None and getattr(inst, "_remote_mode", False):
			return inst
		# Local-spawn mode: only reuse the singleton if the Chromium process we
		# started is STILL ALIVE. A crashed/OOM-killed process leaves a truthy
		# Popen object but poll() returns non-None — without this check the stale
		# instance (and its dead DevTools URL) gets reused → ConnectionRefused.
		if inst is None or not inst._chromium_process or inst._chromium_process.poll() is not None:
			cls._instance = super().__new__(cls)
		return cls._instance

	def __init__(self):
		"""Initialize only once."""
		if hasattr(self, "_initialized"):  # Prevent multiple initializations
			return
		self._initialized = True  # Mark as initialized

		self._chromium_process = None
		self._chromium_path = None
		self._devtools_url = None
		self._remote_mode = False
		self._remote_port = 0
		self._initialize_chromium()

	def _initialize_chromium(self):
		# ideally browser is initailized from before request hook.
		# if _chromium_process is not available then initialize it.
		if self._chromium_process:
			return
		# get site config and load chromium settings.
		site_config = frappe.get_common_site_config()

		# only when we want to chromium on separate docker / server ( not implemented/tested yet )
		self.CHROMIUM_WEBSOCKET_URL = site_config.get("chromium_websocket_url", "")
		if self.CHROMIUM_WEBSOCKET_URL:
			frappe.warn("Using external chromium websocket url. Make sure it is accessible.")
			self._remote_mode = True
			self._devtools_url = self.CHROMIUM_WEBSOCKET_URL
			return

		# Shared Chromium managed by systemd on a fixed localhost debug port.
		# Every worker connects to the SAME instance instead of spawning its own
		# (which proliferated to ~1 Chromium per gunicorn worker → OOM/swap →
		# Chromium killed → stale singleton → ConnectionRefused). The live browser
		# WebSocket URL is fetched from /json/version (its GUID changes whenever
		# systemd restarts Chromium, so it is re-fetched on connection failure).
		self._remote_port = cint(site_config.get("chromium_remote_debugging_port", 0))
		if self._remote_port:
			self._remote_mode = True
			self._devtools_url = self.fetch_devtools_url(self._remote_port)
			return

		"""
		Number of allowed open websocket connections to chromium.
		This number will basically define how many concurrent requests can be handled by one chromium instance.
		#TODO: Implement/Modify logic to handle multiple chromium instance in one class / per worker. currently we are starting one chromium.
		"""
		self.CHROME_OPEN_CONNECTIONS = site_config.get("chromium_max_concurrent", 1)
		# if we want to use persistent ( long running ) chromium for all sites.
		# current approch starts chrome per worker process.
		# TODO: Better Implement logic to support for persistent chrome proccess.
		self.USE_PERSISTENT_CHROMIUM = site_config.get("use_persistent_chromium", False)
		#  time to wait for chromium to start and provide dev tools url used in _set_devtools_url.
		self.START_TIMEOUT = site_config.get("chromium_start_timeout", 3)
		# Allow a single PDF request to opt into interactive Chromium debugging in developer mode only.
		self.debug_mode = frappe.conf.developer_mode and bool(frappe.form_dict.get("pdf_debug"))

		self._chromium_path = find_or_download_chromium_executable()
		if self._verify_chromium_installation():
			if not self._devtools_url:
				self.start_chromium_process(debug=self.debug_mode)
				# Ensure the Chromium process is terminated when the owning Python
				# process exits (bench console/execute, worker graceful shutdown).
				# Without this, headless_shell processes are orphaned and pile up,
				# eating RAM on small instances. _terminate_chromium is idempotent.
				atexit.register(self._terminate_chromium)

	def _verify_chromium_installation(self):
		"""Ensures Chromium is available and executable, raising clearer errors if not."""
		if not os.path.exists(self._chromium_path):
			frappe.throw(
				f"Chromium not available at the specified path. Please check the path: {self._chromium_path}"
			)
		if not os.access(self._chromium_path, os.X_OK):
			frappe.throw(f"Chromium not executable at {self._chromium_path}")
		return True

	def start_chromium_process(self, debug=False):
		"""
		Launches Chromium in headless mode with robust logging and error handling.
		chrome switches
		https://peter.sh/experiments/chromium-command-line-switches/

		NOTE: dbus issue in docker
		  https://source.chromium.org/chromium/chromium/src/+/main:content/app/content_main.cc;l=229-241?q=DBUS_SESSION_BUS_ADDRESS&ss=chromium
		"""
		try:
			if debug:
				command_args = [
					"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",  # path to locally installed chrome browser for debugging.
					"--remote-debugging-port=0",
					"--user-data-dir=/tmp/chromium-{}-user-data".format(
						frappe.local.site + frappe.utils.random_string(10)
					),
					"--disable-gpu",
					"--no-sandbox",
					"--no-first-run",
					"",
				]
			else:
				command_args = [
					self._chromium_path,
					# 0 will automatically select a random open port from the ephemeral port range.
					"--remote-debugging-port=0",
					"--disable-gpu",  # GPU is not available in production environment.
					"--disable-field-trial-config",
					"--disable-background-networking",
					"--disable-background-timer-throttling",
					"--disable-backgrounding-occluded-windows",
					"--disable-back-forward-cache",
					"--disable-breakpad",
					"--disable-client-side-phishing-detection",
					"--disable-component-extensions-with-background-pages",
					"--disable-component-update",
					"--no-default-browser-check",
					"--disable-default-apps",
					"--disable-dev-shm-usage",
					"--disable-extensions",
					"--disable-features=ImprovedCookieControls,LazyFrameLoading,GlobalMediaControls,DestroyProfileOnBrowserClose,MediaRouter,DialMediaRouteProvider,AcceptCHFrame,AutoExpandDetailsElement,CertificateTransparencyComponentUpdater,AvoidUnnecessaryBeforeUnloadCheckSync,Translate,HttpsUpgrades,PaintHolding,ThirdPartyStoragePartitioning,LensOverlay,PlzDedicatedWorker",
					"--allow-pre-commit-input",
					"--disable-hang-monitor",
					"--disable-ipc-flooding-protection",
					"--disable-popup-blocking",
					"--disable-prompt-on-repost",
					"--disable-renderer-backgrounding",
					"--force-color-profile=srgb",
					"--metrics-recording-only",
					"--no-first-run",
					"--password-store=basic",
					"--use-mock-keychain",
					"--no-service-autorun",
					"--export-tagged-pdf",
					"--disable-search-engine-choice-screen",
					"--unsafely-disable-devtools-self-xss-warnings",
					"--enable-use-zoom-for-dsf=false",
					"--use-angle",
					"--headless",
					"--hide-scrollbars",
					"--mute-audio",
					"--blink-settings=primaryHoverType=2,availableHoverTypes=2,primaryPointerType=4,availablePointerTypes=4",
					"--no-sandbox",
					"--no-startup-window",
					# related to HeadlessExperimental flag enable when Implement Deterministic rendering. check page class for more info.
					# "--enable-surface-synchronization",
					# "--run-all-compositor-stages-before-draw",
					# "--disable-threaded-animation",
					# "--disable-threaded-scrolling",
					# "--disable-checker-imaging",
				]

			self._start_chromium_process(command_args)

		except Exception as e:
			frappe.log_error(f"Error starting Chromium: {e}")
			frappe.throw(_("Could not start Chromium. Check logs for details."))

	# Apply the decorator to monitor Chromium subprocess usage for development / debugging purposes.
	# it will print and write usage data to a file ( defaults to chrome_process_usage.json).
	# from print_designer.pdf_generator.monitor_subprocess import monitor_subprocess_usage
	# @monitor_subprocess_usage(interval=0.1)
	def _start_chromium_process(self, command_args):
		if platform.system().lower() == "windows":
			# hide cmd window
			startupinfo = subprocess.STARTUPINFO()
			startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
			startupinfo.wShowWindow = subprocess.SW_HIDE
			self._chromium_process = subprocess.Popen(
				command_args,
				stdout=subprocess.PIPE,
				stderr=subprocess.PIPE,
				startupinfo=startupinfo,
				text=True,
			)
		else:
			self._chromium_process = subprocess.Popen(
				command_args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
			)
		return self._chromium_process

	def _set_devtools_url(self):
		"""
		Monitor Chromium's stderr for the DevTools WebSocket URL
		----------------
		other approch: if we choose port using find_available_port we can avoid this entirely and fetch_devtools_url() method.

		NOTE:	1) in current approch output to stderr is pretty consistent.
		                2) other approch may seem reliable but it is slow compared to this in testing.

		TODO:
		final approch can be decided later after testing in production.
		"""
		stderr = self._chromium_process.stderr
		start_time = time.time()

		while time.time() - start_time < self.START_TIMEOUT:
			# Read a single line from stderr and check if it contains the DevTools URL.
			# Not using select() because it is not supported on Windows for non-socket file descriptors.
			line = stderr.readline()
			# not sure if "DevTools listening on" is consistent in all chromium versions.
			if "DevTools listening on" in line:
				url_start = line.find("ws://")
				if url_start != -1:
					self._devtools_url = line[url_start:].strip()
					break

		if not self._devtools_url:
			self._chromium_process.terminate()
			raise TimeoutError("Chromium took too long to start.")

	def ensure_devtools_url(self, force=False):
		"""Return a usable DevTools WebSocket URL, (re)establishing it if needed.

		Called by Browser.open() so a single dead/restarted Chromium recovers
		instead of 500ing on a stale URL.

		- Remote mode (shared systemd Chromium / external): re-fetch the live
		  browser URL from /json/version. The GUID changes on every Chromium
		  restart, so `force=True` (after a failed connect) picks up the new one.
		- Local mode: if the owned process died, terminate the husk and start a
		  fresh Chromium, then read its URL from stderr.
		"""
		if self._remote_mode:
			if self._remote_port:
				# On-demand shared Chromium: start the systemd service if it is
				# down (it auto-stops when idle to free RAM), mark activity, and
				# always return a freshly-fetched URL (its GUID changes on restart).
				url = self._ensure_remote_service()
				if not url:
					frappe.throw(
						_("Chrome PDF: shared Chromium could not be started on the configured debug port.")
					)
				self._devtools_url = url
				return self._devtools_url
			# External full websocket URL mode (advanced).
			if force or not self._devtools_url:
				if not self.CHROMIUM_WEBSOCKET_URL:
					frappe.throw(_("Chrome PDF: external chromium websocket url is not reachable."))
				self._devtools_url = self.CHROMIUM_WEBSOCKET_URL
			return self._devtools_url

		# Local-spawn mode.
		process_dead = not self._chromium_process or self._chromium_process.poll() is not None
		if force or process_dead:
			self._terminate_chromium()
			self._chromium_process = None
			self._devtools_url = None
			self._initialize_chromium()
			self._set_devtools_url()
		elif not self._devtools_url:
			self._set_devtools_url()
		return self._devtools_url

	# Activity marker read by the chromium-pdf-idle systemd timer to decide when
	# the shared Chromium has been unused long enough to stop (free its RAM).
	ACTIVITY_FILE = os.path.expanduser("~/.cache/chromium-pdf/.last-activity")

	def _touch_activity(self):
		try:
			from pathlib import Path

			p = Path(self.ACTIVITY_FILE)
			p.parent.mkdir(parents=True, exist_ok=True)
			p.touch()
		except Exception:
			pass

	def _ensure_remote_service(self):
		"""Return a live DevTools URL for the shared Chromium, starting the
		on-demand systemd service if it is currently stopped (idle).

		The service auto-stops after inactivity to free ~300 MB; the first PDF
		after an idle period transparently restarts it (a scoped sudoers rule
		lets the worker run `systemctl start chromium-pdf.service`).
		"""
		url = self.fetch_devtools_url(self._remote_port)
		if url:
			self._touch_activity()
			return url
		# Endpoint down → start the service on demand, then poll for it.
		try:
			subprocess.run(
				["sudo", "-n", "/usr/bin/systemctl", "start", "chromium-pdf.service"],
				timeout=10,
				check=False,
				stdout=subprocess.DEVNULL,
				stderr=subprocess.DEVNULL,
			)
		except Exception as e:
			frappe.log_error("Chrome PDF: failed to start chromium-pdf.service", str(e))
		deadline = time.time() + 15  # Chromium cold start ~1-2s; extra margin under memory pressure
		while time.time() < deadline:
			url = self.fetch_devtools_url(self._remote_port)
			if url:
				self._touch_activity()
				return url
			time.sleep(0.25)
		# Auto-start + poll window elapsed and the endpoint is still down: a genuine
		# failure worth a single Error Log entry (not one per poll).
		frappe.log_error(
			"Chrome PDF DevTools unreachable",
			f"chromium-pdf.service did not expose a DevTools endpoint on "
			f"127.0.0.1:{self._remote_port} within 15s after an on-demand start.",
		)
		return None

	def _close_browser(self):
		"""
		Close the headless Chromium browser.
		"""
		if self._browsers:
			frappe.log("Cannot close Chromium as there are active browser instances.")
			return
		if self._chromium_process:
			self._chromium_process.terminate()
		ChromePDFGenerator._instance = None
		self._chromium_process = None
		self._devtools_url = None
		frappe.log("Headless Chromium closed successfully.")

	def _terminate_chromium(self):
		"""Force-terminate the Chromium process. Used as an atexit handler.

		Unlike _close_browser(), this ignores the active-browser bookkeeping:
		it runs when the owning Python process is shutting down, so any in-flight
		work is already gone. Safe to call multiple times (idempotent).
		"""
		proc = self._chromium_process
		if not proc:
			return
		try:
			if proc.poll() is None:  # still running
				proc.terminate()
				try:
					proc.wait(timeout=5)
				except subprocess.TimeoutExpired:
					proc.kill()
					proc.wait(timeout=5)
		except Exception:
			# Process may already be reaped, or we are deep in interpreter
			# shutdown — never raise from an atexit handler.
			pass
		finally:
			self._chromium_process = None
			self._devtools_url = None
			ChromePDFGenerator._instance = None

	def detach_debug_browser(self):
		"""
		Detach the generator from an interactive debug Chromium process.

		This keeps the debug browser window available for inspection, while ensuring
		the next PDF request starts with a fresh generator/process instead of reusing
		the old debug session.
		"""
		ChromePDFGenerator._instance = None
		self._initialized = False
		self._chromium_process = None
		self._devtools_url = None

	# Polling helper used by _ensure_remote_service() to fetch the DevTools
	# WebSocket URL of the shared on-demand Chromium (re-fetched on each restart).
	def fetch_devtools_url(self, port):
		if not port:
			return None
		url = f"http://127.0.0.1:{port}/json/version"
		try:
			response = requests.get(url, timeout=5)
			response.raise_for_status()  # Raise an exception for HTTP errors
			response_data = response.json()
			return response_data["webSocketDebuggerUrl"].strip()
		except requests.RequestException:
			# Polling helper: a refused connection / timeout here is EXPECTED while
			# the on-demand chromium-pdf service is still starting (cold start). Stay
			# silent; _ensure_remote_service() logs a single error if it never comes
			# up. Logging on every poll spammed Error Log with dozens of false
			# "unreachable" entries per PDF generated after an idle period.
			return None
