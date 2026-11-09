# //// Neoffice — added file (no upstream equivalent): every database connection a request opens is closed when the
# //// request ends, whether it answers or raises (neoffice-maintenance#2407 follow-up).
"""Every database connection a request opens is closed when the request ends, whether it answers or raises.

Two ways a gunicorn worker kept a request's connection open, idle, with its transaction and the metadata locks of every
table the request had read (a DROP or ALTER on those tables then waited without end, #2407):

- a request that raised out of `application()` (from `sync_database` in its `else:`, from `handle_exception`, from its
  `finally:`) skipped the `ClosingIterator` that calls `frappe.destroy()`;
- with `installed_app_modules` gone from Redis (any clear-cache), `frappe.init()` connects by itself to read the installed
  apps, and `init_request` then replaced that connection by its own without closing it.

A connection is checked on the server too: its thread must be gone from the process list.
"""

import time
import traceback
import unittest
from unittest.mock import patch

import frappe
from frappe.database.database import Database
from frappe.tests.test_api import make_request
from frappe.tests.utils import FrappeTestCase
from frappe.utils import get_test_client

PING = "/api/method/ping"
SERVER_GRACE_SECONDS = 3


def read_a_table_then_fail(*args, **kwargs):
	"""What the leaked request did: its transaction read a table, then the end of the request raised."""
	frappe.db.sql("select name from `tabDocType` limit 1")
	raise frappe.QueryTimeoutError("Lock wait timeout exceeded; try restarting transaction")


def fail_in_the_handler(*args, **kwargs):
	raise ValueError("the handler failed")


def why(raised):
	"""What the request raised, with its stack, for an assertion message."""
	return (
		f"the request raised {raised!r}\n" + "".join(getattr(raised, "__notes__", ()))
		if raised
		else "nothing raised"
	)


class TestRequestConnectionIsClosed(FrappeTestCase):
	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		if frappe.conf.db_type not in (None, "mariadb"):
			raise unittest.SkipTest("the server-side check reads MariaDB's process list")

	def request(self, *patches, cold_module_cache=False):
		"""GET PING in a thread of its own (as a worker would), with `patches` applied.

		Returns (what the request raised or None, [(each Database the request connected, its server thread id)])."""
		if cold_module_cache:
			# what a clear-cache leaves: no module map, no global defaults (frappe.init then reads the installed apps itself)
			frappe.cache.delete_value("installed_app_modules")
			frappe.cache.hdel("defaults", "__global")
		else:
			frappe.setup_module_map(include_all_apps=False)  # puts `installed_app_modules` back in Redis

		opened = []
		real_connect = Database.connect

		def connect_and_remember(database):
			real_connect(database)
			opened.append((database, database._conn.thread_id()))

		client = get_test_client(use_cookies=False)

		def get():
			try:
				client.get(PING, buffered=True)  # buffered: werkzeug closes the response, as gunicorn does
			except BaseException as error:
				error.add_note(traceback.format_exc())
				return error

		with patch.object(Database, "connect", connect_and_remember):
			for one in patches:
				one.start()
			try:
				raised = make_request(target=get, args=(), kwargs={}, site=frappe.local.site)
			finally:
				for one in patches:
					one.stop()
		self.addCleanup(self.close_what_leaked, opened)
		return raised, opened

	@staticmethod
	def close_what_leaked(opened):
		for database, _thread_id in opened:
			if database._conn:
				database.close()

	def assertAllClosed(self, opened):
		self.assertTrue(opened, "the request connected to the database")
		for database, thread_id in opened:
			self.assertIsNone(
				database._conn, f"connection {thread_id} is still open in the worker (of {len(opened)})"
			)
			deadline = time.monotonic() + SERVER_GRACE_SECONDS
			while True:
				alive = frappe.db.sql(
					"select count(*) from information_schema.processlist where id = %s", thread_id
				)
				if not alive[0][0] or time.monotonic() > deadline:
					break
				time.sleep(0.1)
			self.assertEqual(alive[0][0], 0, f"the server still holds the request's session {thread_id}")

	def test_a_request_that_answers_closes_its_connection(self):
		raised, opened = self.request()
		self.assertIsNone(raised, why(raised))
		self.assertAllClosed(opened)

	def test_a_request_whose_end_raises_closes_its_connection(self):
		"""`sync_database` runs in the `else:` of application(), outside its `except`: what it raises leaves the request
		(on osiris, `session.update()` met a lock wait timeout)."""
		raised, opened = self.request(patch("frappe.app.sync_database", read_a_table_then_fail))
		self.assertIsInstance(raised, frappe.QueryTimeoutError, why(raised))
		self.assertAllClosed(opened)

	def test_a_request_whose_error_handling_raises_closes_its_connection(self):
		"""On a new instance the error page failed like the page it replaced (#2407): handle_exception raised."""
		raised, opened = self.request(
			patch("frappe.api.handle", fail_in_the_handler),
			patch("frappe.app.handle_exception", read_a_table_then_fail),
		)
		self.assertIsInstance(raised, frappe.QueryTimeoutError, why(raised))
		self.assertAllClosed(opened)

	def test_a_request_on_a_cold_module_cache_closes_the_connection_init_opened(self):
		raised, opened = self.request(cold_module_cache=True)
		self.assertIsNone(raised, why(raised))
		self.assertAllClosed(opened)
