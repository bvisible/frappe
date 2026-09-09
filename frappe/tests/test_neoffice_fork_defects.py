# //// Neoffice — added file (no upstream equivalent). Defects of THIS fork, found by the ////
# //// marking campaign and listed in neoffice-maintenance#205. Each test fails on the code as it
# //// stood before its fix; together they are what stops the next upstream merge from quietly
# //// putting any of them back.
"""Three fork defects: a stuck message flag, a shadowed builtin, a dead broken copy."""

import unittest
from unittest.mock import MagicMock, patch

import frappe


class TestMuteMessagesIsRestored(unittest.TestCase):
	"""get_doc on a missing document muted frappe.msgprint for the REST of the request.

	frappe.msgprint returns early on frappe.flags.mute_messages (frappe/__init__.py), so the flag
	our fork set before throwing DoesNotExistError -- and never put back -- silenced every later
	message of that request: a stale link followed by a real warning showed the user nothing.
	"""

	def setUp(self):
		self.before = frappe.flags.mute_messages
		frappe.flags.mute_messages = False
		frappe.clear_messages()

	def tearDown(self):
		frappe.flags.mute_messages = self.before
		frappe.clear_messages()

	def test_a_missing_document_leaves_the_flag_as_it_found_it(self):
		with self.assertRaises(frappe.DoesNotExistError):
			frappe.get_doc("ToDo", "_NEOFFICE-does-not-exist")
		self.assertFalse(frappe.flags.mute_messages, "the flag stayed on after the throw")

	def test_a_later_message_still_reaches_the_user(self):
		"""What the flag actually costs: the assertion above could pass on a flag that is reset
		somewhere else, this one measures the consequence."""
		with self.assertRaises(frappe.DoesNotExistError):
			frappe.get_doc("ToDo", "_NEOFFICE-does-not-exist")
		frappe.msgprint("_NEOFFICE later message")
		self.assertTrue(
			any("_NEOFFICE later message" in str(m) for m in frappe.get_message_log()),
			"a message raised after a missing-document lookup was swallowed",
		)

	def test_a_caller_that_had_muted_on_purpose_keeps_its_choice(self):
		"""The flag is restored to what it was, not to False: a caller running inside its own
		mute (bulk imports do) must not be un-muted by a lookup failure."""
		frappe.flags.mute_messages = True
		try:
			with self.assertRaises(frappe.DoesNotExistError):
				frappe.get_doc("ToDo", "_NEOFFICE-does-not-exist")
			self.assertTrue(frappe.flags.mute_messages)
		finally:
			frappe.flags.mute_messages = False


class TestUtilsDoesNotShadowConnectionError(unittest.TestCase):
	"""`from redis.exceptions import ConnectionError` in frappe/utils/__init__.py replaced the
	BUILTIN ConnectionError for that module and for everything doing `from frappe.utils import *`:
	an `except ConnectionError` written against the builtin would not catch a socket error."""

	def test_frappe_utils_exports_the_builtin(self):
		import frappe.utils

		self.assertIs(
			getattr(frappe.utils, "ConnectionError", ConnectionError),
			ConnectionError,
			"frappe.utils re-exports redis's ConnectionError over the builtin",
		)

	def test_a_socket_error_is_caught_by_that_name(self):
		namespace = {}
		exec("from frappe.utils import *", namespace)  # noqa: S102 - the star-import is the point
		caught = False
		try:
			raise ConnectionResetError("peer went away")
		except namespace.get("ConnectionError", ConnectionError):
			caught = True
		self.assertTrue(caught, "a real socket error escaped `except ConnectionError`")


class TestPrintUtilsHasNoDeadAttachPrint(unittest.TestCase):
	"""frappe/utils/print_utils.py carried a copy of frappe.attach_print that could never run: it
	calls cint()/cstr(), which the module does not import. Every caller uses frappe.attach_print."""

	def test_the_module_does_not_offer_a_broken_attach_print(self):
		from frappe.utils import print_utils

		self.assertFalse(
			hasattr(print_utils, "attach_print"),
			"the dead copy is back; callers must use frappe.attach_print",
		)

	def test_the_real_one_is_still_there(self):
		self.assertTrue(callable(frappe.attach_print))


class TestEmailQueueSurvivesADeletedCommunication(unittest.TestCase):
	"""A queue entry whose Communication was deleted must not jam the whole queue.

	`update_status` is called from `SendMailContext.__enter__`, BEFORE the mail is
	attempted. Upstream loads the Communication unguarded, so a deleted one raises
	there — after `update_db` has already written "Sending". `get_queue()` only
	selects Not Sent / Partially Sent, so the entry is never looked at again while
	the scheduler raises on it at every flush: 360 errors a day on osiris, retry
	counts up to 362, all from Communications a test run had deleted (#245 → #81).
	"""

	def _entry(self, communication):
		"""Only `update_status` is under test, so the Document constructor — which
		refuses to be called without arguments — is stepped around deliberately."""
		from frappe.email.doctype.email_queue.email_queue import EmailQueue

		q = EmailQueue.__new__(EmailQueue)
		q.name = "test-queue-entry"
		q.communication = communication
		return q

	def test_a_deleted_communication_does_not_raise(self):
		q = self._entry("gone-forever")
		with (
			patch.object(q, "update_db"),
			patch.object(
				frappe, "get_doc", side_effect=frappe.DoesNotExistError("Communication gone-forever not found")
			),
		):
			q.update_status("Sending", commit=True)  # must return, not raise

	def test_it_leaves_no_message_behind(self):
		"""frappe.get_doc throws through frappe.throw, which queues its message even
		when the exception is caught — a later screen would show an error for an
		operation that succeeded."""
		q = self._entry("gone-forever")
		before = list(frappe.message_log)

		def _throwing_get_doc(*a, **kw):
			frappe.message_log.append({"message": "Communication gone-forever not found"})
			raise frappe.DoesNotExistError()

		with patch.object(q, "update_db"), patch.object(frappe, "get_doc", side_effect=_throwing_get_doc):
			q.update_status("Sending")
		self.assertEqual(list(frappe.message_log), before)

	def test_the_status_is_still_written(self):
		"""The entry's own status is the part that must always land."""
		q = self._entry("gone-forever")
		with (
			patch.object(q, "update_db") as db,
			patch.object(frappe, "get_doc", side_effect=frappe.DoesNotExistError()),
		):
			q.update_status("Sending", commit=True)
		db.assert_called_once()
		self.assertEqual(db.call_args.kwargs.get("status"), "Sending")

	def test_a_live_communication_is_still_stamped(self):
		"""The inverse control: nothing is skipped when the document is there."""
		q = self._entry("still-here")
		comm = MagicMock()
		with patch.object(q, "update_db"), patch.object(frappe, "get_doc", return_value=comm):
			q.update_status("Sent", commit=True)
		comm.set_delivery_status.assert_called_once_with(commit=True)

	def test_an_entry_without_communication_touches_nothing(self):
		q = self._entry(None)
		with patch.object(q, "update_db"), patch.object(frappe, "get_doc") as g:
			q.update_status("Sent")
		g.assert_not_called()
