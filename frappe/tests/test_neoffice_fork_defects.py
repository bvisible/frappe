# //// Neoffice — added file (no upstream equivalent). Defects of THIS fork, found by the ////
# //// marking campaign and listed in neoffice-maintenance#205. Each test fails on the code as it
# //// stood before its fix; together they are what stops the next upstream merge from quietly
# //// putting any of them back.
"""Three fork defects: a stuck message flag, a shadowed builtin, a dead broken copy."""

import unittest

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
