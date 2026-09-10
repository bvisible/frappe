# //// Neoffice — added file (no upstream equivalent). Defects of THIS fork, found by the ////
# //// marking campaign and listed in neoffice-maintenance#205. Each test fails on the code as it
# //// stood before its fix; together they are what stops the next upstream merge from quietly
# //// putting any of them back.
"""Three fork defects: a stuck message flag, a shadowed builtin, a dead broken copy."""

import unittest
# //// Neoffice — MagicMock and patch import for the queue/account regression tests below
# //// (715c3b5e99 "fix(email): the counter that disables a broken account survives a restart",
# //// 9401d48caa "fix(email): a deleted Communication no longer jams a queue entry for good").
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


# //// Neoffice ▼▼▼ — regression tests for two survive-a-restart fork fixes: the deleted-Communication
# //// queue jam (9401d48caa "fix(email): a deleted Communication no longer jams a queue entry for good",
# //// tracker #81/#245) below, and the failed-attempts counter (715c3b5e99 "fix(email): the counter that
# //// disables a broken account survives a restart", tracker #250) further down.
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


# //// Neoffice — see the block marker above: restart-proof counter tests (715c3b5e99).
class TestBrokenIncomingAccountIsActuallyDisabled(unittest.TestCase):
	"""The counter that decides it must survive a restart.

	`handle_incoming_connect_error` disables an account after six consecutive
	failures, but upstream keeps the count in the cache Redis — configured
	`maxmemory-policy allkeys-lru` with `save ""`. It is wiped by every bench
	restart and every clear-cache, and can be evicted under normal load. On a
	fleet we redeploy, six-in-a-row was never reached: `_Test Comm Account 1`
	wrote 34 Error Log entries in 40 minutes on osiris and stayed enabled
	forever (#250). `no_failed` — the document's own field, already incremented
	on socket errors and reset on a successful validate — was there all along.
	"""

	def _account(self, no_failed=0):
		from frappe.email.doctype.email_account.email_account import EmailAccount

		a = EmailAccount.__new__(EmailAccount)
		a.name = "test-account"
		a.no_failed = no_failed
		return a

	def test_the_count_is_read_from_the_document(self):
		self.assertEqual(self._account(no_failed=4).get_failed_attempts_count(), 4)

	def test_the_count_is_written_to_the_document(self):
		a = self._account(no_failed=2)
		with patch.object(a, "db_set") as db:
			a.set_failed_attempts_count(3)
		db.assert_called_once()
		self.assertEqual(db.call_args.args[:2], ("no_failed", 3))

	def test_it_does_not_touch_the_cache(self):
		"""A key that survives neither a restart nor an eviction cannot carry a
		decision about six CONSECUTIVE failures."""
		a = self._account(no_failed=1)
		with patch.object(a, "db_set"), patch.object(frappe, "cache") as cache:
			a.set_failed_attempts_count(2)
			a.get_failed_attempts_count()
		cache.set_value.assert_not_called()
		cache.get_value.assert_not_called()

	def test_an_unchanged_count_writes_nothing(self):
		"""A successful poll resets the counter every few minutes; rewriting the
		same zero would be one UPDATE per account per tick, for nothing."""
		a = self._account(no_failed=0)
		with patch.object(a, "db_set") as db:
			a.set_failed_attempts_count(0)
		db.assert_not_called()

	def test_past_the_threshold_the_account_is_disabled(self):
		a = self._account(no_failed=6)
		with patch.object(frappe, "enqueue") as enq, patch.object(a, "db_set") as db:
			a.handle_incoming_connect_error(description="imap.example.com unreachable")
		enq.assert_called_once()
		db.assert_not_called()

	def test_below_the_threshold_it_only_counts(self):
		"""The inverse control: a passing network hiccup must not disable anything."""
		a = self._account(no_failed=1)
		with patch.object(frappe, "enqueue") as enq, patch.object(a, "db_set") as db:
			a.handle_incoming_connect_error(description="hiccup")
		enq.assert_not_called()
		self.assertEqual(db.call_args.args[:2], ("no_failed", 2))


class TestIsSafePathIsNotWidened(unittest.TestCase):
	"""is_safe_path() is the guard File.get_full_path() asks before touching a path on disk.

	The fork's very first commit widened it with a hard-coded `/mnt/neoffice` prefix, so anything
	on the data volume — including another site's private files and the backup directory — passed
	a check whose whole job is to keep a path inside the site. It bought nothing: our instances
	reach that volume through the site's own `private`/`public` SYMLINKS, and os.path.abspath does
	not follow symlinks, so the upstream check already says yes to every real file. Measured on
	2026-09-09: zero File rows carry a /mnt file_url across four instances of the fleet
	(32 057 files). neoffice-maintenance#205.
	"""

	def test_a_path_inside_the_site_is_accepted(self):
		from frappe.utils.file_manager import is_safe_path

		self.assertTrue(is_safe_path(frappe.get_site_path("private", "files", "x.pdf")))

	def test_the_data_volume_is_not_a_base_directory_of_its_own(self):
		from frappe.utils.file_manager import is_safe_path

		self.assertFalse(is_safe_path("/mnt/neoffice/backups/prod.local.sql.gz"))
		self.assertFalse(is_safe_path("/mnt/neoffice/private/files/other-site.pdf"))

	def test_a_path_outside_the_site_is_still_refused(self):
		from frappe.utils.file_manager import is_safe_path

		self.assertFalse(is_safe_path("/etc/passwd"))
		self.assertFalse(is_safe_path(frappe.get_site_path("..", "..", "..", "etc", "passwd")))


class TestS3UploadDoesNotReportAFailureAsSuccess(unittest.TestCase):
	"""upload_file_to_s3() used to log-and-return on every failure path.

	take_backups_s3() then sent the "backup succeeded" mail all the same: a run where not one
	byte reached S3 was reported as a good backup. The same file also shipped the backups with
	TLS verification disabled (`--no-check-certificate` on the rclone branch, which wrote the S3
	secret to a file on disk) and, for files under 50 MB, handed the secret to the endpoint as
	HTTP Basic auth — which S3 never accepts. All three are gone; one transport remains, and it
	raises. neoffice-maintenance#205.
	"""

	def _settings(self, backup_path="osiris/"):
		doc = MagicMock()
		doc.backup_path = backup_path
		return doc

	def test_a_missing_backup_file_raises_instead_of_returning(self):
		from frappe.integrations.doctype.s3_backup_settings.s3_backup_settings import upload_file_to_s3

		conn = MagicMock()
		with patch("frappe.get_single", return_value=self._settings()):
			with self.assertRaises(frappe.ValidationError):
				upload_file_to_s3("/nonexistent/prod.local-database.sql.gz", "folder", conn, "bucket")
		conn.upload_file.assert_not_called()

	def test_an_upload_failure_reaches_the_caller(self):
		import os
		import tempfile

		from frappe.integrations.doctype.s3_backup_settings.s3_backup_settings import upload_file_to_s3

		conn = MagicMock()
		conn.upload_file.side_effect = OSError("connection reset")
		with tempfile.NamedTemporaryFile(suffix=".sql.gz") as f:
			f.write(b"x")
			f.flush()
			with patch("frappe.get_single", return_value=self._settings()):
				with self.assertRaises(OSError):
					upload_file_to_s3(f.name, "folder", conn, "bucket")

	def test_the_key_carries_backup_path_again(self):
		import tempfile

		from frappe.integrations.doctype.s3_backup_settings.s3_backup_settings import upload_file_to_s3

		conn = MagicMock()
		with tempfile.NamedTemporaryFile(suffix=".sql.gz") as f:
			f.write(b"x")
			f.flush()
			with patch("frappe.get_single", return_value=self._settings("osiris/")):
				upload_file_to_s3(f.name, "folder", conn, "bucket")
		key = conn.upload_file.call_args[0][2]
		self.assertTrue(key.startswith("osiris/"), key)
		self.assertIn("/folder/", key)
		self.assertTrue(key.endswith(f.name.rsplit("/", 1)[-1]), key)

	def test_no_backup_path_still_builds_a_key(self):
		import tempfile

		from frappe.integrations.doctype.s3_backup_settings.s3_backup_settings import upload_file_to_s3

		conn = MagicMock()
		with tempfile.NamedTemporaryFile(suffix=".sql.gz") as f:
			f.write(b"x")
			f.flush()
			with patch("frappe.get_single", return_value=self._settings(None)):
				upload_file_to_s3(f.name, "folder", conn, "bucket")
		key = conn.upload_file.call_args[0][2]
		self.assertFalse(key.startswith("/"), key)
		self.assertIn("/folder/", key)

	def test_the_module_ships_no_tls_bypass_and_no_secret_on_disk(self):
		"""Scanned on the CODE, comments stripped — ast.unparse drops them.

		Reading the raw source would match the comment above upload_file_to_s3, which names
		each removed transport on purpose.
		"""
		import ast
		import inspect

		from frappe.integrations.doctype.s3_backup_settings import s3_backup_settings

		code = ast.unparse(ast.parse(inspect.getsource(s3_backup_settings)))
		for gone in ("--no-check-certificate", "rclone", "NamedTemporaryFile", "requests.put"):
			self.assertNotIn(gone, code, f"{gone} is back in the S3 backup transport")


class TestAPartialBackupIsNotAnnouncedAsASuccess(unittest.TestCase):
	"""68d7f3a760 let a failed files tarball fall back to a database-only run — and the mail still
	said "backup succeeded". take_backups_s3() now asks backup_to_s3() what did NOT reach S3 and
	names it instead. neoffice-maintenance#205."""

	def test_a_clean_run_still_sends_the_success_mail(self):
		from frappe.integrations.doctype.s3_backup_settings import s3_backup_settings

		with patch.object(s3_backup_settings, "validate_file_size"), patch.object(
			s3_backup_settings, "backup_to_s3", return_value=[]
		), patch.object(s3_backup_settings, "send_email") as send, patch.object(
			s3_backup_settings, "notify"
		) as notify:
			s3_backup_settings.take_backups_s3()
		send.assert_called_once()
		self.assertIs(send.call_args[0][0], True)
		notify.assert_not_called()

	def test_a_run_missing_a_piece_does_not_send_the_success_mail(self):
		from frappe.integrations.doctype.s3_backup_settings import s3_backup_settings

		with patch.object(s3_backup_settings, "validate_file_size"), patch.object(
			s3_backup_settings, "backup_to_s3", return_value=["the private files archive"]
		), patch.object(s3_backup_settings, "send_email") as send, patch.object(
			s3_backup_settings, "notify"
		) as notify:
			s3_backup_settings.take_backups_s3()
		send.assert_not_called()
		notify.assert_called_once()
		self.assertIn("private files", notify.call_args[0][0])

# //// Neoffice ▲▲▲
