# //// Neoffice — added file (no upstream equivalent). The immediate send frappe.sendmail forces waits for the caller's
# //// transaction to commit (frappe/__init__.py `_neoffice_send_after_commit`, neoffice-maintenance#2654).
"""A mail leaves right after the caller's commit, never inside its transaction."""

from contextlib import contextmanager
from unittest.mock import patch

import frappe
from frappe.email.doctype.email_queue.email_queue import EmailQueue
from frappe.tests.utils import FrappeTestCase
from frappe.utils import set_request

RECIPIENT = "neoffice-sendmail-2654@example.com"


class TestTheImmediateSendWaitsForTheCommit(FrappeTestCase):
	def setUp(self):
		self.sent = []
		sent = self.sent

		def send(queue_doc, *args, **kwargs):  # records the send; the real one commits
			sent.append(queue_doc.name)

		patcher = patch.object(EmailQueue, "send", send)  # enterContext needs Python 3.11
		patcher.start()
		self.addCleanup(patcher.stop)
		self._request = getattr(frappe.local, "request", None)
		self._commit_flag = frappe.local.flags.commit
		frappe.local.request = None

	def tearDown(self):
		frappe.db.after_commit.reset()
		frappe.local.request = self._request
		frappe.local.flags.commit = self._commit_flag

	@contextmanager
	def outside_tests(self):
		"""Under test frappe.sendmail only queues; the forced send is what production does."""
		in_test = frappe.flags.in_test
		frappe.flags.in_test = False
		try:
			yield
		finally:
			frappe.flags.in_test = in_test

	def _sendmail(self, **kwargs):
		with self.outside_tests():
			return frappe.sendmail(
				recipients=[RECIPIENT], subject="Enrolment confirmed", message="Welcome.", **kwargs
			)

	def test_the_mail_is_not_sent_inside_the_callers_transaction(self):
		queued = self._sendmail()
		self.assertEqual(self.sent, [])
		self.assertEqual(frappe.db.get_value("Email Queue", queued.name, "status"), "Not Sent")

	def test_the_mail_is_sent_when_the_caller_commits(self):
		queued = self._sendmail()
		frappe.db.after_commit.run()  # what frappe.db.commit() runs once the transaction is committed
		self.assertEqual(self.sent, [queued.name])

	def test_a_savepoint_taken_before_the_mail_is_still_there(self):
		frappe.db.savepoint("neoffice_enrol_one_student")
		self._sendmail()
		frappe.db.release_savepoint("neoffice_enrol_one_student")
		self.assertEqual(self.sent, [])

	def test_a_mail_rolled_back_with_its_savepoint_is_never_sent(self):
		frappe.db.savepoint("neoffice_enrol_one_student")
		queued = self._sendmail()
		frappe.db.rollback(save_point="neoffice_enrol_one_student")
		frappe.db.after_commit.run()
		self.assertFalse(frappe.db.exists("Email Queue", queued.name))
		self.assertEqual(self.sent, [])

	def test_a_callers_own_now_still_sends_in_place(self):
		queued = self._sendmail(now=True)
		self.assertEqual(self.sent, [queued.name])

	def test_a_get_request_that_queues_a_mail_ends_with_a_commit(self):
		set_request(method="GET", path="/confirm")
		frappe.local.flags.commit = False
		self._sendmail()
		self.assertTrue(frappe.local.flags.commit)
