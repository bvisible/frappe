# //// Neoffice — added file (no upstream equivalent). Our fork sends every mail at once (frappe.sendmail forces
# //// send_now); when that send fails, the mail must stay in Email Queue for the flush and the caller's action must
# //// go on, unless the caller itself asked for now=True (maintenance #2488).
"""A mail that Neoffice sends at once keeps its send error in the queue; a caller that asked for now=True gets it.

On 2026-11-10 a fake hub answering 503 then 200 showed it on a test clone: the mail was not lost (the row stayed
"Not Sent" and the next flush sent it), but `frappe.sendmail` raised `ValidationError: NeoMail API HTTP 503` up to
the code that called it, so a save, a notification or a password reset failed during every restart of the hub.

Pure tests: the queue row, the site and the send are replaced, nothing touches a database.
"""

import unittest
from unittest import mock

import frappe
from frappe.email.doctype.email_queue import email_queue
from frappe.email.doctype.email_queue.email_queue import QueueBuilder

HUB_DOWN = frappe.ValidationError("NeoMail API HTTP 503: Service Unavailable")


class FakeQueueRow:
	"""An Email Queue row whose send fails the way SendMailContext lets it fail: the row is updated, the error rises."""

	def __init__(self, error=None):
		self.error = error
		self.sends = 0

	def send(self):
		self.sends += 1
		if self.error:
			raise self.error


def process(row, **kwargs):
	"""QueueBuilder.process for one recipient, with the row it creates replaced by `row`."""
	with (
		mock.patch.object(QueueBuilder, "final_recipients", return_value=["buyer@example.com"]),
		mock.patch.object(QueueBuilder, "final_cc", return_value=[]),
		mock.patch.object(QueueBuilder, "final_bcc", return_value=[]),
		mock.patch.object(QueueBuilder, "as_dict", return_value={"sender": "shop@example.com"}),
		mock.patch.object(email_queue.EmailQueue, "new", return_value=row),
	):
		return QueueBuilder(recipients=["buyer@example.com"], sender="shop@example.com").process(**kwargs)


def sendmail(**kwargs):
	"""frappe.sendmail outside the test runner, with no site: returns how it asked QueueBuilder to process."""
	session = frappe._dict(user="Administrator")
	with (
		mock.patch.object(frappe, "flags", frappe._dict(in_test=False)),
		mock.patch.object(frappe, "db", mock.MagicMock(get_value=mock.Mock(return_value=None))),
		mock.patch.object(frappe, "session", session),
		mock.patch("frappe.defaults.get_user_default", return_value=None),
		mock.patch("frappe.defaults.get_global_default", return_value=None),
		mock.patch.object(QueueBuilder, "process", autospec=True, return_value="the row") as process_call,
	):
		frappe.sendmail(recipients=["buyer@example.com"], subject="Order", message="Thanks", **kwargs)
	return process_call.call_args.kwargs


class TestAMailSentAtOnceKeepsItsErrorInTheQueue(unittest.TestCase):
	def test_a_failed_send_that_is_ours_stays_in_the_queue_and_the_caller_goes_on(self):
		row = FakeQueueRow(error=HUB_DOWN)

		returned = process(row, send_now=True, raise_send_error=False)

		self.assertIs(returned, row)
		self.assertEqual(row.sends, 1)

	def test_a_caller_that_asked_for_now_still_gets_the_error(self):
		with self.assertRaises(frappe.ValidationError):
			process(FakeQueueRow(error=HUB_DOWN), send_now=True)

	def test_a_send_that_works_is_made_once(self):
		row = FakeQueueRow()

		self.assertIs(process(row, send_now=True, raise_send_error=False), row)
		self.assertEqual(row.sends, 1)

	def test_sendmail_keeps_the_error_unless_the_caller_asked_for_now(self):
		self.assertEqual(sendmail(), {"send_now": True, "raise_send_error": False})
		self.assertEqual(sendmail(now=True), {"send_now": True, "raise_send_error": True})
		self.assertEqual(sendmail(delayed=False), {"send_now": True, "raise_send_error": True})


if __name__ == "__main__":
	unittest.main()
