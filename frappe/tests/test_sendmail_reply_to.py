# //// Neoffice — added file (no upstream equivalent). Regression tests for neoffice-maintenance#1288:
# //// the NeoMail sender block of `frappe.sendmail` rewrote `reply_to` in every branch and never read
# //// the one the caller passed, so the website contact form (`reply_to=<visitor>`) left with the
# //// shop's own mailbox as Reply-To and the shop could not answer the visitor.
"""`frappe.sendmail` keeps the Reply-To its caller asked for."""

import email
import unittest
from email.utils import parseaddr
from unittest.mock import patch

import frappe

VISITOR = "visitor-neoffice-test@example.com"
RECIPIENT = "shop-neoffice-test@example.com"


class TestSendmailKeepsTheCallersReplyTo(unittest.TestCase):
	def setUp(self):
		frappe.db.savepoint("neoffice_sendmail_reply_to")
		# A site made for the tests (the CI's) has no outgoing Email Account and `sendmail` then refuses
		# to queue anything: this test makes one, in its own transaction. A site that has one (a clone
		# of a real site) is left as it is. The cache of accounts is per process: emptied here and put
		# back, so an account made here never reaches the tests that run after these.
		self.accounts = getattr(frappe.local, "outgoing_email_account", None)
		frappe.local.outgoing_email_account = {}
		if not frappe.db.exists("Email Account", {"enable_outgoing": 1, "default_outgoing": 1}):
			frappe.get_doc(
				{
					"doctype": "Email Account",
					"email_account_name": "_NEOFFICE reply-to test account",
					"email_id": "neoffice-reply-to-test@example.com",
					"enable_outgoing": 1,
					"default_outgoing": 1,
					"smtp_server": "smtp.example.invalid",
				}
			).insert(ignore_permissions=True)

	def tearDown(self):
		if self.accounts is None:
			try:
				delattr(frappe.local, "outgoing_email_account")
			except AttributeError:
				pass
		else:
			frappe.local.outgoing_email_account = self.accounts
		frappe.db.rollback(save_point="neoffice_sendmail_reply_to")

	def headers(self, **kwargs):
		"""Queue a mail (under test `sendmail` queues and does not send) and read its headers."""
		queued = frappe.sendmail(
			recipients=[RECIPIENT], subject="_NEOFFICE reply-to", content="Hello", delayed=True, **kwargs
		)
		self.assertIsNotNone(queued, "sendmail queued nothing")
		return email.message_from_string(queued.message)

	def test_a_contact_form_message_is_answered_to_the_visitor(self):
		"""What frappe.www.contact.send_message does: reply_to is the visitor, no sender."""
		mail = self.headers(reply_to=VISITOR)
		self.assertEqual(parseaddr(mail["Reply-To"])[1], VISITOR)

	def test_the_reply_to_wins_whatever_the_sender_is(self):
		mail = self.headers(reply_to=VISITOR, sender="someone-neoffice-test@example.com")
		self.assertEqual(parseaddr(mail["Reply-To"])[1], VISITOR)

	def test_without_a_reply_to_the_mail_is_answered_to_its_sender_as_before(self):
		mail = self.headers()
		self.assertEqual(parseaddr(mail["Reply-To"])[1], parseaddr(mail["From"])[1])

	def test_a_reply_to_that_is_not_an_address_is_ignored_as_before(self):
		"""Callers were free to pass anything while the argument was dropped: it must not start to
		raise now that it is read."""
		mail = self.headers(reply_to="not an address")
		self.assertEqual(parseaddr(mail["Reply-To"])[1], parseaddr(mail["From"])[1])

	def test_a_helpdesk_reply_still_goes_back_to_the_ticket_mailbox(self):
		"""A ticket is answered through its own mailbox, which is fetched; an agent's personal
		address as Reply-To would take the customer's reply out of the ticket for good."""
		real_get_value = frappe.db.get_value

		def get_value(doctype, filters=None, fieldname="name", *args, **kwargs):
			if doctype == "HD Ticket":
				return "_NEOFFICE Tickets"
			if doctype == "Email Account" and filters == "_NEOFFICE Tickets":
				return 1 if fieldname == "enable_outgoing" else "tickets-neoffice-test@example.com"
			return real_get_value(doctype, filters, fieldname, *args, **kwargs)

		with patch.object(frappe.db, "get_value", side_effect=get_value):
			mail = self.headers(
				reply_to="agent-neoffice-test@example.com",
				reference_doctype="HD Ticket",
				reference_name="_NEOFFICE-T-1",
			)
		self.assertEqual(parseaddr(mail["Reply-To"])[1], "tickets-neoffice-test@example.com")
