# //// Neoffice — added file (no upstream equivalent): tests for maintenance#1521, the sending account's signature
# //// goes out once on a mail written in the composer (frappe.email.email_body._message_has_signature).
# //// Drop together with that change once the composer is the only place that adds the signature.
"""The sending account's signature is in a mail once.

The composer (communication.js, set_content) writes the signature into the editor for an account that may read
Email Account, and the render (get_formatted_html) added it again under the message: a mail written by hand ended
with it twice. A notification or a reminder, written by code, has none in its body and gets the render's.
"""

import frappe
from frappe.email.email_body import _message_has_signature, get_formatted_html
from frappe.tests.utils import FrappeTestCase

# as the account keeps it: one block, a line break inside
SIGNATURE = "<p>Meilleures salutations,<br>L'équipe X</p>"
# as the editor writes it back into the message: the same words, in two paragraphs
IN_THE_EDITOR = "<p>Meilleures salutations,</p><p>L&#x27;équipe X</p>"
WORDS = "Meilleures salutations"


def account():
	return frappe._dict(add_signature=1, signature=SIGNATURE, footer="")


def times_in(html):
	return html.count(WORDS)


class TestSignatureOnce(FrappeTestCase):
	def render(self, message):
		return get_formatted_html("Subject", message, email_account=account())

	def test_a_message_that_holds_the_signature_keeps_it_once(self):
		message = f'<div class="ql-editor read-mode"><p>Bonjour,</p><p>Voici la facture.</p><p><br></p>{IN_THE_EDITOR}</div>'
		self.assertEqual(times_in(self.render(message)), 1)

	def test_a_message_that_holds_it_with_other_line_breaks_keeps_it_once(self):
		message = "<p>Bonjour,</p>\n<p>Meilleures salutations,</p>\n<p>L'équipe X</p>\n"
		self.assertEqual(times_in(self.render(message)), 1)

	def test_a_message_without_the_signature_receives_it_once(self):
		self.assertEqual(times_in(self.render("<p>Bonjour,</p><p>Voici la facture.</p>")), 1)

	def test_a_notification_receives_the_signature_once(self):
		self.assertEqual(times_in(self.render("<p>Reminder: the invoice SINV-0001 is due.</p>")), 1)

	def test_a_signature_that_only_an_earlier_mail_holds_is_still_added(self):
		quoted = f"<blockquote><p>On Monday, someone wrote:</p><p>Hello</p>{IN_THE_EDITOR}</blockquote>"
		html = self.render(f"<p>Thank you.</p>{quoted}")
		self.assertEqual(times_in(html), 2, "the quoted one, and the signature of this mail")

	def test_a_message_that_says_it_holds_the_signature_gets_none(self):
		self.assertEqual(times_in(self.render("<p>Your code is 1234.</p><!-- signature-included -->")), 0)

	def test_an_account_without_a_signature_adds_none(self):
		html = get_formatted_html(
			"Subject",
			"<p>Hello</p>",
			email_account=frappe._dict(add_signature=0, signature=SIGNATURE, footer=""),
		)
		self.assertEqual(times_in(html), 0)


class TestMessageHasSignature(FrappeTestCase):
	def test_the_text_is_compared_without_tags_entities_and_line_breaks(self):
		self.assertTrue(_message_has_signature(IN_THE_EDITOR, SIGNATURE))
		self.assertTrue(
			_message_has_signature("<p>x</p>\n<p>Meilleures salutations,\nL'équipe X</p>", SIGNATURE)
		)
		self.assertTrue(
			_message_has_signature("<p>Meilleures&nbsp;salutations,</p><p>L'équipe X</p>", SIGNATURE)
		)

	def test_another_text_is_not_the_signature(self):
		self.assertFalse(_message_has_signature("<p>Meilleures salutations,</p><p>L'équipe Y</p>", SIGNATURE))

	def test_a_signature_without_text_is_never_found(self):
		self.assertFalse(_message_has_signature("<p>Hello</p>", "<br><p> </p>"))
		self.assertFalse(_message_has_signature("<p>Hello</p>", ""))

	def test_a_quoted_block_does_not_count_however_deep_it_is_nested(self):
		nested = f"<blockquote><p>Earlier</p><blockquote>{IN_THE_EDITOR}</blockquote><p>tail</p>{IN_THE_EDITOR}</blockquote>"
		self.assertFalse(_message_has_signature(f"<p>Thanks</p>{nested}", SIGNATURE))
		self.assertTrue(_message_has_signature(f"<p>Thanks</p>{IN_THE_EDITOR}{nested}", SIGNATURE))
