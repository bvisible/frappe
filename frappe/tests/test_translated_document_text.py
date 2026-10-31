# //// Neoffice — added file (no upstream equivalent)
"""A mail is written in the language the apps give the document it is about (multilingual lot 9, neoffice-maintenance#2037).

An Email Template and a Notification hold one wording, in the site's language. The apps hold the others and say so through the
hook ``translated_document_text``; the language of the mail is the one ``document_print_language`` gives the document. These
tests stand both hooks in with ``frappe.get_hooks`` patched: they need no app but frappe.

    bench --site <site> run-tests --module frappe.tests.test_translated_document_text
"""

from unittest.mock import patch

import frappe
from frappe.core.doctype.user.user import User, in_the_users_language
from frappe.email.doctype.notification.notification import Notification
from frappe.tests.utils import FrappeTestCase
from frappe.translate import (
	document_print_language,
	print_language,
	render_with_fallback,
	translated_document_text,
)

LANGUAGE_HOOK = "frappe.tests.test_translated_document_text.german_for_notes"
TEXT_HOOK = "frappe.tests.test_translated_document_text.german_texts"
USER_HOOK = "frappe.tests.test_translated_document_text.german_for_users"
real_get_hooks = frappe.get_hooks
asked = []


def german_for_notes(doctype=None, name=None, doc=None, print_format=None):
	return "de" if doctype == "Note" else None


def german_for_users(doctype=None, name=None, doc=None, print_format=None):
	return "de" if doctype == "User" else None


def german_texts(doctype=None, name=None, fieldname=None, language=None):
	asked.append((doctype, name, fieldname, language, frappe.local.lang))
	if language == "de" and name == "ZZ lot 9":
		return {"subject": "Guten Tag", "response": "<p>Danke</p>", "message": "Danke schön"}.get(fieldname)
	return None


def hooked(**hooks):
	def get_hooks(hook=None, *args, **kwargs):
		if hook in hooks:
			return list(hooks[hook])
		return real_get_hooks(hook, *args, **kwargs)

	return patch("frappe.get_hooks", side_effect=get_hooks)


class TestTheQuestion(FrappeTestCase):
	def setUp(self):
		asked.clear()

	def test_without_a_hook_nobody_answers(self):
		self.assertIsNone(translated_document_text("Email Template", "ZZ lot 9", "subject", "de"))

	def test_the_first_answer_wins_and_the_language_in_force_is_the_default(self):
		with hooked(translated_document_text=[TEXT_HOOK]):
			self.assertEqual(
				translated_document_text("Email Template", "ZZ lot 9", "subject", "de"), "Guten Tag"
			)
			with print_language("de"):
				self.assertEqual(
					translated_document_text("Email Template", "ZZ lot 9", "subject"), "Guten Tag"
				)
			self.assertIsNone(translated_document_text("Email Template", "ZZ lot 9", "subject", "it"))

	def test_an_incomplete_question_is_not_asked(self):
		with hooked(translated_document_text=[TEXT_HOOK]):
			self.assertIsNone(translated_document_text("Email Template", None, "subject", "de"))
			self.assertIsNone(translated_document_text("", "ZZ lot 9", "subject", "de"))
		self.assertEqual(asked, [])


class TestAnEmailTemplate(FrappeTestCase):
	def setUp(self):
		asked.clear()
		self.template = frappe.get_doc(
			{
				"doctype": "Email Template",
				"name": "ZZ lot 9",
				"subject": "Bonjour {{ title }}",
				"response": "<p>Merci {{ title }}</p>",
				"use_html": 0,
			}
		)
		self.template.name = "ZZ lot 9"

	def test_without_a_hook_it_renders_its_own_text(self):
		mail = self.template.get_formatted_email({"doctype": "Note", "name": "x", "title": "Marie"})
		self.assertEqual(mail, {"subject": "Bonjour Marie", "message": "<p>Merci Marie</p>"})

	def test_a_mail_about_a_german_document_is_the_german_wording(self):
		with hooked(document_print_language=[LANGUAGE_HOOK], translated_document_text=[TEXT_HOOK]):
			mail = self.template.get_formatted_email({"doctype": "Note", "name": "x", "title": "Marie"})
		self.assertEqual(mail, {"subject": "Guten Tag", "message": "<p>Danke</p>"})

	def test_the_language_is_in_force_when_the_wording_is_asked_for(self):
		with hooked(document_print_language=[LANGUAGE_HOOK], translated_document_text=[TEXT_HOOK]):
			self.template.get_formatted_email({"doctype": "Note", "name": "x"})
		self.assertTrue(asked)
		for _doctype, _name, _fieldname, language, in_force in asked:
			self.assertEqual((language, in_force), ("de", "de"))

	def test_the_html_body_is_asked_for_by_its_own_field(self):
		self.template.use_html = 1
		self.template.response_html = "<p>Merci</p>"
		with hooked(document_print_language=[LANGUAGE_HOOK], translated_document_text=[TEXT_HOOK]):
			self.template.get_formatted_email({"doctype": "Note", "name": "x"})
		self.assertIn("response_html", [call[2] for call in asked])

	def test_a_document_nobody_gives_a_language_to_keeps_the_language_in_force(self):
		with hooked(document_print_language=[LANGUAGE_HOOK], translated_document_text=[TEXT_HOOK]):
			mail = self.template.get_formatted_email({"doctype": "ToDo", "name": "x", "title": "Marie"})
		self.assertEqual(mail["subject"], "Bonjour Marie")

	def test_the_language_is_put_back(self):
		before = frappe.local.lang
		with hooked(document_print_language=[LANGUAGE_HOOK], translated_document_text=[TEXT_HOOK]):
			self.template.get_formatted_email({"doctype": "Note", "name": "x"})
		self.assertEqual(frappe.local.lang, before)

	def test_a_json_string_is_read_as_the_composer_sends_it(self):
		with hooked(document_print_language=[LANGUAGE_HOOK], translated_document_text=[TEXT_HOOK]):
			mail = self.template.get_formatted_email('{"doctype": "Note", "name": "x"}')
		self.assertEqual(mail["subject"], "Guten Tag")


class TestANotification(FrappeTestCase):
	def notification(self, **values):
		doc = frappe.get_doc(
			{"doctype": "Notification", "subject": "s", "document_type": "Note", "channel": "Email", **values}
		)
		doc.name = "ZZ lot 9"
		return doc

	def test_the_print_it_attaches_is_in_the_language_of_the_document(self):
		note = frappe.get_doc({"doctype": "Note", "title": "x", "name": "x"})
		with hooked(document_print_language=[LANGUAGE_HOOK]):
			attachment = self.notification(print_format="Standard").get_print(note)
		self.assertEqual(attachment["lang"], "de")

	def test_a_notification_without_a_print_format_prints_in_the_language_of_the_document_too(self):
		note = frappe.get_doc({"doctype": "Note", "title": "x", "name": "x"})
		with hooked(document_print_language=[LANGUAGE_HOOK]):
			self.assertEqual(self.notification().get_print(note)["lang"], "de")

	def test_upstreams_answer_is_the_fallback(self):
		note = frappe.get_doc({"doctype": "Note", "title": "x", "name": "x"})
		self.assertEqual(self.notification().get_print(note)["lang"], "en")
		note.language = "fr"
		self.assertEqual(self.notification(print_format="Standard").get_print(note)["lang"], "fr")

	def test_the_whole_mail_is_made_in_the_language_of_the_document(self):
		seen = []
		note = frappe.get_doc({"doctype": "Note", "title": "x", "name": "x"})
		notification = self.notification()
		with (
			hooked(document_print_language=[LANGUAGE_HOOK]),
			patch.object(Notification, "_send_an_email", lambda *args: seen.append(frappe.local.lang)),
		):
			notification.send_an_email(note, {})
		self.assertEqual(seen, ["de"])

	def test_a_document_nobody_gives_a_language_to_is_sent_in_the_language_in_force(self):
		seen = []
		todo = frappe.get_doc({"doctype": "ToDo", "description": "x", "name": "x"})
		with (
			hooked(document_print_language=[LANGUAGE_HOOK]),
			patch.object(Notification, "_send_an_email", lambda *args: seen.append(frappe.local.lang)),
		):
			self.notification().send_an_email(todo, {})
		self.assertEqual(seen, [frappe.local.lang])

	def test_the_language_hook_is_the_one_of_lot_7(self):
		with hooked(document_print_language=[LANGUAGE_HOOK]):
			self.assertEqual(document_print_language("Note", "x"), "de")


class TestAUsersMails(FrappeTestCase):
	def user(self, language):
		return frappe.get_doc({"doctype": "User", "email": "zz-lot9@example.com", "first_name": "ZZ", "language": language})

	def test_the_password_reset_mail_is_written_in_the_language_the_apps_give_its_recipient(self):
		seen = []
		with (
			hooked(document_print_language=[USER_HOOK]),
			patch.object(User, "send_login_mail", lambda self, *args, **kwargs: seen.append(frappe.local.lang)),
		):
			self.user("de").password_reset_mail("https://example.com/reset")
		self.assertEqual(seen, ["de"])

	def test_a_user_the_apps_give_no_language_keeps_the_language_in_force(self):
		"""A user whose own language the site does not speak (the hook answers nothing) is mailed as upstream does."""
		seen = []
		with patch.object(User, "send_login_mail", lambda self, *args, **kwargs: seen.append(frappe.local.lang)):
			self.user("de").password_reset_mail("https://example.com/reset")
		self.assertEqual(seen, [frappe.local.lang])

	def test_the_language_is_put_back(self):
		before = frappe.local.lang
		with (
			hooked(document_print_language=[USER_HOOK]),
			patch.object(User, "send_login_mail", lambda self, *args, **kwargs: None),
		):
			self.user("de").password_reset_mail("https://example.com/reset")
		self.assertEqual(frappe.local.lang, before)

	def test_the_decorator_gives_its_method_the_arguments_and_the_name(self):
		@in_the_users_language
		def write(self, first, second=None):
			return (frappe.local.lang, first, second)

		with hooked(document_print_language=[USER_HOOK]):
			self.assertEqual(write(frappe._dict(name="x"), 1, second=2), ("de", 1, 2))
		self.assertEqual(write.__name__, "write")


class TestATranslationThatDoesNotRender(FrappeTestCase):
	def test_a_wording_that_renders_is_used(self):
		self.assertEqual(
			render_with_fallback("Guten Tag {{ title }}", "Bonjour {{ title }}", {"title": "Marie"}),
			"Guten Tag Marie",
		)

	def test_nothing_translated_renders_the_source(self):
		self.assertEqual(
			render_with_fallback(None, "Bonjour {{ title }}", {"title": "Marie"}), "Bonjour Marie"
		)
		self.assertEqual(render_with_fallback("", "Bonjour {{ title }}", {"title": "Marie"}), "Bonjour Marie")

	def test_a_broken_wording_falls_back_to_the_source_and_tells_nobody_on_screen(self):
		shown = len(frappe.local.message_log)
		result = render_with_fallback("Guten Tag {{ title }", "Bonjour {{ title }}", {"title": "Marie"})
		self.assertEqual(result, "Bonjour Marie")
		self.assertEqual(len(frappe.local.message_log), shown)

	def test_a_broken_source_still_raises(self):
		with self.assertRaises(frappe.ValidationError):
			render_with_fallback("Guten Tag {{ title }", "Bonjour {{ title }", {"title": "Marie"})

	def test_an_email_template_with_a_broken_translation_still_renders(self):
		template = frappe.get_doc(
			{
				"doctype": "Email Template",
				"name": "ZZ lot 9",
				"subject": "Bonjour {{ title }}",
				"response": "<p>Merci</p>",
				"use_html": 0,
			}
		)
		template.name = "ZZ lot 9"

		def broken(doctype=None, name=None, fieldname=None, language=None):
			return "Guten Tag {{ title }" if fieldname == "subject" else None

		with patch("frappe.translate.translated_document_text", side_effect=broken):
			mail = template.get_formatted_email({"title": "Marie"})
		self.assertEqual(mail, {"subject": "Bonjour Marie", "message": "<p>Merci</p>"})
