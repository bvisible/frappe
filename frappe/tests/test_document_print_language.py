# //// Neoffice — added file (no upstream equivalent)
"""A document is printed in the language the apps give it, and a caller's own choice wins (multilingual lot 7,
neoffice-maintenance#2035).

Upstream prints in the SESSION's language everywhere on the server, so the PDF of a German order, sent by a clerk working in
French, went out in French. ``document_print_language`` asks the apps (hook of the same name); ``print_language()`` remembers
what a caller chose, so that choice is never replaced. These tests stand a hook in with ``frappe.get_hooks`` patched: they
need no app but frappe.

    bench --site <site> run-tests --module frappe.tests.test_document_print_language
"""

from unittest.mock import patch

from frappe.tests.utils import FrappeTestCase

import frappe
from frappe.translate import document_print_language, explicit_print_language, print_language

HOOK = "frappe.tests.test_document_print_language.german_for_notes"
OTHER_HOOK = "frappe.tests.test_document_print_language.italian_for_everything"
LISTENING_HOOK = "frappe.tests.test_document_print_language.listening"
real_get_hooks = frappe.get_hooks


def german_for_notes(doctype=None, name=None, doc=None):
	return "de" if doctype == "Note" else None


def italian_for_everything(doctype=None, name=None, doc=None):
	return "it"


TOLD = []


def listening(doctype=None, name=None, doc=None, print_format=None):
	TOLD.append((doctype, print_format))
	return None


def hooked(*methods):
	def get_hooks(hook=None, *args, **kwargs):
		if hook == "document_print_language":
			return list(methods)
		return real_get_hooks(hook, *args, **kwargs)

	return patch("frappe.get_hooks", side_effect=get_hooks)


class TestPrintLanguageIsRemembered(FrappeTestCase):
	def test_a_choice_is_in_force_inside_the_block_only(self):
		self.assertIsNone(explicit_print_language())
		with print_language("de"):
			self.assertEqual(explicit_print_language(), "de")
			self.assertEqual(frappe.local.lang, "de")
		self.assertIsNone(explicit_print_language())

	def test_the_session_language_chosen_again_is_still_a_choice(self):
		with print_language(frappe.local.lang):
			self.assertEqual(explicit_print_language(), frappe.local.lang)
		self.assertIsNone(explicit_print_language())

	def test_no_language_is_no_choice(self):
		with print_language(None):
			self.assertIsNone(explicit_print_language())

	def test_a_block_that_raises_gives_the_language_and_the_choice_back(self):
		before = frappe.local.lang
		with self.assertRaises(ZeroDivisionError):
			with print_language("de"):
				1 / 0
		self.assertEqual(frappe.local.lang, before)
		self.assertIsNone(explicit_print_language())

	def test_choices_nest(self):
		with print_language("de"):
			with print_language("it"):
				self.assertEqual(explicit_print_language(), "it")
			self.assertEqual(explicit_print_language(), "de")
			self.assertEqual(frappe.local.lang, "de")


class TestTheAppsDecide(FrappeTestCase):
	def test_without_a_hook_nothing_changes(self):
		self.assertIsNone(document_print_language("Note", "x"))

	def test_the_hook_is_called_with_the_document_it_is_about(self):
		with hooked(HOOK):
			self.assertEqual(document_print_language("Note", "x"), "de")
			self.assertIsNone(document_print_language("ToDo", "x"))

	def test_the_first_answer_wins_and_a_silent_hook_is_skipped(self):
		with hooked(HOOK, OTHER_HOOK):
			self.assertEqual(document_print_language("Note", "x"), "de")
			self.assertEqual(document_print_language("ToDo", "x"), "it")


class TestTheHookIsToldTheFormat(FrappeTestCase):
	"""A format has a default language of its own (the Oslo formats say French): the apps need to know which one prints."""

	def setUp(self):
		TOLD.clear()

	def test_the_format_is_passed_to_the_hook(self):
		with hooked(LISTENING_HOOK):
			document_print_language("Note", "x", print_format="Oslo Note")
		self.assertEqual(TOLD, [("Note", "Oslo Note")])

	def test_a_hook_that_does_not_take_a_format_still_works(self):
		with hooked(HOOK):
			self.assertEqual(document_print_language("Note", "x", print_format="Oslo Note"), "de")

	def test_get_print_names_its_format(self):
		with (
			hooked(LISTENING_HOOK),
			patch(
				"frappe.website.serve.get_response_without_exception_handling",
				return_value=type("Response", (), {"data": b"<p>printed</p>"})(),
			),
		):
			frappe.get_print("Note", "x", print_format="Oslo Note")
		self.assertEqual(TOLD, [("Note", "Oslo Note")])

	def test_attach_print_names_its_format(self):
		doc = frappe.get_doc({"doctype": "Note", "title": "Attached", "name": "Attached"})
		with (
			hooked(LISTENING_HOOK),
			patch("frappe.get_print", return_value="<p>printed</p>"),
			patch("frappe.get_cached_doc", return_value=frappe._dict(print_format_builder_beta=0)),
		):
			frappe.attach_print("Note", "Attached", doc=doc, print_format="Oslo Note")
		self.assertEqual(TOLD, [("Note", "Oslo Note")])


class _Response:
	def __init__(self, data):
		self.data = data


class TestGetPrintFollowsTheDocument(FrappeTestCase):
	"""``get_print`` renders through the website's ``printview`` page: stand that page in and record the language it ran in."""

	def render(self, **kwargs):
		seen = []

		def page(path, status):
			seen.append((frappe.local.lang, explicit_print_language()))
			return _Response(b"<p>printed</p>")

		with patch("frappe.website.serve.get_response_without_exception_handling", side_effect=page):
			html = frappe.get_print("Note", "x", **kwargs)
		self.assertIn("printed", html)
		return seen

	def test_a_document_is_printed_in_its_own_language(self):
		session = frappe.local.lang
		with hooked(HOOK):
			((lang, _chosen),) = self.render()
		self.assertEqual(lang, "de")
		self.assertEqual(frappe.local.lang, session, "the session's language is put back")
		self.assertIsNone(explicit_print_language())

	def test_a_language_a_caller_chose_is_never_replaced(self):
		with hooked(HOOK):
			with print_language("it"):
				((lang, _chosen),) = self.render()
		self.assertEqual(lang, "it")

	def test_without_a_hook_it_prints_in_the_session_language(self):
		session = frappe.local.lang
		((lang, chosen),) = self.render()
		self.assertEqual(lang, session)
		self.assertIsNone(chosen)

	def test_a_document_in_the_session_language_is_one_render(self):
		with patch.object(
			frappe.translate, "document_print_language", side_effect=lambda *a, **k: frappe.local.lang
		):
			seen = self.render()
		self.assertEqual(len(seen), 1, "asking for the language we are already in does not render twice")


class TestThePortalOpensThePrintViewWithNoLang(FrappeTestCase):
	"""``/printview`` is the customer portal's link to the PDF of an order: it carries no ``_lang``."""

	def context(self, form):
		from frappe.www import printview

		seen = []

		def rendered(doc, **kwargs):
			seen.append(frappe.local.lang)
			return "<p>body</p>"

		doc = frappe.get_doc({"doctype": "Note", "title": "Print language", "name": "Print language"})
		frappe.form_dict.update({"doctype": "Note", "name": "Print language", "doc": doc, **form})
		self.addCleanup(
			lambda: [frappe.form_dict.pop(key, None) for key in ("doctype", "name", "doc", "_lang")]
		)
		with (
			patch.object(printview, "get_rendered_template", side_effect=rendered),
			patch.object(printview, "make_access_log"),
			patch.object(printview, "get_print_style", return_value=""),
		):
			context = printview.get_context(frappe._dict())
		return context, seen

	def test_the_page_is_in_the_documents_language(self):
		session = frappe.local.lang
		with hooked(HOOK):
			context, seen = self.context({})
		self.assertEqual(seen, ["de"])
		self.assertEqual(context["lang"], "de")
		self.assertEqual(frappe.local.lang, session)

	def test_a_lang_in_the_address_is_a_choice(self):
		with hooked(HOOK):
			context, seen = self.context({"_lang": frappe.local.lang})
		self.assertEqual(seen, [frappe.local.lang])
		self.assertEqual(context["lang"], frappe.local.lang)

	def test_a_language_a_caller_chose_is_kept(self):
		with hooked(HOOK), print_language("it"):
			context, seen = self.context({})
		self.assertEqual(seen, ["it"])
		self.assertEqual(context["lang"], "it")

	def test_without_a_hook_the_page_is_in_the_visitors_language(self):
		_context, seen = self.context({})
		self.assertEqual(seen, [frappe.local.lang])


class TestAttachPrintFollowsTheDocument(FrappeTestCase):
	"""``attach_print`` is what the mails attach: it prints through ``get_print``, or through weasyprint on its own."""

	def attach(self, **kwargs):
		seen = []

		def printed(doctype, name, **kw):
			seen.append((frappe.local.lang, explicit_print_language()))
			return b"%PDF" if kw.get("as_pdf") else "<p>printed</p>"

		doc = frappe.get_doc({"doctype": "Note", "title": "Attached", "name": "Attached"})
		with patch("frappe.get_print", side_effect=printed):
			frappe.attach_print("Note", "Attached", doc=doc, **kwargs)
		return seen

	def test_a_mail_attachment_is_in_the_documents_language(self):
		session = frappe.local.lang
		with hooked(HOOK):
			((lang, chosen),) = self.attach()
		self.assertEqual((lang, chosen), ("de", "de"))
		self.assertEqual(frappe.local.lang, session)

	def test_the_language_a_caller_names_wins(self):
		with hooked(HOOK):
			((lang, _chosen),) = self.attach(lang="it")
		self.assertEqual(lang, "it")

	def test_without_a_hook_it_is_the_session_language_as_before(self):
		((lang, _chosen),) = self.attach()
		self.assertEqual(lang, frappe.local.lang)
