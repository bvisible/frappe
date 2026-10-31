# Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

import json
from typing import Any

import frappe
from frappe.model.document import Document
from frappe.utils.jinja import validate_template


class EmailTemplate(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		response: DF.TextEditor | None
		response_html: DF.Code | None
		subject: DF.Data
		use_html: DF.Check

	# end: auto-generated types

	@property
	def response_(self):
		return self.response_html if self.use_html else self.response

	def validate(self):
		validate_template(self.subject)
		validate_template(self.response_)

	# //// Neoffice — multilingual lot 9 (neoffice-maintenance#2037): the subject and the body are the wording of the language in
	# //// force when the apps hold one (translated_document_text), else the template's own text.
	def get_formatted_subject(self, doc):
		text = frappe.translate.translated_document_text("Email Template", self.name, "subject")
		return frappe.translate.render_with_fallback(text, self.subject, doc)

	def get_formatted_response(self, doc):
		# //// Neoffice — lot 9: the body field the template ticks, translated when the apps hold a wording of it
		field = "response_html" if self.use_html else "response"
		text = frappe.translate.translated_document_text("Email Template", self.name, field)
		return frappe.translate.render_with_fallback(text, self.response_, doc)

	def get_formatted_email(self, doc):
		if isinstance(doc, str):
			doc = json.loads(doc)

		# //// Neoffice — lot 9: a mail about a document is written in the language the apps give that document (its own, its
		# //// customer's...), whoever is composing it; no answer keeps the language in force.
		language = None
		if hasattr(doc, "get") and doc.get("doctype"):
			language = frappe.translate.document_print_language(doc.get("doctype"), doc.get("name"), doc)
		with frappe.translate.print_language(language):
			return {
				"subject": self.get_formatted_subject(doc),
				"message": self.get_formatted_response(doc),
			}


@frappe.whitelist()
def get_email_template(template_name: str, doc: str | dict[str, Any]):
	"""Returns the processed HTML of a email template with the given doc"""

	email_template = frappe.get_doc("Email Template", template_name)
	email_template.check_permission("read")
	return email_template.get_formatted_email(doc)
