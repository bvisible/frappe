# //// Neoffice — added file (no upstream equivalent)
# A failed form save wrote the Password fields of its document in clear into frappe.log: the document travels as ONE
# JSON string under `doc`, which sanitized_dict did not read (neoffice-maintenance#2549).
import json
import unittest
from types import SimpleNamespace
from unittest import mock

from frappe.utils import logger


def meta(*password_fields):
	return SimpleNamespace(
		fields=[SimpleNamespace(fieldname=name, fieldtype="Password") for name in password_fields]
		+ [SimpleNamespace(fieldname="email_id", fieldtype="Data")]
	)


class TestTheLogMasksASavedDocument(unittest.TestCase):
	def sanitize(self, form_dict, metas=None):
		metas = metas or {}
		with mock.patch.object(logger.frappe, "get_meta", side_effect=lambda doctype: metas.get(doctype, meta())):
			return logger.sanitized_dict(form_dict)

	def test_a_password_field_of_the_document_is_masked(self):
		doc = {"doctype": "Email Account", "email_id": "box@example.invalid", "password": "s3cret-value"}
		out = self.sanitize({"cmd": "frappe.desk.form.save.savedocs", "doc": json.dumps(doc), "action": "Save"})
		saved = json.loads(out["doc"])
		self.assertEqual(saved["password"], "********")
		self.assertEqual(saved["email_id"], "box@example.invalid")
		self.assertNotIn("s3cret-value", json.dumps(out))

	def test_a_password_field_named_like_nothing_secret_is_masked_by_its_type(self):
		doc = {"doctype": "Till", "pin": "4711", "items": [{"doctype": "Till Row", "code": "8812", "label": "a"}]}
		out = self.sanitize({"doc": json.dumps(doc)}, {"Till": meta("pin"), "Till Row": meta("code")})
		saved = json.loads(out["doc"])
		self.assertEqual(saved["pin"], "********")
		self.assertEqual(saved["items"][0]["code"], "********")
		self.assertEqual(saved["items"][0]["label"], "a")

	def test_a_list_of_documents_and_a_text_that_is_not_json_are_handled(self):
		docs = [{"doctype": "X", "api_secret": "abc"}]
		out = self.sanitize({"docs": json.dumps(docs), "doc": "not json"})
		self.assertEqual(json.loads(out["docs"])[0]["api_secret"], "********")
		self.assertEqual(out["doc"], "not json")

	def test_without_a_database_the_names_still_decide(self):
		with mock.patch.object(logger.frappe, "get_meta", side_effect=RuntimeError("no database")):
			out = logger.sanitized_dict({"doc": json.dumps({"doctype": "X", "smtp_password": "p", "pin": "1"})})
		saved = json.loads(out["doc"])
		self.assertEqual(saved["smtp_password"], "********")
		self.assertEqual(saved["pin"], "1")
