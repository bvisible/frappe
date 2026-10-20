# //// Neoffice — added file (no upstream equivalent): tests of the security fixes taken from upstream
# //// version-15 ahead of the full merge (bvisible/neoffice-maintenance#1748). Each test fails on the code
# //// before its fix. upstream 5e8cf7082e's own test (write_file) is in core/doctype/file/test_file.py.
import os
import tempfile

import frappe
from frappe.tests.utils import FrappeTestCase


class TestNeofficeSecurityBackports(FrappeTestCase):
	def test_an_include_outside_its_app_is_refused(self):
		# GHSA-67rf-pxgh-vfqv, upstream ccf873ada0: "{% include 'app/../…' %}" read any file
		from frappe.model.utils import render_include

		with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as f:
			f.write("neoffice include outside the app")
		self.addCleanup(os.remove, f.name)
		relative = os.path.relpath(f.name, frappe.get_app_path("frappe"))
		self.assertTrue(relative.startswith(".."))

		self.assertRaises(frappe.ValidationError, render_include, f'{{% include "frappe/{relative}" %}}')

	def test_an_include_inside_its_app_still_works(self):
		from frappe.model.utils import render_include

		rendered = render_include('{% include "frappe/public/js/frappe/utils/datatype.js" %}')
		self.assertNotIn("{% include", rendered)

	def test_a_mail_image_is_read_from_the_files_folders_only(self):
		# GHSA-8v98-cfpm-9p67, upstream 16e8b41584: any other path, an absolute one included, was read and sent inline
		from frappe.email.email_body import get_filecontent_from_path

		with tempfile.NamedTemporaryFile("wb", delete=False) as f:
			f.write(b"neoffice mail image outside the files")
		self.addCleanup(os.remove, f.name)
		self.assertIsNone(get_filecontent_from_path(f.name))

		outside = frappe.get_site_path("neoffice_mail_image_test.txt")
		with open(outside, "wb") as f:
			f.write(b"neoffice mail image in the site folder")
		self.addCleanup(os.remove, outside)
		self.assertIsNone(get_filecontent_from_path("files/../../neoffice_mail_image_test.txt"))

	def test_a_mail_image_in_the_public_files_is_still_read(self):
		from frappe.email.email_body import get_filecontent_from_path

		name = f"neoffice-mail-image-{frappe.generate_hash(length=8)}.txt"
		path = frappe.get_site_path("public", "files", name)
		with open(path, "wb") as f:
			f.write(b"neoffice public image")
		self.addCleanup(os.remove, path)
		self.assertEqual(get_filecontent_from_path(f"/files/{name}"), b"neoffice public image")
