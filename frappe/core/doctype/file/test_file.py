# Copyright (c) 2022, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE
import base64
import json
import os
import shutil
import tempfile
import zipfile
from contextlib import contextmanager
from typing import TYPE_CHECKING
from unittest.mock import patch

import frappe
from frappe import _
from frappe.core.api.file import (
	create_new_folder,
	get_attached_images,
	get_files_in_folder,
	move_file,
	unzip_file,
)
from frappe.core.doctype.file.exceptions import FileTypeNotAllowed
from frappe.core.doctype.file.utils import get_corrupted_image_msg, get_extension
from frappe.desk.form.utils import add_comment
from frappe.exceptions import ValidationError
from frappe.tests.utils import FrappeTestCase, change_settings
from frappe.utils import get_files_path, set_request

if TYPE_CHECKING:
	from frappe.core.doctype.file.file import File

test_content1 = "Hello"
test_content2 = "Hello World"

# //// Neoffice — no `test_dependencies = ["User"]` here: on a site with ERPNext, User's links pull ERPNext's
# //// test records, which die there before any test runs. TestAttachmentsAccess makes the users it needs.


def make_test_doc(ignore_permissions=False):
	d = frappe.new_doc("ToDo")
	d.description = "Test"
	d.assigned_by = frappe.session.user
	d.save(ignore_permissions)
	return d.doctype, d.name


@contextmanager
def make_test_image_file(private=False):
	file_path = frappe.get_app_path("frappe", "tests/data/sample_image_for_optimization.jpg")
	with open(file_path, "rb") as f:
		file_content = f.read()

	test_file = frappe.get_doc(
		{
			"doctype": "File",
			"file_name": "sample_image_for_optimization.jpg",
			"content": file_content,
			"is_private": private,
		}
	).insert()
	# remove those flags
	_test_file: File = frappe.get_doc("File", test_file.name)

	try:
		yield _test_file
	finally:
		_test_file.delete()


class TestSimpleFile(FrappeTestCase):
	def setUp(self):
		self.attached_to_doctype, self.attached_to_docname = make_test_doc()
		self.test_content = test_content1
		_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "test1.txt",
				"attached_to_doctype": self.attached_to_doctype,
				"attached_to_name": self.attached_to_docname,
				"content": self.test_content,
			}
		)
		_file.save()
		self.saved_file_url = _file.file_url

	def test_save(self):
		_file = frappe.get_doc("File", {"file_url": self.saved_file_url})
		content = _file.get_content()
		self.assertEqual(content, self.test_content)


class TestFSRollbacks(FrappeTestCase):
	def test_rollback_from_file_system(self):
		file_name = content = frappe.generate_hash()
		file = frappe.new_doc("File", file_name=file_name, content=content).insert()
		self.assertTrue(file.exists_on_disk())

		frappe.db.rollback()
		self.assertFalse(file.exists_on_disk())


class TestWriteFileContainment(FrappeTestCase):
	def test_write_file_rejects_target_outside_files_dir(self):
		from frappe.utils.file_manager import write_file

		files_path = get_files_path(is_private=1)
		bad_names = (
			"../../../../ESCAPE_TEST.txt",  # parent traversal
			"sub/../../ESCAPE_TEST.txt",  # traversal via a nested segment
			"/tmp/ESCAPE_TEST.txt",  # absolute path outside the files dir
			"subdir/ESCAPE_TEST.txt",  # nested name: not a direct child of the files dir
		)
		for fname in bad_names:
			with self.subTest(fname=fname):
				# where the bytes would land if the target were not confined
				would_be = os.path.realpath(os.path.join(files_path, fname))
				self.assertRaises(ValidationError, write_file, b"data\n", fname, is_private=1)
				self.assertFalse(os.path.exists(would_be))

	def test_write_file_allows_plain_basename(self):
		from frappe.utils.file_manager import write_file

		content = b"safe content\n"
		for is_private in (0, 1):
			with self.subTest(is_private=is_private):
				fname = f"{frappe.generate_hash()}.txt"
				write_file(content, fname, is_private=is_private)

				on_disk = get_files_path(fname, is_private=is_private)
				self.addCleanup(lambda p=on_disk: os.path.exists(p) and os.remove(p))
				self.assertEqual(
					os.path.realpath(os.path.dirname(on_disk)),
					os.path.realpath(get_files_path(is_private=is_private)),
				)
				with open(on_disk, "rb") as f:
					self.assertEqual(f.read(), content)


class TestExtensionValidations(FrappeTestCase):
	@change_settings("System Settings", {"allowed_file_extensions": "JPG\nCSV"})
	def test_allowed_extension(self):
		set_request(method="POST", path="/")
		file_name = content = frappe.generate_hash()
		bad_file = frappe.new_doc("File", file_name=f"{file_name}.png", content=content)
		self.assertRaises(FileTypeNotAllowed, bad_file.insert)

		bad_file = frappe.new_doc("File", file_name=f"{file_name}.csv", content=content).insert()
		frappe.db.rollback()
		self.assertFalse(bad_file.exists_on_disk())

	@change_settings("System Settings", {"allowed_file_extensions": "JPG\nCSV"})
	def test_allowlist_blocks_extension_without_known_mimetype(self):
		set_request(method="POST", path="/")
		file_name = content = frappe.generate_hash()
		bad_file = frappe.new_doc("File", file_name=f"{file_name}.phtml", content=content)
		self.assertRaises(FileTypeNotAllowed, bad_file.insert)


class TestBase64File(FrappeTestCase):
	def setUp(self):
		self.attached_to_doctype, self.attached_to_docname = make_test_doc()
		self.test_content = base64.b64encode(test_content1.encode("utf-8"))
		_file: File = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "test_base64.txt",
				"attached_to_doctype": self.attached_to_doctype,
				"attached_to_name": self.attached_to_docname,
				"content": self.test_content,
				"decode": True,
			}
		)
		_file.save()
		self.saved_file_url = _file.file_url

	def test_saved_content(self):
		_file = frappe.get_doc("File", {"file_url": self.saved_file_url})
		content = _file.get_content()
		self.assertEqual(content, test_content1)


class TestSameFileName(FrappeTestCase):
	def test_saved_content(self):
		self.attached_to_doctype, self.attached_to_docname = make_test_doc()
		self.test_content1 = test_content1
		self.test_content2 = test_content2
		_file1 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "testing.txt",
				"attached_to_doctype": self.attached_to_doctype,
				"attached_to_name": self.attached_to_docname,
				"content": self.test_content1,
			}
		)
		_file1.save()
		_file2 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "testing.txt",
				"attached_to_doctype": self.attached_to_doctype,
				"attached_to_name": self.attached_to_docname,
				"content": self.test_content2,
			}
		)
		_file2.save()
		self.saved_file_url1 = _file1.file_url
		self.saved_file_url2 = _file2.file_url

		_file = frappe.get_doc("File", {"file_url": self.saved_file_url1})
		content1 = _file.get_content()
		self.assertEqual(content1, self.test_content1)
		_file = frappe.get_doc("File", {"file_url": self.saved_file_url2})
		content2 = _file.get_content()
		self.assertEqual(content2, self.test_content2)

	def test_saved_content_private(self):
		_file1 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "testing-private.txt",
				"content": test_content1,
				"is_private": 1,
			}
		).insert()
		_file2 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "testing-private.txt",
				"content": test_content2,
				"is_private": 1,
			}
		).insert()

		_file = frappe.get_doc("File", {"file_url": _file1.file_url})
		self.assertEqual(_file.get_content(), test_content1)

		_file = frappe.get_doc("File", {"file_url": _file2.file_url})
		self.assertEqual(_file.get_content(), test_content2)


class TestSameContent(FrappeTestCase):
	def setUp(self):
		self.attached_to_doctype1, self.attached_to_docname1 = make_test_doc()
		self.attached_to_doctype2, self.attached_to_docname2 = make_test_doc()
		self.test_content1 = test_content1
		self.test_content2 = test_content1
		self.orig_filename = "hello.txt"
		self.dup_filename = "hello2.txt"
		_file1 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": self.orig_filename,
				"attached_to_doctype": self.attached_to_doctype1,
				"attached_to_name": self.attached_to_docname1,
				"content": self.test_content1,
			}
		)
		_file1.save()

		_file2 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": self.dup_filename,
				"attached_to_doctype": self.attached_to_doctype2,
				"attached_to_name": self.attached_to_docname2,
				"content": self.test_content2,
			}
		)

		_file2.save()

	def test_saved_content(self):
		self.assertFalse(os.path.exists(get_files_path(self.dup_filename)))

	def test_attachment_limit(self):
		doctype, docname = make_test_doc()
		from frappe.custom.doctype.property_setter.property_setter import make_property_setter

		limit_property = make_property_setter("ToDo", None, "max_attachments", 1, "int", for_doctype=True)
		file1 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "test-attachment",
				"attached_to_doctype": doctype,
				"attached_to_name": docname,
				"content": "test",
			}
		)

		file1.insert()

		file2 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "test-attachment",
				"attached_to_doctype": doctype,
				"attached_to_name": docname,
				"content": "test2",
			}
		)

		self.assertRaises(frappe.exceptions.AttachmentLimitReached, file2.insert)
		limit_property.delete()
		frappe.clear_cache(doctype="ToDo")

	def test_create_attachment_copy(self):
		doctype, docname = make_test_doc()
		source_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": f"existing-file-{frappe.generate_hash(length=8)}.txt",
				"content": "Existing attachment content",
			}
		).insert()
		comment_count_before = frappe.db.count(
			"Comment", {"reference_doctype": doctype, "reference_name": docname}
		)

		copied_file = source_file.create_attachment_copy(doctype, docname)
		comment_count_after = frappe.db.count(
			"Comment", {"reference_doctype": doctype, "reference_name": docname}
		)

		self.assertNotEqual(copied_file.name, source_file.name)
		self.assertEqual(copied_file.file_url, source_file.file_url)
		self.assertEqual(copied_file.attached_to_doctype, doctype)
		self.assertEqual(copied_file.attached_to_name, docname)
		self.assertEqual(
			copied_file.folder,
			frappe.db.get_value("File", {"is_attachments_folder": 1}),
		)
		self.assertEqual(comment_count_after, comment_count_before + 1)

	def test_create_attachment_copy_respects_attachment_limit(self):
		doctype, docname = make_test_doc()
		from frappe.custom.doctype.property_setter.property_setter import make_property_setter

		limit_property = make_property_setter("ToDo", None, "max_attachments", 1, "int", for_doctype=True)
		source_file_1 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": f"existing-limit-file-{frappe.generate_hash(length=8)}.txt",
				"content": "Existing attachment content 1",
			}
		).insert()
		source_file_2 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": f"existing-limit-file-{frappe.generate_hash(length=8)}.txt",
				"content": "Existing attachment content 2",
			}
		).insert()

		try:
			source_file_1.create_attachment_copy(doctype, docname)
			self.assertRaises(
				frappe.exceptions.AttachmentLimitReached,
				source_file_2.create_attachment_copy,
				doctype,
				docname,
			)
		finally:
			limit_property.delete()
			frappe.clear_cache(doctype="ToDo")


class TestFile(FrappeTestCase):
	def setUp(self):
		frappe.set_user("Administrator")
		self.delete_test_data()
		self.upload_file()

	def tearDown(self):
		try:
			frappe.get_doc("File", {"file_name": "file_copy.txt"}).delete()
		except frappe.DoesNotExistError:
			pass

	def delete_test_data(self):
		test_file_data = frappe.get_all(
			"File",
			pluck="name",
			filters={"is_home_folder": 0, "is_attachments_folder": 0},
			order_by="creation desc",
		)
		for f in test_file_data:
			frappe.delete_doc("File", f)

	def upload_file(self):
		_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "file_copy.txt",
				"attached_to_name": "",
				"attached_to_doctype": "",
				"folder": self.get_folder("Test Folder 1", "Home").name,
				"content": "Testing file copy example.",
			}
		)
		_file.save()
		self.saved_folder = _file.folder
		self.saved_name = _file.name
		self.saved_filename = get_files_path(_file.file_name)

	def get_folder(self, folder_name, parent_folder="Home"):
		return frappe.get_doc(
			{"doctype": "File", "file_name": _(folder_name), "is_folder": 1, "folder": _(parent_folder)}
		).insert()

	def tests_after_upload(self):
		self.assertEqual(self.saved_folder, _("Home/Test Folder 1"))
		file_folder = frappe.db.get_value("File", self.saved_name, "folder")
		self.assertEqual(file_folder, _("Home/Test Folder 1"))

	def test_file_copy(self):
		folder = self.get_folder("Test Folder 2", "Home")

		file = frappe.get_doc("File", {"file_name": "file_copy.txt"})
		move_file([{"name": file.name}], folder.name, file.folder)
		file = frappe.get_doc("File", {"file_name": "file_copy.txt"})

		self.assertEqual(_("Home/Test Folder 2"), file.folder)

	def test_folder_depth(self):
		result1 = self.get_folder("d1", "Home")
		self.assertEqual(result1.name, "Home/d1")
		result2 = self.get_folder("d2", "Home/d1")
		self.assertEqual(result2.name, "Home/d1/d2")
		result3 = self.get_folder("d3", "Home/d1/d2")
		self.assertEqual(result3.name, "Home/d1/d2/d3")
		result4 = self.get_folder("d4", "Home/d1/d2/d3")
		_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "folder_copy.txt",
				"attached_to_name": "",
				"attached_to_doctype": "",
				"folder": result4.name,
				"content": "Testing folder copy example",
			}
		)
		_file.save()

	def test_folder_copy(self):
		folder = self.get_folder("Test Folder 2", "Home")
		folder = self.get_folder("Test Folder 3", "Home/Test Folder 2")
		_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "folder_copy.txt",
				"attached_to_name": "",
				"attached_to_doctype": "",
				"folder": folder.name,
				"content": "Testing folder copy example",
			}
		)
		_file.save()

		move_file([{"name": folder.name}], "Home/Test Folder 1", folder.folder)

		file = frappe.get_doc("File", {"file_name": "folder_copy.txt"})
		file_copy_txt = frappe.get_value("File", {"file_name": "file_copy.txt"})
		if file_copy_txt:
			frappe.get_doc("File", file_copy_txt).delete()

		self.assertEqual(_("Home/Test Folder 1/Test Folder 3"), file.folder)

	def test_default_folder(self):
		d = frappe.get_doc({"doctype": "File", "file_name": _("Test_Folder"), "is_folder": 1})
		d.save()
		self.assertEqual(d.folder, "Home")

	def test_folder_file_url_is_always_empty(self):
		folder = self.get_folder("Test Folder URL", "Home")
		self.assertFalse(folder.file_url)

		folder.file_url = "/private/files/somewhere.txt"
		self.assertRaises(ValidationError, folder.save)

		self.assertRaises(
			ValidationError,
			frappe.get_doc(
				{
					"doctype": "File",
					"file_name": "another_folder",
					"is_folder": 1,
					"folder": "Home",
					"file_url": "/private/files/somewhere_else.txt",
				}
			).insert,
		)

	def test_on_delete(self):
		file = frappe.get_doc("File", {"file_name": "file_copy.txt"})
		file.delete()

		self.assertEqual(frappe.db.get_value("File", _("Home/Test Folder 1"), "file_size"), 0)

		folder = self.get_folder("Test Folder 3", "Home/Test Folder 1")
		_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "folder_copy.txt",
				"attached_to_name": "",
				"attached_to_doctype": "",
				"folder": folder.name,
				"content": "Testing folder copy example",
			}
		)
		_file.save()

		folder = frappe.get_doc("File", "Home/Test Folder 1/Test Folder 3")
		self.assertRaises(ValidationError, folder.delete)

	def test_same_file_url_update(self):
		attached_to_doctype1, attached_to_docname1 = make_test_doc()
		attached_to_doctype2, attached_to_docname2 = make_test_doc()

		file1 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "file1.txt",
				"attached_to_doctype": attached_to_doctype1,
				"attached_to_name": attached_to_docname1,
				"is_private": 1,
				"content": test_content1,
			}
		).insert()

		file2 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "file2.txt",
				"attached_to_doctype": attached_to_doctype2,
				"attached_to_name": attached_to_docname2,
				"is_private": 1,
				"content": test_content1,
			}
		).insert()

		self.assertEqual(file1.is_private, file2.is_private, 1)
		self.assertEqual(file1.file_url, file2.file_url)
		self.assertTrue(os.path.exists(file1.get_full_path()))

		file1.is_private = 0
		file1.save()

		file2 = frappe.get_doc("File", file2.name)

		self.assertEqual(file1.is_private, file2.is_private, 0)
		self.assertEqual(file1.file_url, file2.file_url)
		self.assertTrue(os.path.exists(file2.get_full_path()))

	def test_parent_directory_validation_in_file_url(self):
		file1 = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "parent_dir.txt",
				"is_private": 1,
				"content": test_content1,
			}
		).insert()

		file1.file_url = "/private/files/../test.txt"
		self.assertRaises(ValidationError, file1.save)

		# No validation to see if file exists
		file1.reload()
		file1.file_url = "/private/files/parent_dir2.txt"
		self.assertRaises(OSError, file1.save)

	def test_file_url_validation(self):
		test_file: File = frappe.new_doc("File")
		test_file.update({"file_name": "logo", "file_url": "https://frappe.io/files/frappe.png"})

		self.assertIsNone(test_file.validate())

		# bad path
		test_file.file_url = "/usr/bin/man"
		self.assertRaisesRegex(
			ValidationError, f"Cannot access file path {test_file.file_url}", test_file.validate
		)

		test_file.file_url = None
		test_file.file_name = "/usr/bin/man"
		self.assertRaisesRegex(ValidationError, "There is some problem with the file url", test_file.validate)

		test_file.file_url = None
		test_file.file_name = "_file"
		self.assertRaisesRegex(IOError, "does not exist", test_file.validate)

		test_file.file_url = None
		test_file.file_name = "/private/files/_file"
		self.assertRaisesRegex(ValidationError, "File name cannot have", test_file.validate)

	def test_make_thumbnail(self):
		# test web image
		test_file: File = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "logo",
				"file_url": frappe.utils.get_url("/_test/assets/image.jpg"),
			}
		).insert(ignore_permissions=True)

		test_file.make_thumbnail()
		self.assertEqual(test_file.thumbnail_url, "/files/image_small.jpg")

		# test web image without extension
		test_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "logo",
				"file_url": frappe.utils.get_url("/_test/assets/image"),
			}
		).insert(ignore_permissions=True)

		test_file.make_thumbnail()
		self.assertTrue(test_file.thumbnail_url.endswith("_small.jpg"))

		# test local image
		test_file.db_set("thumbnail_url", None)
		test_file.reload()
		test_file.file_url = "/files/image_small.jpg"
		test_file.make_thumbnail(suffix="xs", crop=True)
		self.assertEqual(test_file.thumbnail_url, "/files/image_small_xs.jpg")

		frappe.clear_messages()
		test_file.db_set("thumbnail_url", None)
		test_file.reload()
		test_file.file_url = frappe.utils.get_url("unknown.jpg")
		test_file.make_thumbnail(suffix="xs")
		self.assertEqual(
			frappe.message_log[0].get("message"),
			f"File '{frappe.utils.get_url('unknown.jpg')}' not found",
		)
		self.assertEqual(test_file.thumbnail_url, None)

	def test_file_unzip(self):
		file_path = frappe.get_app_path("frappe", "www/_test/assets/file.zip")
		public_file_path = frappe.get_site_path("public", "files")
		try:
			import shutil

			shutil.copy(file_path, public_file_path)
		except Exception:
			pass

		test_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_url": "/files/file.zip",
			}
		).insert(ignore_permissions=True)

		self.assertListEqual(
			[file.file_name for file in unzip_file(test_file.name)],
			["css_asset.css", "image.jpg", "js_asset.min.js"],
		)

		test_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_url": frappe.utils.get_url("/_test/assets/image.jpg"),
			}
		).insert(ignore_permissions=True)
		self.assertRaisesRegex(ValidationError, "not a zip file", test_file.unzip)

	def test_file_unzip_respects_dedicated_extract_size_setting(self):
		file_path = frappe.get_app_path("frappe", "www/_test/assets/file.zip")
		public_file_path = frappe.get_site_path("public", "files")
		try:
			shutil.copy(file_path, public_file_path)
		except Exception:
			pass

		test_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_url": "/files/file.zip",
			}
		).insert(ignore_permissions=True)
		self.addCleanup(test_file.delete)

		file_count_before = frappe.db.count("File")

		# a dedicated, tighter zip-extraction budget must be enforced even though
		# max_file_size (used for ordinary uploads) stays at its generous default
		with patch.dict(frappe.conf, {"max_zip_extract_size": 1000}):
			self.assertRaisesRegex(ValidationError, "maximum allowed size", test_file.unzip)

		self.assertTrue(frappe.db.exists("File", test_file.name))
		self.assertEqual(frappe.db.count("File"), file_count_before)

	def test_file_unzip_requires_read_permission(self):
		file_path = frappe.get_app_path("frappe", "www/_test/assets/file.zip")
		with open(file_path, "rb") as f:
			zip_content = f.read()

		try:
			frappe.set_user("test@example.com")
			test_file = frappe.get_doc(
				{
					"doctype": "File",
					"file_name": "file.zip",
					"content": zip_content,
					"is_private": 1,
				}
			).insert()

			file_count_before = frappe.db.count("File")

			# block unzip
			frappe.set_user("test4@example.com")
			self.assertRaises(frappe.PermissionError, unzip_file, test_file.name)
			self.assertTrue(frappe.db.exists("File", test_file.name))
			self.assertEqual(frappe.db.count("File"), file_count_before)

			# allow unzip
			frappe.set_user("test@example.com")
			self.assertListEqual(
				[file.file_name for file in unzip_file(test_file.name)],
				["css_asset.css", "image.jpg", "js_asset.min.js"],
			)
		finally:
			frappe.set_user("Administrator")

	def test_file_unzip_rolls_back_children_on_mid_extraction_failure(self):
		fixture_dir = tempfile.mkdtemp()
		zip_path = os.path.join(fixture_dir, "corrupt.zip")
		with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_STORED) as zf:
			zf.writestr("a.txt", "hello-a")
			zf.writestr("b.txt", "hello-b")
			zf.writestr("c.txt", "hello-c")

		# flip a byte in the last member's stored (uncompressed) data so it fails
		# its CRC check on read, without touching the central directory metadata
		with open(zip_path, "rb") as f:
			data = bytearray(f.read())
		corrupt_offset = data.rfind(b"hello-c")
		self.assertNotEqual(corrupt_offset, -1)
		data[corrupt_offset] ^= 0xFF
		with open(zip_path, "wb") as f:
			f.write(data)

		public_file_path = frappe.get_site_path("public", "files")
		shutil.copy(zip_path, public_file_path)

		test_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_url": "/files/corrupt.zip",
			}
		).insert(ignore_permissions=True)
		self.addCleanup(test_file.delete)

		file_count_before = frappe.db.count("File")

		# a.txt and b.txt extract fine and get saved before c.txt fails its CRC check;
		# the whole call must still roll back to a clean no-op
		self.assertRaisesRegex(ValidationError, "not a valid zip file", test_file.unzip)

		self.assertTrue(frappe.db.exists("File", test_file.name))
		self.assertEqual(frappe.db.count("File"), file_count_before)
		self.assertFalse(frappe.db.exists("File", {"file_name": "a.txt"}))
		self.assertFalse(frappe.db.exists("File", {"file_name": "b.txt"}))

	def test_create_file_without_file_url(self):
		test_file = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "logo",
				"content": "frappe",
			}
		).insert()
		assert test_file is not None

	def test_symlinked_files_folder(self):
		files_dir = os.path.abspath(get_files_path())
		with convert_to_symlink(files_dir):
			file = frappe.get_doc(
				{
					"doctype": "File",
					"file_name": "symlinked_folder_test.txt",
					"content": "42",
				}
			)
			file.save()
			file.content = ""
			file._content = ""
			file.save().reload()
			self.assertIn("42", file.get_content())

	def test_guest_upload_to_non_allowed_doctype(self):
		"""Verify Guest cannot upload to a restricted DocType."""
		from werkzeug.test import EnvironBuilder
		from werkzeug.wrappers import Request

		from frappe.handler import upload_file

		old_allow_guests = frappe.db.get_single_value("System Settings", "allow_guests_to_upload_files")
		old_allowed_doctypes = frappe.db.get_single_value(
			"System Settings", "allowed_doctypes_for_guest_uploads"
		)

		frappe.db.set_single_value("System Settings", "allow_guests_to_upload_files", 1)
		frappe.db.set_single_value("System Settings", "allowed_doctypes_for_guest_uploads", "ToDo")

		builder = EnvironBuilder(path="/", base_url="http://localhost")
		frappe.local.request = Request(builder.get_environ())

		frappe.set_user("Guest")
		frappe.form_dict.doctype = "User"
		frappe.form_dict.docname = "Administrator"

		try:
			self.assertRaises(frappe.PermissionError, upload_file)
		finally:
			frappe.db.set_single_value("System Settings", "allow_guests_to_upload_files", old_allow_guests)
			frappe.db.set_single_value(
				"System Settings", "allowed_doctypes_for_guest_uploads", old_allowed_doctypes
			)

			frappe.set_user("Administrator")
			frappe.form_dict.pop("doctype", None)
			frappe.form_dict.pop("docname", None)
			if hasattr(frappe.local, "request"):
				del frappe.local.request

	def test_guest_upload_to_allowed_doctype(self):
		"""Verify Guest can upload to an explicitly whitelisted DocType."""
		from werkzeug.test import EnvironBuilder
		from werkzeug.wrappers import Request

		from frappe.handler import upload_file

		old_allow_guests = frappe.db.get_single_value("System Settings", "allow_guests_to_upload_files")
		old_allowed_doctypes = frappe.db.get_single_value(
			"System Settings", "allowed_doctypes_for_guest_uploads"
		)

		frappe.db.set_single_value("System Settings", "allow_guests_to_upload_files", 1)
		frappe.db.set_single_value("System Settings", "allowed_doctypes_for_guest_uploads", "User\nToDo")

		builder = EnvironBuilder(path="/", base_url="http://localhost")
		frappe.local.request = Request(builder.get_environ())

		frappe.set_user("Administrator")
		todo = frappe.get_doc({"doctype": "ToDo", "description": "Test Target"}).insert()

		frappe.set_user("Guest")
		frappe.form_dict.doctype = "ToDo"
		frappe.form_dict.docname = todo.name
		frappe.form_dict.file_url = "https://frappe.io/assets/img/logo.png"
		frappe.form_dict.file_name = "guest_logo.png"

		file_doc = None
		try:
			file_doc = upload_file()
			self.assertEqual(file_doc.attached_to_name, todo.name)
		finally:
			frappe.db.set_single_value("System Settings", "allow_guests_to_upload_files", old_allow_guests)
			frappe.db.set_single_value(
				"System Settings", "allowed_doctypes_for_guest_uploads", old_allowed_doctypes
			)

			frappe.set_user("Administrator")
			if file_doc:
				file_doc.delete()
			todo.delete()

			frappe.form_dict.pop("doctype", None)
			frappe.form_dict.pop("docname", None)
			frappe.form_dict.pop("file_url", None)
			frappe.form_dict.pop("file_name", None)

			if hasattr(frappe.local, "request"):
				del frappe.local.request

	def test_guest_upload_for_empty_whitelist(self):
		"""Verify Guest can upload anywhere if the configuration whitelist string is left completely empty."""
		from werkzeug.test import EnvironBuilder
		from werkzeug.wrappers import Request

		from frappe.handler import upload_file

		old_allow_guests = frappe.db.get_single_value("System Settings", "allow_guests_to_upload_files")
		old_allowed_doctypes = frappe.db.get_single_value(
			"System Settings", "allowed_doctypes_for_guest_uploads"
		)

		frappe.db.set_single_value("System Settings", "allow_guests_to_upload_files", 1)
		frappe.db.set_single_value("System Settings", "allowed_doctypes_for_guest_uploads", "")

		builder = EnvironBuilder(path="/", base_url="http://localhost")
		frappe.local.request = Request(builder.get_environ())

		frappe.set_user("Guest")
		frappe.form_dict.doctype = "User"
		frappe.form_dict.docname = "Administrator"
		frappe.form_dict.file_url = "https://frappe.io/assets/img/logo.png"
		frappe.form_dict.file_name = "guest_fallback.png"

		file_doc = None
		try:
			file_doc = upload_file()
			self.assertEqual(file_doc.attached_to_name, "Administrator")
		finally:
			frappe.db.set_single_value("System Settings", "allow_guests_to_upload_files", old_allow_guests)
			frappe.db.set_single_value(
				"System Settings", "allowed_doctypes_for_guest_uploads", old_allowed_doctypes
			)

			frappe.set_user("Administrator")
			if file_doc:
				file_doc.delete()

			frappe.form_dict.pop("doctype", None)
			frappe.form_dict.pop("docname", None)
			frappe.form_dict.pop("file_url", None)
			frappe.form_dict.pop("file_name", None)

			if hasattr(frappe.local, "request"):
				del frappe.local.request

	def test_remote_file_ignores_content_hash(self):
		existing = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": f"hash_{frappe.generate_hash(length=6)}.txt",
				"content": "private-content",
				"is_private": 1,
			}
		).insert()
		self.addCleanup(frappe.delete_doc, "File", existing.name, force=True)

		for file_url in ("https://example.com/remote.png", "/api/method/remote"):
			with self.subTest(file_url=file_url):
				frappe.set_user("test@example.com")
				try:
					remote = frappe.get_doc(
						{
							"doctype": "File",
							"file_name": "remote.png",
							"is_private": 1,
							"file_url": file_url,
							"content_hash": existing.content_hash,
						}
					).insert()
				finally:
					frappe.set_user("Administrator")
				self.addCleanup(frappe.delete_doc, "File", remote.name, force=True)

				self.assertEqual(remote.file_url, file_url)
				self.assertFalse(remote.content_hash)

				remote.content_hash = existing.content_hash
				remote.save()
				self.assertFalse(remote.content_hash)


@contextmanager
def convert_to_symlink(directory):
	"""Moves a directory to temp directory and symlinks original path for testing"""
	try:
		new_directory = shutil.move(directory, tempfile.mkdtemp())
		os.symlink(new_directory, directory)
		yield
	finally:
		os.unlink(directory)
		shutil.move(new_directory, directory)


class TestAttachment(FrappeTestCase):
	test_doctype = "Test For Attachment"

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.get_doc(
			doctype="DocType",
			name=cls.test_doctype,
			module="Custom",
			custom=1,
			fields=[
				{"label": "Title", "fieldname": "title", "fieldtype": "Data"},
				{"label": "Attachment", "fieldname": "attachment", "fieldtype": "Attach"},
			],
		).insert(ignore_if_duplicate=True)

	@classmethod
	def tearDownClass(cls):
		frappe.db.rollback()
		frappe.delete_doc("DocType", cls.test_doctype)

	def test_file_attachment_on_update(self):
		doc = frappe.get_doc(doctype=self.test_doctype, title="test for attachment on update").insert()

		file = frappe.get_doc(
			{"doctype": "File", "file_name": "test_attach.txt", "content": "Test Content"}
		).save()

		doc.attachment = file.file_url
		doc.save()

		exists = frappe.db.exists(
			"File",
			{
				"file_name": "test_attach.txt",
				"file_url": file.file_url,
				"attached_to_doctype": self.test_doctype,
				"attached_to_name": doc.name,
				"attached_to_field": "attachment",
			},
		)

		self.assertTrue(exists)

	def test_the_same_content_is_not_attached_again_under_another_url(self):
		# //// Neoffice — added test (see attach_files_to_document): a field pointing at a copy of a
		# //// file already attached to it must not gain one more File at every save.
		doc = frappe.get_doc(doctype=self.test_doctype, title="neo same content").insert()
		other = frappe.get_doc(doctype=self.test_doctype, title="neo same content, other").insert()
		content = "Neoffice same content " + frappe.generate_hash()
		first = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "neo_same_a.txt",
				"content": content,
				"attached_to_doctype": self.test_doctype,
				"attached_to_name": doc.name,
				"attached_to_field": "attachment",
			}
		).insert()
		# The same content under another name on disk, attached elsewhere: File would store a
		# content it already has under the first url, so the copy is written directly.
		copy_name = f"neo_same_b_{frappe.generate_hash(length=6)}.txt"
		with open(frappe.get_site_path("public", "files", copy_name), "w") as f:
			f.write(content)
		second = frappe.get_doc(
			{
				"doctype": "File",
				"file_url": f"/files/{copy_name}",
				"attached_to_doctype": self.test_doctype,
				"attached_to_name": other.name,
			}
		).insert()
		self.assertNotEqual(first.file_url, second.file_url)

		doc.attachment = second.file_url
		doc.save()
		doc.save()

		attached = {"attached_to_doctype": self.test_doctype, "attached_to_name": doc.name}
		self.assertEqual(frappe.db.count("File", attached), 1)


class TestAttachmentsAccess(FrappeTestCase):
	def setUp(self) -> None:
		frappe.db.delete("File", {"is_folder": 0})

	def test_list_private_attachments(self):
		frappe.set_user("test4@example.com")
		self.attached_to_doctype, self.attached_to_docname = make_test_doc()

		frappe.new_doc(
			"File",
			file_name="test_user_attachment.txt",
			attached_to_doctype=self.attached_to_doctype,
			attached_to_name=self.attached_to_docname,
			content="Testing User",
			is_private=1,
		).insert()

		frappe.new_doc(
			"File",
			file_name="test_user_standalone.txt",
			content="User Home",
			is_private=1,
		).insert()

		frappe.set_user("test@example.com")

		frappe.new_doc(
			"File",
			file_name="test_sm_attachment.txt",
			attached_to_doctype=self.attached_to_doctype,
			attached_to_name=self.attached_to_docname,
			content="Testing System Manager",
			is_private=1,
		).insert()

		frappe.new_doc(
			"File",
			file_name="test_sm_standalone.txt",
			content="System Manager Home",
			is_private=1,
		).insert()

		system_manager_files = [file.file_name for file in get_files_in_folder("Home")["files"]]
		system_manager_attachments_files = [
			file.file_name for file in get_files_in_folder("Home/Attachments")["files"]
		]

		frappe.set_user("test4@example.com")
		user_files = [file.file_name for file in get_files_in_folder("Home")["files"]]
		user_attachments_files = [file.file_name for file in get_files_in_folder("Home/Attachments")["files"]]

		self.assertIn("test_sm_standalone.txt", system_manager_files)
		self.assertNotIn("test_sm_standalone.txt", user_files)

		self.assertIn("test_user_standalone.txt", user_files)
		self.assertNotIn("test_user_standalone.txt", system_manager_files)

		self.assertIn("test_sm_attachment.txt", system_manager_attachments_files)
		self.assertIn("test_user_attachment.txt", system_manager_attachments_files)
		self.assertIn("test_user_attachment.txt", user_attachments_files)

	def test_attach_to_doc_without_write_permission_is_blocked(self):
		frappe.set_user("test4@example.com")
		self.assertFalse(frappe.has_permission("User", "write", "test@example.com"))

		attack = frappe.new_doc(
			"File",
			file_name="poisoned.svg",
			attached_to_doctype="User",
			attached_to_name="test@example.com",
			content="<svg xmlns='http://www.w3.org/2000/svg'><script>alert(1)</script></svg>",
			is_private=1,
		)
		self.assertRaises(frappe.PermissionError, attack.insert)

		frappe.set_user("test@example.com")
		self.assertEqual(
			frappe.get_all(
				"File", filters={"attached_to_doctype": "User", "attached_to_name": "test@example.com"}
			),
			[],
		)

	def tearDown(self) -> None:
		frappe.set_user("Administrator")
		frappe.db.rollback()

	# //// Neoffice — added (neoffice-maintenance#1744): the test above acts as two of frappe's User test
	# //// records, which only a run that made them has. Upstream's whole-suite CI made them in an earlier
	# //// module; a run of File's tests alone (our CI) did not, and making them pulls ERPNext's test records
	# //// on a site that has it, which die there. The "System Manager" was then a user without any role,
	# //// File's list rule rightly showed it its own files only, and the test failed. The two users are made
	# //// here when missing, as their test records define them, and removed after the class.
	USERS = (("test@example.com", ("System Manager",)), ("test4@example.com", ()))

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.set_user("Administrator")
		cls.users_made_here = []
		for email, roles in cls.USERS:
			if frappe.db.exists("User", email):
				continue
			user = frappe.get_doc(
				{"doctype": "User", "email": email, "first_name": "_Test", "send_welcome_email": 0}
			).insert(ignore_permissions=True)
			if roles:
				user.add_roles(*roles)
			cls.users_made_here.append(email)
		frappe.db.commit()

	@classmethod
	def tearDownClass(cls):
		frappe.set_user("Administrator")
		for email in cls.users_made_here:
			frappe.delete_doc("User", email, force=True, ignore_permissions=True)
		frappe.db.commit()
		super().tearDownClass()


class TestFileUtils(FrappeTestCase):
	def test_extract_images_from_doc(self):
		is_private = not frappe.get_meta("ToDo").make_attachments_public

		# with filename in data URI
		todo = frappe.get_doc(
			doctype="ToDo",
			description='Test <img src="data:image/png;filename=pix.png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=">',
		).insert()
		self.assertTrue(frappe.db.exists("File", {"attached_to_name": todo.name, "is_private": is_private}))
		self.assertRegex(todo.description, r"<img src=\"(.*)/files/pix\.png(.*)\">")
		self.assertListEqual(get_attached_images("ToDo", [todo.name])[todo.name], ["/private/files/pix.png"])

		# without filename in data URI
		todo = frappe.get_doc(
			doctype="ToDo",
			description='Test <img src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII=">',
		).insert()
		filename = frappe.db.exists("File", {"attached_to_name": todo.name})
		self.assertIn(f'<img src="{frappe.get_doc("File", filename).file_url}', todo.description)

	def test_extract_images_from_comment(self):
		"""
		Ensure that images are extracted from comments and become private attachments.
		"""
		is_private = not frappe.get_meta("ToDo").make_attachments_public
		test_doc = frappe.get_doc(doctype="ToDo", description="comment test").insert()
		comment = add_comment(
			"ToDo",
			test_doc.name,
			'<div class="ql-editor read-mode"><img src="data:image/png;filename=pix.png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="></div>',
			frappe.session.user,
			frappe.session.user,
		)

		self.assertTrue(
			frappe.db.exists("File", {"attached_to_name": test_doc.name, "is_private": is_private})
		)
		self.assertRegex(comment.content, r"<img src=\"(.*)/files/pix\.png(.*)\">")

	def test_extract_images_from_communication(self):
		"""
		Ensure that images are extracted from communication and become public attachments.
		"""
		is_private = not frappe.get_meta("Communication").make_attachments_public
		communication = frappe.get_doc(
			doctype="Communication",
			communication_type="Communication",
			communication_medium="Email",
			content='<div class="ql-editor read-mode"><img src="data:image/png;filename=pix.png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="></div>',
			recipients="to <to@test.com>",
			cc=None,
			bcc=None,
			sender="sender@test.com",
		).insert(ignore_permissions=True)

		self.assertTrue(
			frappe.db.exists("File", {"attached_to_name": communication.name, "is_private": is_private})
		)
		self.assertRegex(communication.content, r"<img src=\"(.*)/files/pix\.png(.*)\">")

	def test_broken_image(self):
		"""Ensure that broken inline images don't cause errors."""
		is_private = not frappe.get_meta("Communication").make_attachments_public
		communication = frappe.get_doc(
			doctype="Communication",
			communication_type="Communication",
			communication_medium="Email",
			content='<div class="ql-editor read-mode"><img src="data:image/png;filename=pix.png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CY="></div>',
			recipients="to <to@test.com>",
			cc=None,
			bcc=None,
			sender="sender@test.com",
		).insert(ignore_permissions=True)

		self.assertFalse(
			frappe.db.exists("File", {"attached_to_name": communication.name, "is_private": is_private})
		)
		self.assertIn(f'<img src="#broken-image" alt="{get_corrupted_image_msg()}">', communication.content)

	def test_create_new_folder(self):
		folder = create_new_folder("test_folder", "Home")
		self.assertTrue(folder.is_folder)

	def test_resolved_file_path_stays_within_files_directory(self):
		from frappe.utils.file_manager import get_file_path

		normal = frappe.get_doc(
			{"doctype": "File", "file_name": "within_bounds.txt", "content": "ok"}
		).insert()
		original_file_url = normal.file_url
		try:
			self.assertTrue(get_file_path(normal.name).endswith("within_bounds.txt"))

			normal.db_set("file_url", "/private/files/../../../../outside_bounds.txt")
			self.assertRaisesRegex(ValidationError, "Cannot access file path", get_file_path, normal.name)

			normal.db_set("file_url", "/private/files/../../site_level_file.txt")
			self.assertRaisesRegex(ValidationError, "Cannot access file path", get_file_path, normal.name)
		finally:
			normal.db_set("file_url", original_file_url)
			normal.delete()

	def test_traversal_file_url_cannot_reach_other_private_file(self):
		victim = frappe.get_doc(
			{"doctype": "File", "file_name": "traversal_victim.txt", "content": "secret", "is_private": 1}
		).insert()
		try:
			for is_private in (0, 1):
				doc = frappe.get_doc(
					{
						"doctype": "File",
						"file_name": "traversal_copy.txt",
						"file_url": f"/files/../../private/files/{victim.file_name}",
						"is_private": is_private,
					}
				)
				self.assertRaisesRegex(ValidationError, "File URL", doc.insert)
		finally:
			victim.delete()

	def test_resolved_file_path_rejects_sibling_directory_prefix_match(self):
		from frappe.utils.file_manager import get_file_path

		sibling_dir = get_files_path(is_private=1) + "_lookalike"
		os.makedirs(sibling_dir, exist_ok=True)
		with open(os.path.join(sibling_dir, "neighbour.txt"), "w") as f:
			f.write("outside the intended directory")

		normal = frappe.get_doc(
			{"doctype": "File", "file_name": "sibling_check.txt", "content": "ok"}
		).insert()
		original_file_url = normal.file_url
		try:
			normal.db_set("file_url", "/private/files/../files_lookalike/neighbour.txt")
			self.assertRaisesRegex(ValidationError, "Cannot access file path", get_file_path, normal.name)
		finally:
			normal.db_set("file_url", original_file_url)
			normal.delete()
			shutil.rmtree(sibling_dir)


class TestFileOptimization(FrappeTestCase):
	def test_optimize_file(self):
		with make_test_image_file() as test_file:
			original_size = test_file.file_size
			original_content_hash = test_file.content_hash

			test_file.optimize_file()
			optimized_size = test_file.file_size
			updated_content_hash = test_file.content_hash

			self.assertLess(optimized_size, original_size)
			self.assertNotEqual(original_content_hash, updated_content_hash)

	def test_optimize_file_rejects_mismatched_file_url(self):
		"""optimize_file must not read/write through a file_url belonging to another File record."""
		with make_test_image_file(private=True) as first_file:
			original_content = first_file.get_content()

			# distinct content from the first file's, so this doesn't collide with it via
			# the File doctype's identical-content deduplication
			second_image_path = frappe.get_app_path("frappe", "tests/data/exif_sample_image.jpg")
			with open(second_image_path, "rb") as f:
				second_content = f.read()
			self.assertNotEqual(second_content, original_content)

			second_file = frappe.get_doc(
				{
					"doctype": "File",
					"file_name": "second_file.jpg",
					"content": second_content,
					"is_private": 1,
				}
			).insert()
			self.addCleanup(second_file.delete)

			# same identity (name/owner) as second_file, but file_url/file_name swapped
			# to point at first_file's path
			crafted = frappe.get_doc(
				{
					"doctype": "File",
					"name": second_file.name,
					"owner": second_file.owner,
					"file_name": first_file.file_name,
					"file_url": first_file.file_url,
					"is_private": 1,
					"file_size": first_file.file_size,
					"modified": second_file.modified,
					"creation": second_file.creation,
				}
			)

			self.assertRaises(frappe.PermissionError, crafted.optimize_file)

			# neither the read nor the write side of optimize_file executed
			self.assertEqual(first_file.get_content(), original_content)
			self.assertEqual(frappe.get_doc("File", second_file.name).file_url, second_file.file_url)

	def test_validate_file_url_matches_record_allows_own_url(self):
		with make_test_image_file() as test_file:
			test_file.validate_file_url_matches_record()

	def test_validate_file_url_matches_record_rejects_missing_name(self):
		"""A doc with no name at all must not bypass the check by short-circuiting on it -
		optimize_file never legitimately runs against a document with no backing record."""
		doc = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": "not_yet_saved.jpg",
				"file_url": "/private/files/not_yet_saved.jpg",
				"is_private": 1,
			}
		)
		self.assertIsNone(doc.name)
		self.assertRaises(frappe.PermissionError, doc.validate_file_url_matches_record)

	def test_optimize_file_rejects_doc_with_no_name(self):
		"""A crafted doc with owner set to the caller but name omitted must not bypass the
		guard: permission checks upstream may still pass (falling back to create-level
		permission), so this method must fail closed rather than skip validation."""
		with make_test_image_file(private=True) as target:
			original_content = target.get_content()

			crafted = frappe.get_doc(
				{
					"doctype": "File",
					"owner": frappe.session.user,
					"file_name": target.file_name,
					"file_url": target.file_url,
					"is_private": 1,
					"file_size": target.file_size,
				}
			)
			self.assertIsNone(crafted.name)

			self.assertRaises(frappe.PermissionError, crafted.optimize_file)
			self.assertEqual(target.get_content(), original_content)

	def test_validate_file_url_matches_record_allows_url_shared_by_multiple_files(self):
		"""Two File records may legitimately share one file_url (see create_attachment_copy);
		each one must still be able to operate on its own record using that shared url."""
		doctype, docname = make_test_doc()
		source = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": f"shared-{frappe.generate_hash(length=8)}.jpg",
				"content": open(
					frappe.get_app_path("frappe", "tests/data/sample_image_for_optimization.jpg"), "rb"
				).read(),
				"is_private": 1,
			}
		).insert()
		self.addCleanup(source.delete)

		copy = source.create_attachment_copy(doctype, docname)
		self.assertEqual(copy.file_url, source.file_url)

		# both the original and the copy must pass, regardless of DB row ordering
		frappe.get_doc("File", source.name).validate_file_url_matches_record()
		frappe.get_doc("File", copy.name).validate_file_url_matches_record()

	def test_optimize_svg(self):
		file_path = frappe.get_app_path("frappe", "tests/data/sample_svg.svg")
		with open(file_path, "rb") as f:
			file_content = f.read()
		test_file = frappe.get_doc(
			{"doctype": "File", "file_name": "sample_svg.svg", "content": file_content}
		).insert()
		self.assertRaises(TypeError, test_file.optimize_file)
		test_file.delete()

	def test_optimize_textfile(self):
		test_file = frappe.get_doc(
			{"doctype": "File", "file_name": "sample_text.txt", "content": "Text files cannot be optimized"}
		).insert()
		self.assertRaises(NotImplementedError, test_file.optimize_file)
		test_file.delete()

	def test_optimize_folder(self):
		test_folder = frappe.get_doc("File", "Home/Attachments")
		self.assertRaises(TypeError, test_folder.optimize_file)

	def test_revert_optimized_file_on_rollback(self):
		with make_test_image_file() as test_file:
			image_path = test_file.get_full_path()
			size_before_optimization = os.stat(image_path).st_size
			test_file.optimize_file()
			frappe.db.rollback()
			size_after_rollback = os.stat(image_path).st_size

			self.assertEqual(size_before_optimization, size_after_rollback)

	def test_image_header_guessing(self):
		file_path = frappe.get_app_path("frappe", "tests/data/sample_image_for_optimization.jpg")
		with open(file_path, "rb") as f:
			file_content = f.read()

		self.assertEqual(get_extension("", None, file_content), "jpg")


class TestGuestFileAndAttachments(FrappeTestCase):
	def setUp(self) -> None:
		frappe.db.delete("File", {"is_folder": 0})
		frappe.get_doc(
			doctype="DocType",
			name="Test For Attachment",
			module="Custom",
			custom=1,
			fields=[
				{"label": "Title", "fieldname": "title", "fieldtype": "Data"},
				{"label": "Attachment", "fieldname": "attachment", "fieldtype": "Attach"},
			],
		).insert(ignore_if_duplicate=True)

	def tearDown(self) -> None:
		frappe.set_user("Administrator")
		frappe.db.rollback()
		frappe.delete_doc("DocType", "Test For Attachment")

	def test_attach_unattached_guest_file(self):
		"""Ensure that unattached files are attached on doc update."""
		f = frappe.new_doc(
			"File",
			file_name="test_private_guest_attachment.txt",
			content="Guest Home",
			is_private=1,
		).insert(ignore_permissions=True)

		d = frappe.new_doc("Test For Attachment")
		d.title = "Test for attachment on update"
		d.attachment = f.file_url
		d.assigned_by = frappe.session.user
		d.save()

		self.assertTrue(
			frappe.db.exists(
				"File",
				{
					"file_name": "test_private_guest_attachment.txt",
					"file_url": f.file_url,
					"attached_to_doctype": "Test For Attachment",
					"attached_to_name": d.name,
					"attached_to_field": "attachment",
				},
			)
		)

	def test_list_private_guest_single_file(self):
		"""Ensure that guests are not able to read private standalone guest files."""
		frappe.set_user("Guest")

		file = frappe.new_doc(
			"File",
			file_name="test_private_guest_single_txt",
			content="Private single File",
			is_private=1,
		).insert(ignore_permissions=True)

		self.assertFalse(file.is_downloadable())

	def test_list_private_guest_attachment(self):
		"""Ensure that guests are not able to read private guest attachments."""
		frappe.set_user("Guest")

		self.attached_to_doctype, self.attached_to_docname = make_test_doc(ignore_permissions=True)

		file = frappe.new_doc(
			"File",
			file_name="test_private_guest_attachment.txt",
			attached_to_doctype=self.attached_to_doctype,
			attached_to_name=self.attached_to_docname,
			content="Private Attachment",
			is_private=1,
		).insert(ignore_permissions=True)

		self.assertFalse(file.is_downloadable())

	def test_private_remains_private_even_if_same_hash(self):
		file_name = "test" + frappe.generate_hash()
		content = file_name.encode()

		doc_pub: File = frappe.new_doc("File")  # type: ignore
		doc_pub.file_url = f"/files/{file_name}.txt"
		doc_pub.content = content
		doc_pub.save()

		doc_pri: File = frappe.new_doc("File")  # type: ignore
		doc_pri.file_url = f"/private/files/{file_name}.txt"
		doc_pri.is_private = False
		doc_pri.content = content
		doc_pri.save()

		doc_pub.reload()
		doc_pri.reload()

		self.assertEqual(doc_pub.is_private, 0)
		self.assertEqual(doc_pri.is_private, 1)

		self.assertEqual(doc_pub.file_url, f"/files/{file_name}.txt")
		self.assertEqual(doc_pri.file_url, f"/private/files/{file_name}.txt")

		self.assertEqual(doc_pub.get_content(), content)
		self.assertEqual(doc_pri.get_content(), content)

		# Deleting a public File should not delete the private File's disk file
		doc_pub.delete()
		self.assertTrue(os.path.exists(doc_pri.get_full_path()))

		# TODO: Migrate existing Files that have a mismatch between `is_private` and `file_url` prefix?
		# self.assertFalse(os.path.exists(doc_pub.get_full_path()))

		self.assertEqual(doc_pri.get_content(), content)
		doc_pri.delete()
		self.assertFalse(os.path.exists(doc_pri.get_full_path()))


class TestFileListOwnerRestriction(FrappeTestCase):
	"""Test file list owner restriction."""

	OWNER = "test1@example.com"
	OTHER = "test2@example.com"

	def setUp(self):
		frappe.set_user("Administrator")
		other_user = frappe.get_doc("User", self.OTHER)
		if not any(r.role == "Blogger" for r in other_user.roles):
			other_user.append("roles", {"role": "Blogger"})
			other_user.save(ignore_permissions=True)

		frappe.set_user(self.OWNER)
		self.pddr = frappe.get_doc({"doctype": "Personal Data Download Request", "user": self.OWNER}).insert(
			ignore_permissions=True
		)
		self.file = frappe.new_doc(
			"File",
			file_name="secret_export.json",
			attached_to_doctype="Personal Data Download Request",
			attached_to_name=self.pddr.name,
			content="secret data",
			is_private=1,
		).insert(ignore_permissions=True)

	def tearDown(self):
		frappe.set_user("Administrator")
		frappe.db.rollback()

	def test_other_user_file_count_excludes_owner_restricted_file(self):
		frappe.set_user(self.OTHER)
		files = frappe.get_list("File", filters={"name": self.file.name})
		self.assertEqual(len(files), 0)

	def test_owner_file_count_includes_owner_restricted_file(self):
		frappe.set_user(self.OWNER)
		files = frappe.get_list("File", filters={"name": self.file.name})
		self.assertEqual(len(files), 1)

	def test_shared_but_not_owned_document_still_shows_its_file(self):
		frappe.set_user("Administrator")
		frappe.share.add_docshare("Personal Data Download Request", self.pddr.name, self.OTHER, read=1)

		frappe.set_user(self.OTHER)
		files = frappe.get_list("File", filters={"name": self.file.name})
		self.assertEqual(len(files), 1)


class TestFileListUserPermissionRestriction(FrappeTestCase):
	"""A doctype can grant unconditional role-level read (no if_owner) while still being scoped
	per-user via User Permissions (e.g. multi-company setups). A File attached to a record
	outside that scope must not be listable either."""

	RESTRICTED = "test1@example.com"
	OTHER = "test2@example.com"
	DOCTYPE = "Test User Perm Attachment"

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.get_doc(
			doctype="DocType",
			name=cls.DOCTYPE,
			module="Custom",
			custom=1,
			fields=[
				{"label": "Linked Role", "fieldname": "linked_role", "fieldtype": "Link", "options": "Role"}
			],
			permissions=[{"role": "All", "read": 1, "create": 1}],
		).insert(ignore_if_duplicate=True)

	@classmethod
	def tearDownClass(cls):
		super().tearDownClass()
		frappe.delete_doc("DocType", cls.DOCTYPE, force=True, ignore_permissions=True)

	def setUp(self):
		frappe.set_user("Administrator")
		for user in (self.RESTRICTED, self.OTHER):
			user_doc = frappe.get_doc("User", user)
			if not any(r.role == "Blogger" for r in user_doc.roles):
				user_doc.append("roles", {"role": "Blogger"})
				user_doc.save(ignore_permissions=True)

		frappe.get_doc(
			{
				"doctype": "User Permission",
				"user": self.RESTRICTED,
				"allow": "Role",
				"for_value": "Blogger",
			}
		).insert(ignore_permissions=True)

		self.permitted_record = frappe.get_doc({"doctype": self.DOCTYPE, "linked_role": "Blogger"}).insert(
			ignore_permissions=True
		)
		self.out_of_scope_record = frappe.get_doc(
			{"doctype": self.DOCTYPE, "linked_role": "Website Manager"}
		).insert(ignore_permissions=True)

		self.permitted_file = frappe.new_doc(
			"File",
			file_name="permitted.txt",
			attached_to_doctype=self.DOCTYPE,
			attached_to_name=self.permitted_record.name,
			content="in scope",
			is_private=1,
		).insert(ignore_permissions=True)
		self.out_of_scope_file = frappe.new_doc(
			"File",
			file_name="out_of_scope.txt",
			attached_to_doctype=self.DOCTYPE,
			attached_to_name=self.out_of_scope_record.name,
			content="out of scope",
			is_private=1,
		).insert(ignore_permissions=True)

	def tearDown(self):
		frappe.set_user("Administrator")
		frappe.db.rollback()

	def test_restricted_user_excludes_out_of_scope_file(self):
		frappe.set_user(self.RESTRICTED)
		files = frappe.get_list("File", filters={"name": self.out_of_scope_file.name})
		self.assertEqual(len(files), 0)

	def test_restricted_user_includes_permitted_file(self):
		frappe.set_user(self.RESTRICTED)
		files = frappe.get_list("File", filters={"name": self.permitted_file.name})
		self.assertEqual(len(files), 1)

	def test_unrestricted_user_sees_both_files(self):
		frappe.set_user(self.OTHER)
		files = frappe.get_list(
			"File",
			filters={"name": ["in", [self.permitted_file.name, self.out_of_scope_file.name]]},
		)
		self.assertEqual(len(files), 2)


class TestFilePermissionQuery(FrappeTestCase):
	def test_ignores_stale_custom_docperm_doctype(self):
		"""A stale Custom DocPerm can reference a deleted DocType; must not crash the File list query."""
		from frappe.core.doctype.file.file import get_permission_query_conditions
		from frappe.permissions import SYSTEM_USER_ROLE

		with (
			patch(
				"frappe.core.doctype.file.file.get_doctypes_with_read",
				return_value=["Deleted Doctype XYZ"],
			),
			patch("frappe.get_roles", return_value=[SYSTEM_USER_ROLE]),
		):
			# should not raise frappe.exceptions.DoesNotExistError
			get_permission_query_conditions(user="test1@example.com")


# //// Neoffice — added tests (no upstream equivalent): our fork slugs file names and URLs
# //// (File.before_insert, File.validate_file_url, handler.upload_file). The URL must keep naming
# //// the file that is actually on disk (neoffice-maintenance#662).
def _png_bytes():
	from io import BytesIO

	from PIL import Image

	buffer = BytesIO()
	Image.new("RGB", (4, 4), "white").save(buffer, format="PNG")
	return buffer.getvalue()


class TestNeofficeFileNames(FrappeTestCase):
	def assertOnDisk(self, doc):
		self.assertTrue(os.path.isfile(doc.get_full_path()), doc.file_url)
		self.assertEqual(doc.file_url.rsplit("/", 1)[1], os.path.basename(doc.get_full_path()))

	def test_a_document_named_with_a_space_and_an_accent_is_stored(self):
		"""What a Communication does with a printed attachment: a name built by code, not typed."""
		suffix = frappe.generate_hash(length=6)
		doc = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": f"Salary Certificate été {suffix}.txt",
				"content": f"certificate {suffix}".encode(),
				"is_private": 1,
			}
		).insert(ignore_permissions=True)
		self.assertOnDisk(doc)
		self.assertEqual(doc.get_content(), f"certificate {suffix}".encode())

	def test_a_legacy_upload_named_with_spaces_can_be_attached_again(self):
		"""Attaching an existing file again (email composer, attach from library) creates a File
		by URL, and uploads stored before names were slugged still carry spaces."""
		from frappe.core.doctype.file.utils import get_content_hash

		name = f"legacy upload {frappe.generate_hash(length=6)}.txt"
		path = get_files_path(name)
		with open(path, "w") as f:
			f.write(name)
		self.addCleanup(lambda: os.path.exists(path) and os.remove(path))
		legacy = frappe.new_doc("File")
		legacy.update(
			{
				"name": frappe.generate_hash(length=10),
				"file_name": name,
				"file_url": f"/files/{name}",
				"content_hash": get_content_hash(name.encode()),
			}
		)
		legacy.db_insert()  # as stored back then, without today's validation

		doc = frappe.get_doc({"doctype": "File", "file_url": f"/files/{name}"}).insert(ignore_permissions=True)
		self.assertEqual(doc.file_url, f"/files/{name}")
		self.assertEqual(doc.get_content(), name)  # read from disk: text comes back decoded

	def test_an_image_name_is_still_cleaned(self):
		suffix = frappe.generate_hash(length=6)
		doc = frappe.get_doc(
			{"doctype": "File", "file_name": f"Photo été {suffix}-800x600.png", "content": _png_bytes()}
		).insert(ignore_permissions=True)
		self.assertTrue(doc.file_name.startswith(f"Photo-ete-{suffix}"), doc.file_name)
		self.assertNotIn(" ", doc.file_url)
		self.assertOnDisk(doc)

	def test_an_image_named_only_by_its_size_keeps_a_name(self):
		"""Stripping "1200x800" left ".png": a dot file, which web servers commonly refuse to serve."""
		doc = frappe.get_doc(
			{"doctype": "File", "file_name": "1200x800.png", "content": _png_bytes() + os.urandom(8)}
		).insert(ignore_permissions=True)
		self.assertFalse(doc.file_name.startswith("."), doc.file_name)
		self.assertOnDisk(doc)


# //// Neoffice — added tests (no upstream equivalent): the `file_content_transformers` hook and the EXIF strip's
# //// new leniency in File.save_file (#1526).
def uppercase_transformer(content, file_name, content_type, is_private):
	"""A transformer for the tests: it rewrites the text it is given."""
	return content.upper()


def declining_transformer(content, file_name, content_type, is_private):
	return None


def failing_transformer(content, file_name, content_type, is_private):
	raise RuntimeError("transformer broken")


TRANSFORMER_CALLS = []


def spying_transformer(content, file_name, content_type, is_private):
	TRANSFORMER_CALLS.append((file_name, content_type, is_private))


def reencoding_transformer(content, file_name, content_type, is_private):
	"""Saves the photograph again, with no metadata and in a way the EXIF strip would not reproduce."""
	import io

	from PIL import Image

	buffer = io.BytesIO()
	Image.open(io.BytesIO(content)).save(buffer, "JPEG", quality=80, progressive=True)
	return buffer.getvalue()


def _jpeg_bytes(exif=False):
	import io

	from PIL import Image

	image = Image.effect_noise((120, 90), 60).convert("RGB")
	tags = Image.Exif()
	tags[0x010F] = "Phone Maker"  # Make
	tags[0x0112] = 6  # Orientation
	buffer = io.BytesIO()
	if exif:
		image.save(buffer, "JPEG", quality=90, exif=tags)
	else:
		image.save(buffer, "JPEG", quality=90)
	return buffer.getvalue()


class TestNeofficeFileContentTransformers(FrappeTestCase):
	HOOK = "file_content_transformers"
	PATH = "frappe.core.doctype.file.test_file."

	def hooked(self, *functions):
		"""frappe.get_hooks answering `functions` for the hook under test, and the real answer for the rest."""
		from unittest import mock

		real = frappe.get_hooks

		def get_hooks(hook=None, *args, **kwargs):
			if hook == self.HOOK:
				return [self.PATH + name for name in functions]
			return real(hook, *args, **kwargs)

		patcher = mock.patch.object(frappe, "get_hooks", get_hooks)
		patcher.start()
		self.addCleanup(patcher.stop)

	def stored(self, doc):
		"""The bytes on the disk: doc.get_content() hands back the content the document was given."""
		with open(doc.get_full_path(), "rb") as handle:
			return handle.read()

	def make_file(self, content, name=None, is_private=0):
		name = name or f"transform-{frappe.generate_hash(length=8)}.txt"
		return frappe.get_doc(
			{"doctype": "File", "file_name": name, "content": content, "is_private": is_private}
		).insert(ignore_permissions=True)

	def test_nothing_declared_nothing_changes(self):
		doc = self.make_file(b"as received")
		self.assertEqual(self.stored(doc), b"as received")

	def test_a_transformer_rewrites_the_bytes_before_they_are_hashed(self):
		from frappe.core.doctype.file.utils import get_content_hash

		self.hooked("uppercase_transformer")
		doc = self.make_file(b"as received")

		self.assertEqual(self.stored(doc), b"AS RECEIVED")
		self.assertEqual(doc.content_hash, get_content_hash(b"AS RECEIVED"))
		self.assertEqual(doc.file_size, len(b"AS RECEIVED"))

	def test_two_uploads_that_come_out_the_same_are_one_file_on_disk(self):
		self.hooked("uppercase_transformer")
		first = self.make_file(b"one text")
		second = self.make_file(b"ONE TEXT")  # another upload, and the same bytes once transformed

		self.assertEqual(first.content_hash, second.content_hash)
		self.assertEqual(first.file_url, second.file_url)

	def test_a_transformer_may_leave_the_file_alone(self):
		self.hooked("declining_transformer")
		self.assertEqual(self.stored(self.make_file(b"kept")), b"kept")

	def test_a_transformer_that_fails_does_not_fail_the_upload(self):
		self.hooked("failing_transformer", "uppercase_transformer")
		doc = self.make_file(b"still saved")
		self.assertEqual(self.stored(doc), b"STILL SAVED")  # the next one still ran

	def test_the_transformers_run_in_order_on_what_the_previous_one_returned(self):
		self.hooked("uppercase_transformer", "declining_transformer")
		self.assertEqual(self.stored(self.make_file(b"order")), b"ORDER")

	def test_a_transformer_is_told_the_type_and_the_visibility(self):
		TRANSFORMER_CALLS.clear()
		self.hooked("spying_transformer")
		self.make_file(b"x", name="picture-spy.jpg", is_private=1)
		self.make_file(b"y", name="picture-spy2.png", is_private=0)
		self.assertEqual(
			[call for call in TRANSFORMER_CALLS if call[0].startswith("picture-spy")],
			[("picture-spy.jpg", "image/jpeg", 1), ("picture-spy2.png", "image/png", 0)],
		)

	@change_settings("System Settings", {"strip_exif_metadata_from_uploaded_images": 1})
	def test_a_jpeg_without_exif_is_stored_byte_for_byte(self):
		clean = _jpeg_bytes(exif=False)
		doc = self.make_file(clean, name=f"clean-{frappe.generate_hash(length=6)}.jpg")
		self.assertEqual(self.stored(doc), clean)

	@change_settings("System Settings", {"strip_exif_metadata_from_uploaded_images": 1})
	def test_a_jpeg_with_exif_still_loses_it(self):
		import io

		from PIL import Image

		doc = self.make_file(_jpeg_bytes(exif=True), name=f"phone-{frappe.generate_hash(length=6)}.jpg")
		self.assertFalse(Image.open(io.BytesIO(self.stored(doc))).getexif())

	@change_settings("System Settings", {"strip_exif_metadata_from_uploaded_images": 1})
	def test_a_stored_jpeg_attached_again_is_not_written_twice(self):
		first = self.make_file(_jpeg_bytes(exif=True), name=f"again-{frappe.generate_hash(length=6)}.jpg")
		again = frappe.get_doc(
			{"doctype": "File", "file_name": first.file_name, "file_url": first.file_url}
		).insert(ignore_permissions=True)
		self.assertEqual(again.file_url, first.file_url)
		self.assertEqual(again.content_hash, first.content_hash)

	@change_settings("System Settings", {"strip_exif_metadata_from_uploaded_images": 1})
	def test_the_exif_strip_leaves_alone_what_a_transformer_rewrote(self):
		self.hooked("reencoding_transformer")
		source = _jpeg_bytes(exif=True)
		doc = self.make_file(source, name=f"rewritten-{frappe.generate_hash(length=6)}.jpg")
		self.assertEqual(self.stored(doc), reencoding_transformer(source, "", "", 0))


# //// Neoffice — added tests (no upstream equivalent): the EXIF strip applies the orientation before it drops the
# //// tag that carries it, and no longer copies the pixels through a Python list (#1540).
class TestNeofficeStripExifOrientation(FrappeTestCase):
	def test_a_photo_taken_upright_comes_back_upright(self):
		import io

		from PIL import Image

		from frappe.utils.image import strip_exif_data

		stripped = strip_exif_data(_jpeg_bytes(exif=True), "image/jpeg")  # 120 x 90 pixels, orientation 6

		image = Image.open(io.BytesIO(stripped))
		self.assertEqual(image.size, (90, 120))
		self.assertFalse(image.getexif())

	def test_a_photo_with_no_orientation_keeps_its_size(self):
		import io

		from PIL import Image

		from frappe.utils.image import strip_exif_data

		image = Image.open(io.BytesIO(strip_exif_data(_jpeg_bytes(exif=False), "image/jpeg")))
		self.assertEqual(image.size, (120, 90))
		self.assertFalse(image.getexif())

	def test_other_modes_are_still_read(self):
		import io

		from PIL import Image

		from frappe.utils.image import strip_exif_data

		cmyk = io.BytesIO()
		Image.effect_noise((60, 40), 50).convert("CMYK").save(cmyk, "JPEG")
		transparent = io.BytesIO()
		Image.new("RGBA", (60, 40), (10, 20, 30, 128)).save(transparent, "PNG")
		for content, content_type, size in (
			(cmyk.getvalue(), "image/jpeg", (60, 40)),
			(transparent.getvalue(), "image/png", (60, 40)),
		):
			with self.subTest(content_type=content_type, size=size):
				self.assertEqual(Image.open(io.BytesIO(strip_exif_data(content, content_type))).size, size)

	@change_settings("System Settings", {"strip_exif_metadata_from_uploaded_images": 1})
	def test_a_private_photo_taken_upright_is_stored_upright(self):
		import io

		from PIL import Image

		doc = frappe.get_doc(
			{
				"doctype": "File",
				"file_name": f"upright-{frappe.generate_hash(length=6)}.jpg",
				"content": _jpeg_bytes(exif=True),
				"is_private": 1,
			}
		).insert(ignore_permissions=True)
		with open(doc.get_full_path(), "rb") as handle:
			stored = Image.open(io.BytesIO(handle.read()))
		self.assertEqual(stored.size, (90, 120))
		self.assertFalse(stored.getexif())


# //// Neoffice — added tests (neoffice-maintenance#1738, #1742): a File reaches neither a private file its creator
# //// may not read nor a path outside the files folder, whatever the spelling of its URL and whichever hook the
# //// Suite's File override skips. Upstream 972ccf61b7 has no test; the update case is develop's
# //// test_file_url_change_permission (#43876); the adoption case is cb620dc4ba's test on a version-15 Attach field.
# //// The two desk users are made here: frappe's test users do not exist on a site restored from production.
class TestNeofficePrivateFileReferences(FrappeTestCase):
	OWNER = "neoffice-file-owner@example.com"
	OTHER = "neoffice-file-other@example.com"
	PRIVATE_CONTENT = "neoffice private content"

	@classmethod
	def setUpClass(cls):
		super().setUpClass()
		frappe.set_user("Administrator")
		for email in (cls.OWNER, cls.OTHER):
			if not frappe.db.exists("User", email):
				user = frappe.get_doc(
					{
						"doctype": "User",
						"email": email,
						"first_name": "Neoffice File Test",
						"send_welcome_email": 0,
					}
				).insert(ignore_permissions=True)
				user.add_roles("Blogger")
		frappe.db.commit()

	@classmethod
	def tearDownClass(cls):
		frappe.set_user("Administrator")
		for email in (cls.OWNER, cls.OTHER):
			frappe.delete_doc("User", email, force=True, ignore_permissions=True)
		frappe.db.commit()
		super().tearDownClass()

	def setUp(self):
		frappe.set_user(self.OWNER)
		self.victim = self.upload("neoffice_victim_file.txt", self.PRIVATE_CONTENT)
		frappe.set_user(self.OTHER)

	def tearDown(self):
		frappe.set_user("Administrator")
		frappe.db.rollback()

	def upload(self, file_name, content, is_private=1):
		return frappe.get_doc(
			{"doctype": "File", "file_name": file_name, "content": content, "is_private": is_private}
		).insert()

	def reference(self, file_url, **fields):
		return frappe.get_doc(
			{"doctype": "File", "file_name": "neoffice_reference.txt", "file_url": file_url, **fields}
		)

	@contextmanager
	def file_outside_the_files_folder(self):
		"""A file two levels above the public files folder (the site folder), named relative to it."""
		files_folder = os.path.realpath(get_files_path())
		path = os.path.join(os.path.dirname(os.path.dirname(files_folder)), "neoffice_outside_test.txt")
		with open(path, "w") as f:
			f.write("outside the files folder")
		try:
			yield "/files/../../neoffice_outside_test.txt"
		finally:
			os.remove(path)

	def test_another_users_private_file_cannot_be_referenced(self):
		self.assertRaises(frappe.PermissionError, self.reference(self.victim.file_url).insert)

	def test_other_spellings_of_the_url_cannot_reference_it(self):
		from frappe.utils import get_url

		name = self.victim.file_url.rsplit("/", 1)[1]
		stem, extension = name.rsplit(".", 1)
		for file_url in (
			f"/private/files/./{name}",
			f"/private/files//{name}",
			"/private/files/" + name.replace("_", "%5F"),
			get_url() + self.victim.file_url,
			# validate_file_url points a URL that is not on disk at its "cleaned" name when that one is
			f"/private/files/{stem}-1200x800.{extension}",
		):
			with self.subTest(file_url=file_url):
				self.assertRaises(frappe.PermissionError, self.reference(file_url).insert)

	def test_insert_that_skips_the_core_hooks_is_checked(self):
		# as the Suite's File override does for Drive's own inserts (create_link takes its URL from the caller)
		doc = self.reference(self.victim.file_url, is_private=1)
		doc.before_insert = lambda: None
		doc.validate = lambda: None
		self.assertRaises(frappe.PermissionError, doc.insert)

	def test_a_file_drive_records_without_its_leading_slash_cannot_be_referenced(self):
		# Drive (the Suite) records its files as "private/files/Users/…"; the slash makes it the same file
		self.victim.db_set("file_url", self.victim.file_url.lstrip("/"))
		self.assertRaises(frappe.PermissionError, self.reference("/" + self.victim.file_url).insert)

	def test_a_link_typed_without_its_scheme_is_stored(self):
		# Drive's create_link keeps whatever its creator typed: a text that names no file passes the check
		doc = self.reference("example.com/neoffice-page", is_private=1)
		doc.before_insert = lambda: None
		doc.validate = lambda: None
		doc.insert()
		self.assertEqual(doc.file_url, "example.com/neoffice-page")

	def test_new_url_on_a_saved_file_is_checked(self):
		for skip_core_validate in (False, True):
			with self.subTest(skip_core_validate=skip_core_validate):
				mine = self.upload(f"neoffice_mine_{int(skip_core_validate)}.txt", "neoffice mine")
				mine.file_url = self.victim.file_url
				if skip_core_validate:
					# as the Suite's File override does for every saved File
					mine.validate = lambda: None
				self.assertRaises(frappe.PermissionError, mine.save)

	def test_another_users_unattached_file_is_not_adopted(self):
		from unittest import mock

		from frappe.core.doctype.file.utils import attach_files_to_document

		user = frappe.get_doc("User", self.OTHER)
		user.user_image = self.victim.file_url
		with mock.patch.object(type(user), "log_error"):
			attach_files_to_document(user, "on_update")

		self.victim.reload()
		self.assertIsNone(self.victim.attached_to_doctype)
		self.assertFalse(
			frappe.db.exists("File", {"file_url": self.victim.file_url, "attached_to_doctype": "User"})
		)

	def test_a_path_outside_the_files_folder_is_not_read(self):
		with self.file_outside_the_files_folder() as file_url:
			self.assertRaisesRegex(ValidationError, "File URL", self.reference(file_url).insert)

	def test_a_full_url_of_this_site_is_validated_before_reading(self):
		from frappe.utils import get_url

		mine = frappe.get_doc("File", self.upload("neoffice_mine.txt", "neoffice mine").name)
		with self.file_outside_the_files_folder() as file_url:
			# kept by a save that skips validate(), as the Suite's File override does
			mine.file_url = get_url() + file_url
			self.assertRaisesRegex(ValidationError, "File URL", mine.get_content)

	def test_the_owner_and_administrator_may_reference_it(self):
		frappe.set_user(self.OWNER)
		self.reference(self.victim.file_url).insert()
		frappe.set_user("Administrator")
		self.reference(self.victim.file_url).insert()

	def test_the_same_content_uploaded_by_another_user_is_stored(self):
		# save_file points the new record at the older copy of the same content: the uploader's own content
		doc = self.upload("neoffice_same_content.txt", self.PRIVATE_CONTENT)
		self.assertEqual(doc.file_url, self.victim.file_url)

	def test_making_a_shared_file_private_is_not_refused(self):
		# handle_is_private_changed moves the file and every record sharing it: the same file, not a new URL
		frappe.set_user("Administrator")
		self.upload("neoffice_shared.txt", "neoffice shared", is_private=0)
		frappe.set_user(self.OTHER)
		mine = self.upload("neoffice_shared.txt", "neoffice shared", is_private=0)
		mine.is_private = 1
		mine.save()
		self.assertTrue(mine.file_url.startswith("/private/files/"))
