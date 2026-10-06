# //// Neoffice — added file (no upstream equivalent).
"""`bench run-tests --app <app>` collects the test files placed at the package root of an app (#816).

`_add_test` named the APP as the module to import when a test file sits at the root of the app package, so
the app package was imported instead of the file and no test was collected from it. These tests call the
function with a fake app directory and a fake import: they need no site and import nothing of any app.
"""

import os
import tempfile
import unittest
from unittest import mock

from frappe import test_runner


class TestRootLevelTestModules(unittest.TestCase):
	def _module_imported_for(self, relative_folder, filename):
		"""The module name `_add_test` imports for `filename` placed in `relative_folder` of an app."""
		seen = []

		def fake_import(name):
			seen.append(name)
			return mock.Mock(spec=[])  # a module without test_dependencies

		with tempfile.TemporaryDirectory() as app_dir:
			path = os.path.join(app_dir, relative_folder) if relative_folder else app_dir
			with (
				mock.patch.object(test_runner.frappe, "get_app_path", return_value=app_dir),
				mock.patch.object(test_runner.importlib, "import_module", side_effect=fake_import),
				mock.patch.object(
					test_runner.unittest.TestLoader, "loadTestsFromModule", return_value=unittest.TestSuite()
				),
			):
				test_runner._add_test("someapp", path, filename, verbose=False, test_suite=unittest.TestSuite())
		return seen

	def test_a_test_file_at_the_package_root_is_imported_as_its_own_module(self):
		self.assertEqual(self._module_imported_for("", "test_invoicing.py"), ["someapp.test_invoicing"])

	def test_a_test_file_in_a_sub_folder_is_imported_as_before(self):
		self.assertEqual(self._module_imported_for("tests", "test_invoicing.py"), ["someapp.tests.test_invoicing"])

	def test_a_test_file_in_a_nested_folder_is_imported_as_before(self):
		self.assertEqual(
			self._module_imported_for(os.path.join("someapp", "doctype_folder"), "test_x.py"),
			["someapp.someapp.doctype_folder.test_x"],
		)


if __name__ == "__main__":
	unittest.main()
