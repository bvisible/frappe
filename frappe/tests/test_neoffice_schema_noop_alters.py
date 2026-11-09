# //// Neoffice — added file (no upstream equivalent in version-15). Guards
# //// neoffice-maintenance#1652: two comparisons in frappe/database/schema.py answered "changed"
# //// for columns that were already right, so every migrate rebuilt about 38 tables for nothing.
# //// Upstream fixed both on develop only (frappe/frappe@40b465ee0d and @1a1dc0a62c).
"""A column that is already right must not be queued for a MODIFY, a real change still must."""

from types import SimpleNamespace

import frappe
from frappe.database.schema import DbColumn
from frappe.query_builder.utils import db_type_is
from frappe.tests.test_query_builder import run_only_if
from frappe.tests.utils import FrappeTestCase

CHANGE_LISTS = (
	"add_column",
	"change_type",
	"change_name",
	"add_unique",
	"add_index",
	"drop_unique",
	"drop_index",
	"set_default",
)


def stub_table():
	"""Collects what DbColumn.build_for_alter_table would queue, without touching a real table."""
	return SimpleNamespace(**{name: [] for name in CHANGE_LISTS})


def column(table, fieldtype, default=None):
	# DbColumn(table, fieldname, fieldtype, length, default, set_index, options, unique, precision)
	return DbColumn(table, "neoffice_probe", fieldtype, None, default, 0, None, 0, None)


class TestDecimalDefaultIsNotAChange(FrappeTestCase):
	"""Frappe creates Currency, Float and Percent as `decimal(21,9) not null default 0`; the
	database reports the default as '0.000000000'."""

	def test_the_default_zero_the_database_reports_is_not_a_change(self):
		current = {"default": "0.000000000", "type": "decimal(21,9)"}
		for fieldtype in ("Currency", "Float", "Percent"):
			for default in (None, "", 0.0):
				with self.subTest(fieldtype=fieldtype, default=default):
					self.assertFalse(
						column(stub_table(), fieldtype, default).default_changed_for_decimal(current)
					)

	def test_a_non_zero_default_on_the_field_is_still_a_change(self):
		current = {"default": "0.000000000", "type": "decimal(21,9)"}
		self.assertTrue(column(stub_table(), "Currency", "5").default_changed_for_decimal(current))

	def test_a_non_zero_default_in_the_database_is_still_a_change(self):
		current = {"default": "3.000000000", "type": "decimal(21,9)"}
		self.assertTrue(column(stub_table(), "Currency", None).default_changed_for_decimal(current))
		self.assertTrue(column(stub_table(), "Currency", "").default_changed_for_decimal(current))

	def test_no_default_on_either_side_is_not_a_change(self):
		for current_default in (None, "", "NULL"):
			with self.subTest(current_default=current_default):
				current = {"default": current_default, "type": "decimal(21,9)"}
				self.assertFalse(column(stub_table(), "Float", None).default_changed_for_decimal(current))

	def test_alter_queues_no_default_change_for_a_column_that_is_already_right(self):
		table = stub_table()
		current = {
			"default": "0.000000000",
			"type": "decimal(21,9)",
			"unique": 0,
			"index": 0,
		}
		column(table, "Currency", None).build_for_alter_table(current)
		self.assertEqual(table.set_default, [])
		self.assertEqual(table.change_type, [])


class TestJsonColumnIsNotATypeChange(FrappeTestCase):
	"""MariaDB declares a JSON column as longtext and information_schema still says longtext."""

	@run_only_if(db_type_is.MARIADB)
	def test_longtext_for_a_json_field_is_not_a_type_change(self):
		table = stub_table()
		current = {"default": None, "type": "longtext", "unique": 0, "index": 0}
		column(table, "JSON").build_for_alter_table(current)
		self.assertEqual(table.change_type, [])

	@run_only_if(db_type_is.MARIADB)
	def test_longtext_for_another_field_type_is_still_a_type_change(self):
		table = stub_table()
		current = {"default": None, "type": "longtext", "unique": 0, "index": 0}
		column(table, "Data").build_for_alter_table(current)
		self.assertEqual(len(table.change_type), 1)

	@run_only_if(db_type_is.MARIADB)
	def test_another_current_type_for_a_json_field_is_still_a_type_change(self):
		table = stub_table()
		current = {"default": None, "type": "varchar(140)", "unique": 0, "index": 0}
		column(table, "JSON").build_for_alter_table(current)
		self.assertEqual(len(table.change_type), 1)
