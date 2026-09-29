# Copyright (c) 2022, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE
from unittest.mock import patch

import frappe

# //// Neoffice — the extra names are for TestEmptyTablesByModuleCache below.
from frappe.config import (
	EMPTY_TABLES_CACHE_KEY,
	compute_all_empty_tables_by_module,
	get_all_empty_tables_by_module,
	get_modules_from_all_apps_for_user,
)
from frappe.tests.utils import FrappeTestCase


class TestConfig(FrappeTestCase):
	def test_get_modules(self):
		frappe_modules = frappe.get_all("Module Def", filters={"app_name": "frappe"}, pluck="name")
		all_modules_data = get_modules_from_all_apps_for_user()
		all_modules = [x["module_name"] for x in all_modules_data]
		self.assertIsInstance(all_modules_data, list)
		self.assertFalse([x for x in frappe_modules if x not in all_modules])


# //// Neoffice — added (no upstream equivalent). get_all_empty_tables_by_module() is kept for an hour
# //// (frappe/config/__init__.py, neoffice-maintenance#805): a second call must not read
# //// information_schema again, the cached answer must be the computed one, and clear_cache() must
# //// drop it.
class TestEmptyTablesByModuleCache(FrappeTestCase):
	def setUp(self):
		frappe.cache.delete_value(EMPTY_TABLES_CACHE_KEY)

	def tearDown(self):
		frappe.cache.delete_value(EMPTY_TABLES_CACHE_KEY)

	def _scans_of_empty_tables(self, fn):
		"""Run `fn`; return its result and how many empty-table scans of information_schema it sent."""
		real_sql = frappe.db.sql
		scans = []

		def spy(query, *args, **kwargs):
			# Only the scan under test: clear_cache() also makes the meta code read information_schema.
			if "information_schema" in str(query) and "table_rows" in str(query):
				scans.append(str(query))
			return real_sql(query, *args, **kwargs)

		with patch.object(frappe.db, "sql", spy):
			result = fn()
		return result, len(scans)

	def test_a_second_call_does_not_read_information_schema_again(self):
		first, first_scans = self._scans_of_empty_tables(get_all_empty_tables_by_module)
		second, second_scans = self._scans_of_empty_tables(get_all_empty_tables_by_module)

		self.assertEqual(first_scans, 1)
		self.assertEqual(second_scans, 0)
		self.assertEqual(second, first)

	def test_the_cached_answer_is_the_computed_one(self):
		computed = []

		def compute_and_remember():
			computed.append(compute_all_empty_tables_by_module())
			return computed[-1]

		with patch("frappe.config.compute_all_empty_tables_by_module", compute_and_remember):
			first = get_all_empty_tables_by_module()
			# Forget the copy kept for this request, so that the second call reads the cache in Redis.
			frappe.local.cache.pop(frappe.cache.make_key(EMPTY_TABLES_CACHE_KEY), None)
			second = get_all_empty_tables_by_module()

		self.assertEqual(len(computed), 1)
		self.assertIsInstance(first, dict)
		self.assertEqual(first, computed[0])
		self.assertEqual(second, computed[0])

	def test_a_computed_answer_survives_the_cache_and_still_marks_the_modules(self):
		# Today the computed answer is always empty (see the marker in frappe/config/__init__.py), so
		# prove the round trip and the consumer with an answer that is not.
		computed = {"Core": ["DocType", "User"], "Desk": ["Note"]}
		with patch("frappe.config.compute_all_empty_tables_by_module", return_value=computed) as compute:
			cold = get_modules_from_all_apps_for_user("Administrator")
			# A new request: the copy kept for the first one is gone, the cache in Redis answers.
			frappe.local.cache.pop(frappe.cache.make_key(EMPTY_TABLES_CACHE_KEY), None)
			warm = get_modules_from_all_apps_for_user("Administrator")
			from_redis = get_all_empty_tables_by_module()

		compute.assert_called_once()
		self.assertEqual(from_redis, computed)
		self.assertEqual(cold, warm)
		flagged = {m["module_name"] for m in warm if m.get("onboard_present")}
		self.assertEqual(flagged, {"Core", "Desk"})

	def test_the_modules_of_a_user_are_the_same_with_a_warm_cache_as_with_a_cold_one(self):
		cold = get_modules_from_all_apps_for_user("Administrator")
		warm = get_modules_from_all_apps_for_user("Administrator")

		self.assertEqual(cold, warm)

	def test_an_empty_answer_is_cached_too(self):
		with patch("frappe.config.compute_all_empty_tables_by_module", return_value={}) as compute:
			self.assertEqual(get_all_empty_tables_by_module(), {})
			frappe.local.cache.pop(frappe.cache.make_key(EMPTY_TABLES_CACHE_KEY), None)
			self.assertEqual(get_all_empty_tables_by_module(), {})

		compute.assert_called_once()

	def test_the_answer_expires_after_about_an_hour(self):
		get_all_empty_tables_by_module()

		ttl = frappe.cache.ttl(frappe.cache.make_key(EMPTY_TABLES_CACHE_KEY))

		self.assertTrue(3500 < ttl <= 3600, f"ttl was {ttl}")

	def test_clear_cache_drops_the_answer(self):
		get_all_empty_tables_by_module()
		self.assertIsNotNone(frappe.cache.get_value(EMPTY_TABLES_CACHE_KEY, expires=True))

		frappe.clear_cache()

		self.assertIsNone(frappe.cache.get_value(EMPTY_TABLES_CACHE_KEY, expires=True))
		_, scans = self._scans_of_empty_tables(get_all_empty_tables_by_module)
		self.assertEqual(scans, 1)
