# //// Neoffice — added file (no upstream equivalent)
"""``resolve_redirect`` must not grow the list of redirects the hooks cache holds (neoffice-maintenance#2063).

``frappe.get_hooks()`` hands out the list the process keeps cached; ``resolve_redirect`` appended the database's rows to it with
``+=``, on every call, for good. A page renamed twice then kept its first rule in front of the second for as long as the worker
lived, and the list grew at every route cache miss. Upstream's fix (frappe ``f59ebe32e6``) copied the other two lists it reads
and missed this one.

    bench --site <site> run-tests --module frappe.tests.test_redirects_leave_the_hooks_alone
"""

from unittest.mock import patch

from frappe.tests.utils import FrappeTestCase

import frappe

real_get_hooks = frappe.get_hooks


class TestResolveRedirectLeavesTheHooksAlone(FrappeTestCase):
	def resolve(self, path, cached, rows):
		from frappe.website.path_resolver import resolve_redirect

		def get_hooks(hook=None, *args, **kwargs):
			return cached if hook == "website_redirects" else real_get_hooks(hook, *args, **kwargs)

		frappe.flags.redirect_location = None
		with patch("frappe.get_hooks", side_effect=get_hooks), patch("frappe.get_all", return_value=rows):
			try:
				resolve_redirect(path)
			except frappe.Redirect:
				pass
		return frappe.flags.redirect_location

	def test_the_cached_list_does_not_grow(self):
		cached = [{"source": "/from", "target": "/to"}]
		rows = [{"source": "old", "target": "/new", "redirect_http_status": 301}]
		for _ in range(3):
			self.resolve("nothing-matches", cached, rows)
		self.assertEqual(cached, [{"source": "/from", "target": "/to"}])

	def test_a_rule_of_the_database_is_followed_and_a_changed_one_is_read_again(self):
		cached = []
		frappe.cache.delete_key("website_redirects")
		first = [{"source": "wstest-old", "target": "/wstest-new", "redirect_http_status": 301}]
		self.assertEqual(self.resolve("wstest-old", cached, first), "/wstest-new")
		frappe.cache.delete_key("website_redirects")
		second = [{"source": "wstest-old", "target": "/wstest-newer", "redirect_http_status": 301}]
		self.assertEqual(self.resolve("wstest-old", cached, second), "/wstest-newer")
		frappe.cache.delete_key("website_redirects")
