# //// Neoffice — added file (no upstream equivalent). Guards neoffice-maintenance#2368: a page rendered by the
# //// Chrome generator that calls the site's own API (a shop page asking for the cart) must not write an Error
# //// Log row per call, while a request that reaches for a file outside public/files still does.
"""What the Chrome generator does with the requests a rendered page sends to the site's own host."""

from unittest import mock

import frappe
from frappe.tests.utils import FrappeTestCase

HOST = "https://shop.example.test/"
UNAUTHORIZED_ACCESS = "Attempted Unauthorized File Access in PDF Generator"


class TestChromePdfRequestsToTheSite(FrappeTestCase):
	def request(self, path):
		"""Play one request of the rendered page, for `path` on the site's host, through the generator's handler.

		Returns the CDP commands the handler sent, as (method, params), and the mock standing for frappe.log_error."""
		from frappe.utils.pdf_generator.page import Page

		session = mock.MagicMock()
		page = Page.__new__(Page)  # no browser, no tab: only the interception is under test
		page.session = session
		page.session_id, page.target_id, page.frame_id = "session", "target", "frame"
		with (
			mock.patch("frappe.utils.pdf_generator.page.get_host_url", return_value=HOST),
			mock.patch.object(frappe, "log_error") as log_error,
		):
			page.intercept_request_for_local_resources()
			handler = session.start_listener.call_args.args[1]
			session.send.reset_mock()
			handler(mock.MagicMock(), {"params": {"requestId": "r1", "request": {"url": HOST + path}}})
		return [(call.args[0], call.args[1]) for call in session.send.call_args_list], log_error

	def test_a_call_to_the_sites_api_fails_quietly(self):
		"""The cart of a shop page: refused as before, but it is not an intrusion, so nothing is logged."""
		path = "api/method/webshop.webshop.shopping_cart.cart.get_cart_quotation?with_items=1"
		sent, log_error = self.request(path)
		self.assertEqual(sent, [("Fetch.failRequest", {"requestId": "r1", "errorReason": "BlockedByClient"})])
		log_error.assert_not_called()

	def test_a_desk_route_fails_quietly_too(self):
		sent, log_error = self.request("app/item")
		self.assertEqual(sent, [("Fetch.failRequest", {"requestId": "r1", "errorReason": "BlockedByClient"})])
		log_error.assert_not_called()

	def test_a_file_outside_public_files_is_still_refused_and_logged(self):
		"""The attempts the log is for: climbing out of public/, a path made to look like a route, a private file."""
		for path in ("files/../../site_config.json", "api/../../site_config.json", "private/files/secret.pdf"):
			with self.subTest(path=path):
				sent, log_error = self.request(path)
				self.assertEqual(
					sent, [("Fetch.failRequest", {"requestId": "r1", "errorReason": "AccessDenied"})]
				)
				log_error.assert_called_once()
				self.assertEqual(log_error.call_args.kwargs["title"], UNAUTHORIZED_ACCESS)

	def test_which_paths_are_routes(self):
		from frappe.utils.pdf_generator.page import is_dynamic_route

		for path in ("api", "api/method/ping", "api/resource/Item?limit=1", "app", "app/item/view/report"):
			with self.subTest(path=path):
				self.assertTrue(is_dynamic_route(path))
		for path in (
			"",
			"favicon.ico",
			"files/a.png",
			"assets/frappe/js/x.js",
			"apiary/x",
			"private/api/x",
			"api/../x",
			"app/../../x",
		):
			with self.subTest(path=path):
				self.assertFalse(is_dynamic_route(path))


if __name__ == "__main__":
	import unittest

	unittest.main()
