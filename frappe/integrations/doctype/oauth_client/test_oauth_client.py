# Copyright (c) 2015, Frappe Technologies and Contributors
# License: MIT. See LICENSE
import frappe
from frappe.tests.utils import FrappeTestCase

# test_records = frappe.get_test_records('OAuth Client')


class TestOAuthClient(FrappeTestCase):
	# //// Neoffice — added test: the secret of a new client is long (neoffice-maintenance#1985).
	def test_the_secret_of_a_new_client_is_long_and_random(self):
		secrets_made = set()
		for index in range(2):
			client = frappe.get_doc(
				{
					"doctype": "OAuth Client",
					"app_name": f"zz secret length {index}",
					"scopes": "all openid",
					"redirect_uris": "https://client.example/callback",
					"default_redirect_uri": "https://client.example/callback",
					"grant_type": "Authorization Code",
					"response_type": "Code",
				}
			).insert(ignore_permissions=True)
			self.assertGreaterEqual(len(client.client_secret), 64)
			secrets_made.add(client.client_secret)
		self.assertEqual(len(secrets_made), 2)
