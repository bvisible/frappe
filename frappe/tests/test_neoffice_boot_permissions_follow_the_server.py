# //// Neoffice — added file (no upstream equivalent): the permissions the desk is booted with must be the ones the server
# //// applies (neoffice-maintenance#2710). The boot's can_read is what the browser's guards trust before they call the
# //// server; a doctype it grants and the server refuses ends in a refusal the person never asked for. Each test fails on
# //// frappe/permissions.py before get_valid_perms left out the Custom DocPerm rows of the meta's special doctypes.
import frappe
from frappe.model.meta import Meta
from frappe.permissions import AUTOMATIC_ROLES, get_doctypes_with_read, get_role_permissions
from frappe.tests.utils import FrappeTestCase
from frappe.utils.user import UserPermissions


class TestNeofficeBootPermissionsFollowTheServer(FrappeTestCase):
	def setUp(self):
		# The row every Neoffice site carries: "Desk User" may read DocType. The meta of a special doctype never applies a
		# Custom DocPerm, so the server ignores it.
		if not frappe.db.exists("Custom DocPerm", {"parent": "DocType", "role": "Desk User", "permlevel": 0}):
			row = frappe.get_doc(
				{
					"doctype": "Custom DocPerm",
					"parent": "DocType",
					"parenttype": "DocType",
					"parentfield": "permissions",
					"role": "Desk User",
					"permlevel": 0,
					"read": 1,
				}
			).insert(ignore_permissions=True)
			self.addCleanup(
				frappe.delete_doc, "Custom DocPerm", row.name, force=True, ignore_permissions=True
			)
		self.desk_user = self.make_user(
			"neoffice-boot-desk@example.com",
			frappe.get_all(
				"Role",
				filters={
					"desk_access": 1,
					"disabled": 0,
					"name": ["not in", (*AUTOMATIC_ROLES, "System Manager", "Workspace Manager")],
				},
				pluck="name",
			),
		)
		self.system_manager = self.make_user("neoffice-boot-sm@example.com", ["System Manager"])
		frappe.local.role_permissions = {}

	def make_user(self, email, roles):
		if frappe.db.exists("User", email):
			frappe.delete_doc("User", email, force=True, ignore_permissions=True)
		user = frappe.get_doc(
			{
				"doctype": "User",
				"email": email,
				"first_name": email.split("@")[0],
				"send_welcome_email": 0,
				"roles": [{"role": role} for role in roles],
			}
		).insert(ignore_permissions=True)
		self.addCleanup(frappe.delete_doc, "User", user.name, force=True, ignore_permissions=True)
		return user.name

	def booted(self, user):
		# The boot reads the session's own roles (build_perm_map calls get_valid_perms() without a user), as at login.
		frappe.set_user(user)
		try:
			permissions = UserPermissions(user)
			permissions.build_permissions()
		finally:
			frappe.set_user("Administrator")
		return permissions

	def test_a_desk_account_is_not_booted_with_doctype_the_server_refuses(self):
		self.assertFalse(frappe.has_permission("DocType", "read", user=self.desk_user))
		booted = self.booted(self.desk_user)
		self.assertNotIn("DocType", booted.can_read)
		self.assertNotIn("DocType", booted.can_select)
		# The permission queries of Comment and File read the same list.
		self.assertNotIn("DocType", get_doctypes_with_read(self.desk_user))

	def test_no_doctype_is_read_in_the_boot_and_refused_by_the_server(self):
		# Every doctype the boot says the account reads, the server must say so too (select as well): the general check.
		# A child table is left out: the boot never lists one in can_read, and the server reads it through its parent.
		booted = self.booted(self.desk_user)
		child_tables = set(frappe.get_all("DocType", filters={"istable": 1}, pluck="name"))
		boot_only = []
		for doctype, rights in booted.perm_map.items():
			if doctype in child_tables or not frappe.db.exists("DocType", doctype):
				continue
			server = get_role_permissions(frappe.get_meta(doctype), user=self.desk_user)
			for right in ("read", "select"):
				if rights.get(right) and not server.get(right):
					boot_only.append(f"{doctype}: {right}")
		self.assertEqual(boot_only, [])

	def test_a_system_manager_still_reads_doctype_in_the_boot(self):
		# A Custom DocPerm row on DocType used to replace its standard rows in the boot: a site whose rows named only
		# Desk User booted its System Managers without DocType. The standard rows count, as in the meta.
		self.assertTrue(frappe.has_permission("DocType", "read", user=self.system_manager))
		self.assertIn("DocType", self.booted(self.system_manager).can_read)
		self.assertIn("DocType", get_doctypes_with_read(self.system_manager))

	def test_every_special_doctype_of_the_meta_is_named(self):
		# The list the fix reads is the meta's own: a doctype added to it upstream is covered without a change here.
		self.assertIn("DocType", Meta.special_doctypes)
		self.assertIn("Module Def", Meta.special_doctypes)
