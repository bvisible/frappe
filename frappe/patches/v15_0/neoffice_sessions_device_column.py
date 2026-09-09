# //// Neoffice — added file (no upstream equivalent).
# ////
# //// `tabSessions.device`: upstream v15 dropped the column from the framework DDL
# //// (older sites still carry it from v13). Our fork writes it again — a session
# //// knows which kind of device opened it, see frappe.sessions.session_device —
# //// so every site needs the column. Idempotent: a site that has it is left alone.
import frappe


def execute():
	columns = [c[0] for c in frappe.db.sql("select column_name from information_schema.columns where table_name = 'tabSessions' and table_schema = %s", (frappe.conf.db_name,))]
	if "device" in columns:
		return
	if frappe.db.db_type == "postgres":
		frappe.db.sql_ddl('alter table "tabSessions" add column "device" varchar(255) default \'desktop\'')
	else:
		frappe.db.sql_ddl("alter table `tabSessions` add column `device` varchar(255) default 'desktop'")
