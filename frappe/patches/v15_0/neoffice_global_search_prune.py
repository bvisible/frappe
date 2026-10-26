# //// Neoffice — added file (no upstream equivalent).
# ////
# //// maintenance#1893: b6f135ea30 indexed the name of every document of every non-child doctype, so
# //// __global_search filled with logs, versions, comments and ledger rows (3.7 M rows on osiris, 98 % of
# //// them never returned by a search) that every full-text query reads. The code is fixed in
# //// Meta.get_global_search_fields. This removes the rows whose document no longer exists
# //// (prune_global_search), copies the rows of the doctypes that still index something into a fresh table,
# //// rebuilds it (rows inserted in bulk wait in InnoDB's full-text cache, which every query then reads:
# //// 11 ms for any word on osiris instead of 1), and swaps it in for the old one (deleting millions of rows
# //// leaves an InnoDB full-text index as large until an OPTIMIZE). Idempotent: on a clean index it copies
# //// the same rows again.
import frappe
from frappe.utils.global_search import doctype_indexed, prune_global_search


def execute():
	if frappe.db.db_type != "mariadb" or "__global_search" not in frappe.db.get_tables(cached=False):
		return
	keep = tuple(
		doctype
		for doctype in frappe.db.sql_list("select distinct doctype from `__global_search`")
		if doctype == "Static Web Page" or doctype_indexed(doctype)
	)
	prune_global_search(list(keep))
	frappe.db.sql_ddl("drop table if exists `__global_search_kept`")
	frappe.db.sql_ddl("drop table if exists `__global_search_dropped`")
	frappe.db.sql_ddl("create table `__global_search_kept` like `__global_search`")
	if keep:
		frappe.db.sql(
			"insert into `__global_search_kept` select * from `__global_search` where doctype in %s", (keep,)
		)
	frappe.db.commit()
	frappe.db.sql("optimize table `__global_search_kept`")
	frappe.db.sql_ddl(
		"rename table `__global_search` to `__global_search_dropped`, `__global_search_kept` to `__global_search`"
	)
	frappe.db.sql_ddl("drop table `__global_search_dropped`")
