frappe.provide("frappe.utils.datatable");

frappe.utils.datatable.get_translations = function () {
	let translations = {};
	translations[frappe.boot.lang] = {
		"Sort Ascending": __("Sort Ascending"),
		"Sort Descending": __("Sort Descending"),
		"Reset sorting": __("Reset sorting"),
		"Remove column": __("Remove column"),
		"No Data": __("No Data"),
		"{count} cells copied": {
			1: __("{count} cell copied"),
			default: __("{count} cells copied"),
		},
		"{count} rows selected": {
			1: __("{count} row selected"),
			default: __("{count} rows selected"),
		},
		//// Neoffice — added block (f1656d6600, 2025-06-17 "add trad and remove +1000"): upstream ships
		//// only the seven frappe-datatable strings above. These extra msgids are handed to the
		//// datatable so its own UI chrome (search box, "Show more") and the Select/Status cell values
		//// it renders itself come out translated — the datatable does not go through frappe's __()
		//// at render time, it looks values up in this table. The bare //// fence is the original
		//// author's. No upstream equivalent in v15.120 or develop.
		//// add traductions for other languages here
		"Search...": __("Search..."),
		"Show more": __("Show more"),
		"Showing": __("Showing"),
		
		"Open": __("Open"),
		"Closed": __("Closed"),
		"Pending": __("Pending"),
		"Draft": __("Draft"),
		"Submitted": __("Submitted"),
		"Cancelled": __("Cancelled"),
		"Completed": __("Completed"),
		"Active": __("Active"),
		"Inactive": __("Inactive"),
		"Paid": __("Paid"),
		"Unpaid": __("Unpaid"),
		"Approved": __("Approved"),
		"Rejected": __("Rejected"),
		"In Progress": __("In Progress"),
		"On Hold": __("On Hold"),
		"Yes": __("Yes"),
		"No": __("No"),
		"Consolidated": __("Consolidated"),
		"Credit Note Issued": __("Credit Note Issued"),
		"Overdue": __("Overdue"),
		"Overdue and Discounted": __("Overdue and Discounted"),
		"Return": __("Return"),
		"Partly Paid": __("Partly Paid"),
		"Unpaid and Discounted": __("Unpaid and Discounted")
	};

	return translations;
};
