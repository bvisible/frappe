// Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt

// for translation
frappe._ = function (txt, replace, context = null) {
	if (!txt) return txt;
	if (typeof txt != "string") return txt;

	let translated_text = "";

	let key = txt; // txt.replace(/\n/g, "");
	//// Neoffice — `context` may also be a list, tried in order (upstream takes one: `${key}:${context}`).
	//// A phrase said of one doctype ("New {0}", "Create your first {0}", "View all {0}") passes the
	//// doctype's name as its context: French cannot build « Nouvelle relance » from « Nouveau(elle) {0} »
	//// and a label, the gender and the elision belong to the noun. So a catalogue may say the whole
	//// phrase of one doctype under its name (msgctxt "Dunning" msgid "New {0}" msgstr "Nouvelle relance"),
	//// and every doctype it does not name keeps the phrase as before (maintenance#2161).
	for (const each of Array.isArray(context) ? context : context ? [context] : []) {
		translated_text = frappe._messages[`${key}:${each}`];
		if (translated_text) break;
	}

	if (!translated_text) {
		translated_text = frappe._messages[key] || txt;
	}

	if (replace && typeof replace === "object") {
		translated_text = $.format(translated_text, replace);
	}
	return translated_text;
};

window.__ = frappe._;

frappe.get_languages = function () {
	if (!frappe.languages) {
		frappe.languages = [];
		$.each(frappe.boot.lang_dict, function (lang, value) {
			frappe.languages.push({ label: lang, value: value });
		});
		frappe.languages = frappe.languages.sort(function (a, b) {
			return a.value < b.value ? -1 : 1;
		});
	}
	return frappe.languages;
};
