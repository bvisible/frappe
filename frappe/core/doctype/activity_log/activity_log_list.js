frappe.listview_settings["Activity Log"] = {
	get_indicator: function (doc) {
		if (doc.operation == "Login" && doc.status == "Success") return [__(doc.status), "green"];
		else if (doc.operation == "Login" && doc.status == "Failed")
			return [__(doc.status), "red"];
	},
	onload: function (listview) {
		frappe.require("logtypes.bundle.js", () => {
			frappe.utils.logtypes.show_log_retention_message(cur_list.doctype);
		});
	},
	//// Neoffice — Frappe writes the subject in the language of whichever request logged the
	//// event (the scheduler, or the session that took over, writes English) and never runs the
	//// reason of a logout through _(): "Admin logged out: Session Expired" on a French desk.
	//// Translated here, when the log is read and in the reader's language, so the rows already
	//// stored are fixed too. Plain text on purpose: the list prints the subject with
	//// textContent, so the <strong> Frappe stores around the reason would show.
	formatters: {
		subject: function (value) {
			if (!value) return value;
			const reasons = {
				"Session Expired": __("Session Expired"),
				"Logged In From Another Session": __("Logged In From Another Session"),
				"User Manually Logged Out": __("User Manually Logged Out"),
				"Force Logged out by the user": __("Force Logged out by the user"),
				"Invalid login credentials": __("Invalid login credentials"),
			};
			const reason = (r) =>
				Object.prototype.hasOwnProperty.call(reasons, r) ? reasons[r] : r;
			const text = String(value);
			let m = text.match(/^([\s\S]+) logged in$/);
			if (m) return __("{0} logged in", [m[1]]);
			m = text.match(/^([\s\S]+) logged out: <strong>([\s\S]*)<\/strong>$/);
			if (m) return __("{0} logged out: {1}", [m[1], reason(m[2])]);
			// Already written in the reader's language, with an English reason.
			m = text.match(/^([\s\S]*: )<strong>([\s\S]*)<\/strong>$/);
			if (m) return m[1] + reason(m[2]);
			// What Frappe logs for a failure ("Invalid login credentials") is a catalogue string.
			return __(text.replace(/<\/?strong>/g, ""));
		},
	},
};

//// Neoffice — the report view, which is the one this log opens in, does not read `formatters`
//// (see report_view.js): same translation, as `report_formatters`.
frappe.listview_settings["Activity Log"].report_formatters = {
	subject: (value, df, doc) =>
		frappe.listview_settings["Activity Log"].formatters.subject(value, df, doc),
};
