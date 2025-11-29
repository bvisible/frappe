frappe.provide("frappe.model.user_settings");

$.extend(frappe.model.user_settings, {
	get: function (doctype) {
		return frappe
			.call("frappe.model.utils.user_settings.get", { doctype })
			.then((r) => JSON.parse(r.message || "{}"));
	},
	save: function (doctype, key, value) {
		if (frappe.session.user === "Guest") return Promise.resolve();

		const old_user_settings = frappe.model.user_settings[doctype] || {};
		const new_user_settings = $.extend(true, {}, old_user_settings); // deep copy

		if ($.isPlainObject(value)) {
			new_user_settings[key] = new_user_settings[key] || {};
			$.extend(new_user_settings[key], value);
		} else {
			new_user_settings[key] = value;
		}

		const a = JSON.stringify(old_user_settings);
		const b = JSON.stringify(new_user_settings);
		if (a !== b) {
			// Sync immediately for GridView changes to prevent data loss
			const sync_immediately = key === "GridView";
			return this.update(doctype, new_user_settings, sync_immediately);
		}
		return Promise.resolve(new_user_settings);
	},
	remove: function (doctype, key) {
		var user_settings = frappe.model.user_settings[doctype] || {};
		delete user_settings[key];

		return this.update(doctype, user_settings);
	},
	update: function (doctype, user_settings, sync_immediately = false) {
		if (frappe.session.user === "Guest") return Promise.resolve();
		return frappe.call({
			method: "frappe.model.utils.user_settings.save",
			args: {
				doctype: doctype,
				user_settings: user_settings,
				sync_immediately: sync_immediately,
			},
			callback: function (r) {
				frappe.model.user_settings[doctype] = r.message;
			},
			error: function (r) {
				console.error("Failed to save user settings:", r);
				frappe.show_alert({
					message: __("Failed to save grid settings"),
					indicator: "red",
				});
			},
		});
	},
});

frappe.get_user_settings = function (doctype, key) {
	var settings = frappe.model.user_settings[doctype] || {};
	if (key) {
		settings = settings[key] || {};
	}
	return settings;
};
