frappe.ui.form.ControlDatetime = class ControlDatetime extends frappe.ui.form.ControlDate {
	set_formatted_input(value) {
		//// Neoffice — upstream version-15's set_formatted_input (no datetime_format any more), see below.
		if (this.timepicker_only) return;
		if (!this.datepicker) return;
		if (!value) {
			this.datepicker.clear();
			return;
		} else if (value.toLowerCase() === "today") {
			value = this.get_now_date();
		} else if (value.toLowerCase() === "now") {
			value = frappe.datetime.now_datetime();
		}
		//// Neoffice — upstream version-15's own code, taken ahead of our next merge (cherry-picks of
		//// frappe/frappe a65f3399e3, 74205be642, 2bffaf4f9e; maintenance#1246). Our older copy
		//// re-selected the date on the first render, the picker's change wrote back the value it
		//// displays (no seconds): every form with a Datetime field opened « Not Saved », and a
		//// save truncated 08:43:20.629 to 08:43. Now the first render only sets the selection.
		const raw_value = value;
		let should_refresh = this.last_value && this.last_value !== value;
		value = this.format_for_input(value);
		this.$input && this.$input.val(value);
		if (should_refresh) {
			this.datepicker.selectDate(frappe.datetime.user_to_obj(value));
		} else if (value && !this.datepicker.selectedDates.length) {   //// Neoffice — upstream's, see above
			const date_obj = frappe.datetime.str_to_obj(raw_value);
			this.datepicker.selectedDates = [date_obj];
			this.datepicker.viewDate = date_obj;
			this.datepicker.lastSelectedDate = date_obj;
		}
	}

	get_start_date() {
		this.value = this.value == null || this.value == "" ? undefined : this.value;
		let value = frappe.datetime.convert_to_user_tz(this.value);
		return frappe.datetime.str_to_obj(value);
	}
	set_date_options() {
		super.set_date_options();
		this.today_text = __("Now");
		let sysdefaults = frappe.boot.sysdefaults;
		this.date_format = frappe.defaultDatetimeFormat;
		let time_format =
			sysdefaults && sysdefaults.time_format ? sysdefaults.time_format : "HH:mm:ss";
		$.extend(this.datepicker_options, {
			timepicker: true,
			timeFormat: time_format.toLowerCase().replace("mm", "ii"),
		});
	}
	get_now_date() {
		return frappe.datetime.now_datetime(true);
	}
	parse(value) {
		if (value) {
			value = frappe.datetime.user_to_str(value, false);

			if (!frappe.datetime.is_system_time_zone()) {
				value = frappe.datetime.convert_to_system_tz(value, true);
			}

			if (value == "Invalid date") {
				value = "";
			}
		}
		return value;
	}
	format_for_input(value) {
		if (!value) return "";
		return frappe.datetime.str_to_user(value, false);
	}
	set_description() {
		//// Neoffice — translated before the time zone is added (upstream develop's fix, not yet in
		//// version-15): « description<br>Europe/Paris » is in no catalogue, so every described
		//// Datetime field showed its description in English (maintenance#1246).
		const description = this.df.description
			? __(this.df.description, null, this.df.parent)
			: this.df.description;
		const time_zone = this.get_user_time_zone();

		if (!this.df.hide_timezone) {
			// Always show the timezone when rendering the Datetime field since the datetime value will
			// always be in system_time_zone rather then local time.

			if (!description) {
				this.df.description = time_zone;
			} else if (!description.includes(time_zone)) {
				this.df.description = description + "<br>" + time_zone;   //// Neoffice — the translated text, see above
			}
		}
		super.set_description();
	}
	get_user_time_zone() {
		return frappe.boot.time_zone ? frappe.boot.time_zone.user : frappe.sys_defaults.time_zone;
	}
	set_datepicker() {
		super.set_datepicker();
		if (this.datepicker.opts.timeFormat.indexOf("s") == -1) {
			// No seconds in time format
			const $tp = this.datepicker.timepicker;
			$tp.$seconds.parent().css("display", "none");
			$tp.$secondsText.css("display", "none");
			$tp.$secondsText.prev().css("display", "none");
		}
	}

	get_model_value() {
		let value = super.get_model_value();
		if (!value && !this.doc) {
			value = this.last_value;
		}
		return !value ? "" : frappe.datetime.get_datetime_as_string(value);
	}
};
