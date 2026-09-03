frappe.provide("frappe.ui");

frappe.ui.Scanner = class Scanner {
	constructor(options) {
		this.dialog = null;
		this.handler = null;
		this.options = options;
		this.is_alive = false;

		if (!("multiple" in this.options)) {
			this.options.multiple = false;
		}
		if (options.container) {
			this.$scan_area = $(options.container);
			this.scan_area_id = frappe.dom.set_unique_id(this.$scan_area);
		}
		if (options.dialog) {
			this.dialog = this.make_dialog();
			this.dialog.show();
		}
	}

	scan() {
		this.load_lib().then(() => this.start_scan());
	}

	start_scan() {
		if (!this.handler) {
			this.handler = new Html5Qrcode(this.scan_area_id); // eslint-disable-line
		}

		//// Neoffice - start_scan rewritten (167333aa9d, 2026-08-07 "fix(scanner): the dialog closed itself on
		//// any desktop, without a word"): upstream passes { facingMode: "environment" } - the REAR camera -
		//// inline, and its .catch closed the dialog. On a desktop that constraint cannot be satisfied, so
		//// every scanner surface of the desk (sales dialogs, POS, item code) opened a window that vanished
		//// instantly with nothing said. The callbacks are hoisted so the same ones serve both attempts: rear
		//// camera first (right on a phone, where scanning happens), then whatever camera exists, then
		//// show_camera_error below.
		const config = { fps: 10, qrbox: 250 };
		const on_success = (decodedText, decodedResult) => {
			if (this.options.on_scan) {
				try {
					this.options.on_scan(decodedResult);
				} catch (error) {
					console.error(error);
				}
			}
			if (!this.options.multiple) {
				this.stop_scan();
				this.hide_dialog();
			}
		};
		const on_parse_error = () => {
			// parse error, ignore it.
		};

		// "environment" is the rear camera. A desktop has only a front one, so
		// the constraint is unsatisfiable there and start() rejects — which used
		// to close the dialog on the spot, with nothing said. Ask for the rear
		// camera first (right on a phone), then take whatever camera exists.
		this.handler
			.start({ facingMode: "environment" }, config, on_success, on_parse_error)
			.catch(() => this.handler.start({ facingMode: "user" }, config, on_success, on_parse_error))
			.catch((err) => {
				this.is_alive = false;
				this.show_camera_error(err);
				console.error(err);
			});
		this.is_alive = true;
	}

	//// Neoffice - added method (167333aa9d, 2026-08-07): replaces upstream's silent hide_dialog(). The
	//// viewfinder is replaced by the reason - permission refused, or no camera at all - and by the way
	//// out that needs none: a barcode reader types into the focused field. Falls back to an alert when
	//// the scan area is not in the DOM. Strings are in frappe/locale/fr.po (same commit).
	show_camera_error(err) {
		// Replace the viewfinder with the reason. Closing in silence leaves the
		// user clicking a button that seems to do nothing.
		const denied = err && /NotAllowed|Permission/i.test(err.name || String(err));
		const message = denied
			? __("Camera access was refused. Allow it for this site, then try again.")
			: __("No camera available on this device.");
		const hint = __("A barcode reader works without this: put the cursor in the field and scan.");

		if (this.$scan_area && this.$scan_area.length) {
			this.$scan_area.html(
				`<div class="text-muted" style="padding: 24px 8px; text-align: center; line-height: 1.6;">
					<div>${frappe.utils.escape_html(message)}</div>
					<div style="font-size: var(--text-sm); margin-top: 6px;">${frappe.utils.escape_html(hint)}</div>
				</div>`
			);
		} else {
			frappe.show_alert({ message: message, indicator: "orange" }, 6);
			this.hide_dialog();
		}
	}

	stop_scan() {
		if (this.handler && this.is_alive) {
			this.handler.stop().then(() => {
				this.is_alive = false;
				this.$scan_area.empty();
				this.hide_dialog();
			});
		}
	}

	make_dialog() {
		let dialog = new frappe.ui.Dialog({
			title: __("Scan QRCode"),
			fields: [
				{
					fieldtype: "HTML",
					fieldname: "scan_area",
				},
			],
			on_page_show: () => {
				this.$scan_area = dialog.get_field("scan_area").$wrapper;
				this.$scan_area.addClass("barcode-scanner");
				this.scan_area_id = frappe.dom.set_unique_id(this.$scan_area);
				this.scan();
			},
			on_hide: () => {
				this.stop_scan();
			},
		});
		return dialog;
	}

	hide_dialog() {
		this.dialog && this.dialog.hide();
	}

	load_lib() {
		return frappe.require("/assets/frappe/node_modules/html5-qrcode/html5-qrcode.min.js");
	}
};
