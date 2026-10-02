//// Neoffice — added file (no upstream equivalent). A link to another document opens that
//// document's own form in a dialog: the reader looks at it or edits it without leaving the page,
//// and what is saved there comes back into the document the link was clicked from (a product
//// description corrected from an invoice reaches the invoice lines that still carried the old
//// one). A double click, or « Open full page », still goes to the document.
////
//// The form is the real frappe.ui.form.Form, mounted in « dialog mode » (frm.form_dialog is set):
//// form.js, toolbar.js, tab.js and sidebar/floating_nav_arrows.js leave the page alone in that
//// mode (no browser title, no breadcrumbs, no hero, no record arrows, no route hooks, tab ids of
//// its own) and form_dialog.scss hides the page head, the sidebar, the activity and the
//// dashboard: what remains is the document's tabs, fields and tables.
////
//// Entry points: router.js (a click on a link drawn by the Link formatter) and controls/link.js
//// (the arrow at the end of a Link field). Server side: frappe/desk/form_dialog.py checks access
//// before anything opens; boot.py sends the switch and the excluded doctypes (hook
//// link_dialog_exclude).

frappe.provide("frappe.ui.form");

// A single click waits this long to know whether a second one follows (a double click goes to the
// document). The loading starts at pointerdown, so the wait is not lost.
const CLICK_DELAY = 250;
// A second click that lands on the dialog this soon, this close, was the end of a slow double click.
const SLOW_DOUBLE_CLICK = 500;
const SLOW_DOUBLE_CLICK_DISTANCE = 8;
const PREFETCH_TTL = 10000;
// Where a document link is the value of a field: form fields, table rows, report and list cells.
const LINK_CONTEXTS = ".frappe-control, .grid-row, .dt-cell, .list-row";
// Navigation and chrome: their links keep going where they point.
const NAVIGATION_CONTEXTS =
	".list-subject, .form-dashboard, .form-sidebar, .form-footer, .page-head, .awesomplete, .dropdown-menu, .navbar";
// What a saved change may update in the document the link was clicked from: texts and pictures
// only - never an amount, a quantity, a unit, an account or a date.
const PROPAGATED_TYPES = [
	"Data",
	"Small Text",
	"Text",
	"Long Text",
	"Text Editor",
	"Markdown Editor",
	"HTML Editor",
	"Read Only",
	"Attach Image",
];

frappe.ui.form.FormDialog = class FormDialog {
	// Open form dialogs, the lowest first; one reusable frappe.ui.Dialog per level of the stack.
	static stack = [];
	static levels = [];
	static pending = null;
	static last_open = null;
	static prefetched = {};

	static enabled() {
		const conf = frappe.boot && frappe.boot.link_dialog;
		return !!(conf && conf.enabled);
	}

	static excluded(doctype) {
		const conf = (frappe.boot && frappe.boot.link_dialog) || {};
		return (conf.exclude || []).includes(doctype);
	}

	// The document an element links to: a link drawn by the Link formatter (data-doctype and
	// data-name) or the arrow of a Link field (data-dialog-doctype and data-dialog-name).
	static target_of(el) {
		if (!el || !el.dataset) return null;
		const doctype = el.dataset.dialogDoctype || el.dataset.doctype;
		const name = el.dataset.dialogName || el.dataset.name;
		return doctype && name ? { doctype, name } : null;
	}

	// The form an element belongs to: a form dialog's, or the page's.
	static form_of(el) {
		const host = el && el.closest && el.closest(".form-dialog-host");
		if (host) return $(host).data("frm") || null;
		const page = frappe.container && frappe.container.page;
		if (page && page.frm && el && page.contains(el)) return page.frm;
		return null;
	}

	static is_candidate(el, target) {
		if (!FormDialog.enabled() || FormDialog.excluded(target.doctype)) return false;
		if (!el.closest) return false;
		// The ID column of a report: the row's own document (maintenance#924 shows it in the panel).
		if (el.dataset.neoId) return false;
		if (!el.dataset.dialogDoctype && !el.closest(LINK_CONTEXTS)) return false;
		if (el.closest(NAVIGATION_CONTEXTS)) return false;
		// A link to the document itself: there is nothing to open.
		const frm = FormDialog.form_of(el);
		if (frm && frm.doctype === target.doctype && frm.docname === target.name) return false;
		return true;
	}

	// Called by router.js and controls/link.js for a click on a document link. Returns true when the
	// click is taken (a dialog opens now or after CLICK_DELAY); false lets the link be followed: the
	// second click of a double click, a modified click (new tab), or anything that is not a link
	// of a field.
	static intercept(e, el, ctx = {}) {
		try {
			const target =
				ctx.doctype && ctx.name
					? { doctype: ctx.doctype, name: ctx.name }
					: FormDialog.target_of(el);
			if (!target || !FormDialog.is_candidate(el, target)) return false;
			const oe = e.originalEvent || e;
			if (oe.button > 0 || oe.ctrlKey || oe.metaKey || oe.shiftKey || oe.altKey)
				return false;

			if (oe.detail >= 2) {
				// The second click of a double click: the caller follows the link.
				FormDialog.cancel_pending();
				FormDialog.close_opened_by(el);
				return false;
			}

			e.preventDefault();
			e.stopPropagation();
			FormDialog.cancel_pending();
			const origin_frm = ctx.frm || FormDialog.form_of(el);
			const click = {
				el,
				x: oe.clientX,
				y: oe.clientY,
				t: Date.now(),
				href: el.getAttribute("href"),
			};
			const open = () => {
				FormDialog.pending = null;
				FormDialog.open(target.doctype, target.name, { origin_frm, click });
			};
			// Keyboard (Enter) and touch have no double click: open at once.
			if (!oe.detail || oe.pointerType === "touch") open();
			else FormDialog.pending = { el, timer: setTimeout(open, CLICK_DELAY) };
			return true;
		} catch (err) {
			// Never break a link: on any surprise, the link is followed as before.
			console.error(err);
			return false;
		}
	}

	// A click in a cell of a read-only table (grid_row.js), where the row would open: a document
	// link of the cell opens its form in a dialog, and the second click of a double click goes to
	// the document (the row's handler stops the event before the router could follow the link).
	static grid_click(e) {
		const el = e.target && e.target.closest && e.target.closest("a[data-doctype][data-name]");
		if (!el) return false;
		if (FormDialog.intercept(e, el)) return true;
		const oe = e.originalEvent || e;
		const target = FormDialog.target_of(el);
		if (
			oe.detail >= 2 &&
			!oe.button &&
			!(oe.ctrlKey || oe.metaKey || oe.shiftKey || oe.altKey) &&
			target &&
			FormDialog.is_candidate(el, target)
		) {
			e.preventDefault();
			FormDialog.go(target.doctype, target.name);
			return true;
		}
		return false;
	}

	static cancel_pending() {
		if (FormDialog.pending) clearTimeout(FormDialog.pending.timer);
		FormDialog.pending = null;
	}

	// The second click of a slow double click on the link itself: drop the dialog its first click
	// just opened, the link is then followed.
	static close_opened_by(el) {
		const last = FormDialog.last_open;
		if (!last || last.click.el !== el || Date.now() - last.click.t > SLOW_DOUBLE_CLICK) return;
		FormDialog.last_open = null;
		last.close();
	}

	// The server's answer on access, asked once for a click (prefetch at pointerdown) and kept a few
	// seconds; the meta and the document are made sure of at every opening - a no-op when they are
	// already loaded, and a reload when a discard dropped the local copy in between.
	static access(doctype, name) {
		const key = `${doctype}\u0000${name}`;
		const hit = FormDialog.prefetched[key];
		if (hit && Date.now() - hit.t < PREFETCH_TTL) return hit.promise;
		const promise = frappe.xcall("frappe.desk.form_dialog.get_access", { doctype, name });
		FormDialog.prefetched[key] = { t: Date.now(), promise };
		promise
			.catch(() => {})
			.then(() => setTimeout(() => delete FormDialog.prefetched[key], PREFETCH_TTL));
		return promise;
	}

	static load(doctype, name) {
		return FormDialog.access(doctype, name).then((access) => {
			if (!access || access.status !== "ok") return access || { status: "forbidden" };
			return Promise.all([
				frappe.model.with_doctype(doctype),
				new Promise((resolve) => frappe.model.with_doc(doctype, name, () => resolve())),
			]).then(() => access);
		});
	}

	static prefetch(doctype, name) {
		FormDialog.load(doctype, name).catch(() => {});
	}

	static open(doctype, name, opts = {}) {
		// Already on screen (a click while it loads): nothing more.
		const open = FormDialog.stack.find((f) => f.doctype === doctype && f.name === name);
		if (open) return Promise.resolve(open);
		const dialog = new FormDialog(doctype, name, opts);
		FormDialog.stack.push(dialog);
		return dialog.show().then(() => dialog);
	}

	static close_all() {
		[...FormDialog.stack].reverse().forEach((f) => f.close());
	}

	// Go to the document, as a click did before.
	static go(doctype, name) {
		FormDialog.close_all();
		frappe.set_route("Form", doctype, name);
	}

	static refuse(status) {
		frappe.show_alert({
			message:
				status === "not_found"
					? __("This document no longer exists.")
					: __("You are not allowed to open this document."),
			indicator: "orange",
		});
	}

	// One frappe.ui.Dialog per level of the stack, kept for the next document; inside it, one
	// form per doctype, kept too (a Form is heavy to build and registers listeners once).
	static level(depth) {
		if (FormDialog.levels[depth]) return FormDialog.levels[depth];
		const dialog = new frappe.ui.Dialog({ title: "", size: "extra-large" });
		const level = { depth, dialog, hosts: {}, current: null };
		dialog.$wrapper.addClass("form-dialog");
		dialog.set_primary_action(__("Save"), () => level.current && level.current.save());
		dialog.set_secondary_action_label(__("Open full page"));
		dialog.set_secondary_action(() => level.current && level.current.open_full_page());
		// The cross (and Escape, which clicks it) asks before dropping unsaved changes.
		dialog
			.get_close_btn()
			.removeAttr("data-dismiss")
			.on("click", (e) => {
				e.preventDefault();
				if (level.current) level.current.request_close();
				else dialog.hide();
			});
		dialog.onhide = () => level.current && level.current.on_hidden();
		level.$loading = $(
			`<div class="form-dialog-loading text-muted">${__("Loading...")}</div>`
		).appendTo(dialog.$body);
		FormDialog.levels[depth] = level;
		return level;
	}

	static copy(doc) {
		return doc ? JSON.parse(JSON.stringify(doc)) : {};
	}

	constructor(doctype, name, opts) {
		this.doctype = doctype;
		this.name = name;
		this.origin_frm = opts.origin_frm || null;
		this.click = opts.click || null;
		this.depth = FormDialog.stack.length;
		// What the page had before the dialog's form took them.
		this.prev_cur_frm = window.cur_frm;
		this.prev_title = document.title;
	}

	async show() {
		this.watch_slow_double_click();
		// Access first (prefetched at pointerdown, so usually already answered): a refused or excluded
		// document shows no dialog at all, not one that flashes open and shut.
		let access;
		try {
			access = await FormDialog.access(this.doctype, this.name);
		} catch (e) {
			// The server did not answer: go to the document as before.
			return this.give_up(() => FormDialog.go(this.doctype, this.name));
		}
		if (!access || access.status !== "ok") {
			return this.give_up(() =>
				access && access.status === "excluded"
					? FormDialog.go(this.doctype, this.name)
					: FormDialog.refuse(access && access.status)
			);
		}

		const level = FormDialog.level(this.depth);
		level.current = this;
		// The hover card of the link (ui/link_preview.js) would stay over the dialog.
		frappe.app?.link_preview?.clear_all_popovers?.();
		this.level = level;
		this.dialog = level.dialog;
		this.dialog.set_title(
			`<span class="form-dialog-doctype">${__(
				this.doctype
			)}</span> ${frappe.utils.escape_html(this.name)}`
		);
		this.dialog.get_primary_btn().addClass("hide");
		Object.values(level.hosts).forEach((h) => h.$host.addClass("hide"));
		level.$loading.removeClass("hide");
		// In the document now, so that the form can be drawn right away: Bootstrap would only add
		// it once the backdrop has faded in, and tabs can't be shown in a detached node.
		if (!this.dialog.$wrapper.get(0).isConnected) this.dialog.$wrapper.appendTo(document.body);
		this.dialog.show();
		this.hold_open(false);

		try {
			await FormDialog.load(this.doctype, this.name);
		} catch (e) {
			this.close();
			return FormDialog.go(this.doctype, this.name);
		}
		if (this.hidden) return;
		this.mount();
		FormDialog.last_open = this;
	}

	// Nothing was shown yet: leave the stack, then go or say why.
	give_up(then) {
		this.hidden = true;
		this.stop_watch && this.stop_watch();
		FormDialog.stack = FormDialog.stack.filter((f) => f !== this);
		return then();
	}

	mount() {
		const level = this.level;
		let host = level.hosts[this.doctype];
		if (!host) {
			const $host = $('<div class="form-dialog-host"></div>').appendTo(this.dialog.$body);
			host = level.hosts[this.doctype] = { $host, frm: null };
		}
		level.$loading.addClass("hide");
		host.$host.removeClass("hide");
		this.snapshot = FormDialog.copy(frappe.get_doc(this.doctype, this.name));
		if (!host.frm) {
			host.frm = new frappe.ui.form.Form(this.doctype, host.$host.get(0), true);
			// Tab ids and the like of this form, distinct from the page's form of the same doctype.
			host.frm.form_dialog_uid = `form-dialog-${level.depth}`;
			host.$host.data("frm", host.frm);
		}
		this.frm = host.frm;
		this.frm.form_dialog = this;
		host.$host
			.off(".form_dialog")
			.on("dirty.form_dialog render_complete.form_dialog", () => this.sync_state());
		this.frm.refresh(this.name);
		this.sync_state();
	}

	sync_state() {
		const frm = this.frm;
		if (!frm || !frm.doc || this.hidden) return;
		this.dialog.set_title(this.title_html());
		const dirty = frm.is_dirty();
		this.dialog.get_primary_btn().toggleClass("hide", !dirty);
		this.hold_open(dirty);
	}

	title_html() {
		const doc = this.frm.doc;
		const title = frappe.utils.escape_html(this.frm.get_title() || doc.name);
		const indicator = frappe.get_indicator(doc, this.doctype);
		const pill =
			indicator && indicator[0]
				? `<span class="indicator-pill ${
						indicator[1]
				  } form-dialog-status">${frappe.utils.escape_html(indicator[0])}</span>`
				: "";
		return `<span class="form-dialog-doctype">${__(this.doctype)}</span> ${title} ${pill}`;
	}

	// A dialog with unsaved changes closes only through its buttons and its cross, which asks
	// first (the rule of the dialogs that hold typing); a clean one closes like any other.
	hold_open(hold) {
		const d = this.dialog;
		d.no_cancel_flag = hold;
		const modal = d.$wrapper.data("bs.modal");
		const config = modal && (modal._config || modal.options);
		if (config) {
			config.backdrop = hold ? "static" : true;
			// Escape goes through Frappe's own handler only (ui/keyboard.js, which clicks the cross):
			// with Bootstrap's as well, one Escape closed the top dialog of a stack and then the one
			// below, which had become cur_dialog in between.
			config.keyboard = false;
		}
	}

	watch_slow_double_click() {
		const click = this.click;
		if (!click || !click.t) return;
		const until = click.t + SLOW_DOUBLE_CLICK;
		const handler = (ev) => {
			if (Date.now() > until) return stop();
			if (
				Math.abs(ev.clientX - click.x) > SLOW_DOUBLE_CLICK_DISTANCE ||
				Math.abs(ev.clientY - click.y) > SLOW_DOUBLE_CLICK_DISTANCE
			)
				return;
			ev.preventDefault();
			ev.stopPropagation();
			stop();
			FormDialog.go(this.doctype, this.name);
		};
		const stop = () => document.removeEventListener("click", handler, true);
		document.addEventListener("click", handler, true);
		setTimeout(stop, Math.max(0, until - Date.now()));
		this.stop_watch = stop;
	}

	save() {
		const frm = this.frm;
		if (!frm || !frm.doc || frm.doc.docstatus === 2) return;
		const action = frm.doc.docstatus === 1 ? "Update" : "Save";
		return frm.save(action, (r) => {
			if (r && !r.exc) {
				this.propagate();
				this.close();
			}
		});
	}

	request_close() {
		if (this.frm && this.frm.is_dirty()) {
			frappe.confirm(__("Discard the changes made to {0}?", [this.frm.get_title()]), () =>
				this.close({ discard: true })
			);
			return;
		}
		this.close();
	}

	close({ discard = false } = {}) {
		this.discard = discard;
		const d = this.dialog;
		if (!d) return this.on_hidden();
		const modal = d.$wrapper.data("bs.modal");
		// Still fading in: Bootstrap ignores a hide during that transition, hide once it is shown.
		if (modal && modal._isTransitioning && !d.display) {
			d.$wrapper.one("shown.bs.modal", () => d.hide());
			return;
		}
		if (d.display || d.is_visible) d.hide();
		else this.on_hidden();
	}

	// The page (or the dialog below) is the one being worked in again.
	on_hidden() {
		if (this.hidden) return;
		this.hidden = true;
		if (this.level && this.level.current === this) this.level.current = null;
		FormDialog.stack = FormDialog.stack.filter((f) => f !== this);
		if (FormDialog.last_open === this) FormDialog.last_open = null;
		this.stop_watch && this.stop_watch();
		const frm = this.frm;
		if (frm) {
			removeEventListener("beforeunload", frm.beforeUnloadListener, { capture: true });
			const row = frappe.ui.form.editable_row;
			if (row && row.grid && row.grid.frm === frm) frappe.ui.form.editable_row = null;
			// Dropped changes must not stay in the local copy that a page would show next.
			if (this.discard && frm.doc && frm.doc.__unsaved) {
				frappe.model.remove_from_locals(this.doctype, this.name);
			}
		}
		window.cur_frm = this.prev_cur_frm;
		document.title = this.prev_title;
		// The realtime listener for comments is one for the whole desk: give it back.
		const prev = this.prev_cur_frm;
		if (prev && prev.doc && prev.docname && prev.setup_docinfo_change_listener) {
			prev.setup_docinfo_change_listener();
		}
	}

	open_full_page() {
		// Unsaved changes stay in the local copy: the page shows them, still to be saved.
		FormDialog.go(this.doctype, this.name);
	}

	// The form found no read permission (form.js refresh): close, and say so.
	refuse_from_form() {
		this.close();
		FormDialog.refuse("forbidden");
	}

	// After a save: the document the link was clicked from takes the new values where it held them
	// from this one - fields fetched from the link (fetch_from), and texts copied when the link was
	// chosen (an invoice line's item name and description) that still carry the old value. Nothing
	// changes in a submitted document, except fields allowed on submit; the reader saves the
	// document themselves.
	propagate() {
		const origin = this.origin_frm;
		if (!origin || !origin.doc || origin === this.frm) return;
		const saved = frappe.get_doc(this.doctype, this.name);
		if (!saved) return;
		const meta = frappe.get_meta(this.doctype);
		const same = (a, b) => cstr(a) === cstr(b);
		const changed = {};
		meta.fields.forEach((df) => {
			if (!PROPAGATED_TYPES.includes(df.fieldtype)) return;
			if (!same(this.snapshot[df.fieldname], saved[df.fieldname])) {
				changed[df.fieldname] = [this.snapshot[df.fieldname], saved[df.fieldname]];
			}
		});
		const title_field = meta.title_field || "name";
		if (changed[title_field] && frappe.utils.add_link_title) {
			frappe.utils.add_link_title(this.doctype, this.name, saved[title_field]);
		}
		if (!Object.keys(changed).length) return;

		const submitted = origin.doc.docstatus === 1;
		if (origin.doc.docstatus === 2) return;
		const docs = [origin.doc];
		frappe.get_meta(origin.doctype).fields.forEach((df) => {
			if (frappe.model.table_fields.includes(df.fieldtype)) {
				(origin.doc[df.fieldname] || []).forEach((row) => docs.push(row));
			}
		});

		let updated = 0;
		docs.forEach((doc) => {
			const fields = frappe.get_meta(doc.doctype).fields;
			const links = fields.filter(
				(df) =>
					doc[df.fieldname] === this.name &&
					((df.fieldtype === "Link" && df.options === this.doctype) ||
						(df.fieldtype === "Dynamic Link" && doc[df.options] === this.doctype))
			);
			links.forEach((link_df) => {
				fields.forEach((df) => {
					if (!PROPAGATED_TYPES.includes(df.fieldtype)) return;
					if (submitted && !df.allow_on_submit) return;
					const current = doc[df.fieldname];
					let source = null;
					if (df.fetch_from) {
						const [link_field, source_field] = df.fetch_from.split(".");
						if (link_field !== link_df.fieldname || !changed[source_field]) return;
						if (df.fetch_if_empty && current) return;
						source = source_field;
					} else if (changed[df.fieldname] && same(current, changed[df.fieldname][0])) {
						source = df.fieldname;
					}
					if (source && !same(current, changed[source][1])) {
						frappe.model.set_value(
							doc.doctype,
							doc.name,
							df.fieldname,
							changed[source][1]
						);
						updated++;
					}
				});
			});
		});
		if (updated) {
			origin.refresh_fields();
			frappe.show_alert({
				message: __("{0} value(s) updated from {1}", [updated, this.frm.get_title()]),
				indicator: "green",
			});
		}
	}
};

// Load while the button is still down: the single-click wait then costs nothing. And no word
// selection on a double click.
["pointerdown", "mousedown"].forEach((type) => {
	document.addEventListener(
		type,
		(e) => {
			if (e.button !== 0 || e.ctrlKey || e.metaKey || e.shiftKey || e.altKey) return;
			const FormDialog = frappe.ui.form.FormDialog;
			const el =
				e.target &&
				e.target.closest &&
				e.target.closest("a[data-doctype][data-name], a[data-dialog-doctype]");
			if (!el || !FormDialog.enabled()) return;
			const target = FormDialog.target_of(el);
			if (!target || !FormDialog.is_candidate(el, target)) return;
			if (type === "pointerdown") FormDialog.prefetch(target.doctype, target.name);
			else if (e.detail > 1) e.preventDefault();
		},
		true
	);
});
