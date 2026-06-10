// //// NEOFFICE PATCH — Form hero (NeoCockpit content redesign, stage 2).
//
// A soft-gradient card on top of every form: avatar/initial + big title +
// "Doctype NAME · contact" subtitle, the document's KEY VALUE on the right
// (serif, e.g. grand_total for sales docs — nothing when the doctype has
// none), and a business pipeline (Created → Draft → … ) driven by a small
// per-doctype registry with a docstatus fallback, so EVERY doctype works.
//
// Design source: ~/Downloads/design_handoff_devis_c (dirC-hero), re-skinned
// to the Neoffice brand. Only renders under the cockpit chrome.

frappe.provide("frappe.ui.form");

// ── per-doctype enrichment ──────────────────────────────────────────
// steps: business pipeline labels (first = "Created", always done)
// rank(doc): 1-based index of the CURRENT step; -1 = dead (lost/cancelled)
// value_field: the key figure shown on the right (auto-detected when absent)
const HERO_REGISTRY = {
	Quotation: {
		steps: () => [__("Created"), __("Draft"), __("Sent"), __("Accepted"), __("Order")],
		rank(doc) {
			if (doc.docstatus === 2 || ["Lost", "Expired", "Cancelled"].includes(doc.status)) return -1;
			if (["Ordered", "Partially Ordered"].includes(doc.status)) return 5;
			if (doc.docstatus === 1) return 3;
			return 2;
		},
	},
	"Sales Order": {
		steps: () => [__("Created"), __("Draft"), __("Confirmed"), __("Delivered"), __("Billed")],
		rank(doc) {
			if (doc.docstatus === 2 || ["Cancelled", "Closed"].includes(doc.status)) return -1;
			if (doc.docstatus === 0) return 2;
			if (flt(doc.per_billed) >= 100) return 5;
			if (flt(doc.per_delivered) >= 100) return 4;
			return 3;
		},
	},
	"Sales Invoice": {
		steps: () => [__("Created"), __("Draft"), __("Submitted"), __("Paid")],
		rank(doc) {
			if (doc.docstatus === 2 || doc.status === "Cancelled") return -1;
			if (doc.docstatus === 0) return 2;
			if (doc.status === "Paid") return 4;
			return 3;
		},
	},
	"Purchase Invoice": {
		steps: () => [__("Created"), __("Draft"), __("Submitted"), __("Paid")],
		rank(doc) {
			if (doc.docstatus === 2 || doc.status === "Cancelled") return -1;
			if (doc.docstatus === 0) return 2;
			if (doc.status === "Paid") return 4;
			return 3;
		},
	},
	"Delivery Note": {
		steps: () => [__("Created"), __("Draft"), __("Submitted"), __("Billed")],
		rank(doc) {
			if (doc.docstatus === 2) return -1;
			if (doc.docstatus === 0) return 2;
			if (flt(doc.per_billed) >= 100) return 4;
			return 3;
		},
	},
};

const DEFAULT_PIPELINE = {
	steps: () => [__("Created"), __("Draft"), __("Submitted")],
	rank(doc) {
		if (doc.docstatus === 2) return -1;
		return doc.docstatus === 1 ? 3 : 2;
	},
};

frappe.ui.form.FormHero = class FormHero {
	constructor(frm) {
		this.frm = frm;
	}

	refresh() {
		if (!document.body.classList.contains("neoffice-cockpit")) return;
		if (this.frm.meta.issingle || this.frm.meta.istable) return;

		if (!this.$wrapper) {
			const $host = this.frm.$wrapper.find(".layout-main-section").first();
			if (!$host.length) return;
			this.$wrapper = $('<div class="form-hero"></div>').prependTo($host);
		}
		// new unsaved docs: no hero (nothing meaningful to show yet)
		if (this.frm.doc.__islocal) {
			this.$wrapper.addClass("hide");
			return;
		}
		this.$wrapper.removeClass("hide");
		this.render();
	}

	render() {
		const doc = this.frm.doc;
		const meta = this.frm.meta;
		const title = (meta.title_field && doc[meta.title_field]) || doc.name;
		const initial = (title || "?").trim().charAt(0).toUpperCase();
		const contact = doc.contact_display && doc.contact_display !== title ? doc.contact_display : null;
		const id_part = doc.name === title ? __(this.frm.doctype) : __(this.frm.doctype) + " " + doc.name;
		const sub = [id_part, contact].filter(Boolean).join(" · ");

		// key value (right side): registry override, else auto grand_total
		let value_html = "";
		const vf =
			(HERO_REGISTRY[this.frm.doctype] || {}).value_field ||
			(meta.fields.some((f) => f.fieldname === "grand_total") ? "grand_total" : null);
		if (vf && doc[vf] != null) {
			const amount = format_number(flt(doc[vf]), null, 2);
			const currency = doc.currency || frappe.boot.sysdefaults.currency || "";
			value_html = `
				<div class="form-hero-value">
					<div class="form-hero-amount">${amount}</div>
					<div class="form-hero-currency">${frappe.utils.escape_html(currency)} · ${__("Grand Total")}</div>
				</div>`;
		}

		// pipeline — only for submittable doctypes (masters have no lifecycle)
		const conf = HERO_REGISTRY[this.frm.doctype] || (meta.is_submittable ? DEFAULT_PIPELINE : null);
		if (!conf) {
			this.$wrapper.html(`
				<div class="form-hero-top">
					<div class="form-hero-avatar">${frappe.utils.escape_html(initial)}</div>
					<div class="form-hero-id">
						<div class="form-hero-title">${frappe.utils.escape_html(title)}</div>
						<div class="form-hero-sub">${frappe.utils.escape_html(sub)}</div>
					</div>
					${value_html}
				</div>
			`);
			return;
		}
		const steps = conf.steps();
		const rank = conf.rank(doc);
		const seg = steps
			.map((label, i) => {
				const n = i + 1;
				let cls = "upcoming";
				if (rank === -1) cls = "dead";
				else if (n < rank) cls = "done";
				else if (n === rank) cls = "current";
				return `
					<div class="form-hero-step ${cls}">
						<div class="bar"></div>
						<div class="lbl">${cls === "done" ? "✓ " : ""}${frappe.utils.escape_html(label)}</div>
					</div>`;
			})
			.join("");

		this.$wrapper.html(`
			<div class="form-hero-top">
				<div class="form-hero-avatar">${frappe.utils.escape_html(initial)}</div>
				<div class="form-hero-id">
					<div class="form-hero-title">${frappe.utils.escape_html(title)}</div>
					<div class="form-hero-sub">${frappe.utils.escape_html(sub)}</div>
				</div>
				${value_html}
			</div>
			<div class="form-hero-pipeline">${seg}</div>
		`);
	}
};
