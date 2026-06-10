// //// NEOFFICE PATCH — Form hero (NeoCockpit content redesign, stage 2).
//
// A soft-gradient card on top of every form: avatar/initial + big title +
// "Doctype NAME · contact" subtitle, the document's KEY VALUE on the right
// (serif, e.g. grand_total for sales docs — nothing when the doctype has
// none), and a numbered business stepper (Draft → Sent → Accepted → Order)
// driven by a small per-doctype registry with a docstatus fallback, so
// EVERY doctype works. Step dates are recovered for free from the Version
// records already shipped in docinfo (no extra request).
//
// Design source: ~/Downloads/design_handoff_devis_c (dirC-hero + step
// markers), re-skinned to the Neoffice brand. Only renders under the
// cockpit chrome.

frappe.provide("frappe.ui.form");

// ── per-doctype enrichment ──────────────────────────────────────────
// steps(doc, tx): ordered business steps; each = {label, when} where
//   `when` is the datetime the step was reached (null = unknown).
//   `tx` resolves transition timestamps from the doc's Version trail.
// rank(doc): 1-based index of the CURRENT step; > steps.length = all
//   done; -1 = dead (lost/cancelled/expired)
// value_field: the key figure shown on the right (auto-detected when absent)
//
// NB: Quotation "Accepted"/"Invoiced" statuses are Neoffice additions
// (Property Setter on the status DocField), not stock v15.
const HERO_REGISTRY = {
	Quotation: {
		steps: (doc, tx) => [
			{ label: __("Draft"), when: doc.creation },
			{ label: __("Sent"), when: tx.submit() || tx.status("Open") },
			{ label: __("Accepted"), when: tx.status("Accepted") },
			{ label: __("Order"), when: tx.status("Ordered", "Partially Ordered", "Invoiced") },
		],
		rank(doc) {
			if (doc.docstatus === 2 || ["Lost", "Expired", "Cancelled"].includes(doc.status))
				return -1;
			if (["Ordered", "Partially Ordered", "Invoiced"].includes(doc.status)) return 5;
			if (doc.status === "Accepted") return 3;
			if (doc.docstatus === 1) return 2;
			return 1;
		},
	},
	"Sales Order": {
		steps: (doc, tx) => [
			{ label: __("Draft"), when: doc.creation },
			{ label: __("Confirmed"), when: tx.submit() },
			{ label: __("Delivered"), when: tx.status("To Bill", "Completed") },
			{ label: __("Billed"), when: tx.status("Completed") },
		],
		rank(doc) {
			if (doc.docstatus === 2 || ["Cancelled", "Closed"].includes(doc.status)) return -1;
			if (doc.docstatus === 0) return 1;
			if (flt(doc.per_billed) >= 100) return 5;
			if (flt(doc.per_delivered) >= 100) return 3;
			return 2;
		},
	},
	"Sales Invoice": {
		steps: (doc, tx) => [
			{ label: __("Draft"), when: doc.creation },
			{ label: __("Submitted"), when: tx.submit() },
			{ label: __("Paid"), when: tx.status("Paid") },
		],
		rank(doc) {
			if (doc.docstatus === 2 || doc.status === "Cancelled") return -1;
			if (doc.docstatus === 0) return 1;
			if (doc.status === "Paid") return 4;
			return 2;
		},
	},
	"Purchase Invoice": {
		steps: (doc, tx) => [
			{ label: __("Draft"), when: doc.creation },
			{ label: __("Submitted"), when: tx.submit() },
			{ label: __("Paid"), when: tx.status("Paid") },
		],
		rank(doc) {
			if (doc.docstatus === 2 || doc.status === "Cancelled") return -1;
			if (doc.docstatus === 0) return 1;
			if (doc.status === "Paid") return 4;
			return 2;
		},
	},
	"Delivery Note": {
		steps: (doc, tx) => [
			{ label: __("Draft"), when: doc.creation },
			{ label: __("Submitted"), when: tx.submit() },
			{ label: __("Billed"), when: tx.status("Completed") },
		],
		rank(doc) {
			if (doc.docstatus === 2) return -1;
			if (doc.docstatus === 0) return 1;
			if (flt(doc.per_billed) >= 100) return 4;
			return 2;
		},
	},
};

const DEFAULT_PIPELINE = {
	steps: (doc, tx) => [
		{ label: __("Draft"), when: doc.creation },
		{ label: __("Submitted"), when: tx.submit() },
	],
	rank(doc) {
		if (doc.docstatus === 2) return -1;
		return doc.docstatus === 1 ? 3 : 1;
	},
};

// Resolve "when did this doc reach status X / get submitted" from the
// Version trail the form already loaded in docinfo. Versions come newest
// first, so the first hit per status is the most recent transition.
function extract_transitions(frm) {
	const docinfo = frappe.model.get_docinfo(frm.doctype, frm.docname) || {};
	const status_dates = {};
	let submit_date = null;
	(docinfo.versions || []).forEach((v) => {
		let data;
		try {
			data = JSON.parse(v.data);
		} catch (e) {
			return;
		}
		((data && data.changed) || []).forEach((change) => {
			const [field, , val] = change;
			if (field === "status" && val && !status_dates[val]) {
				status_dates[val] = v.creation;
			}
			if (field === "docstatus" && cint(val) === 1 && !submit_date) {
				submit_date = v.creation;
			}
		});
	});
	return {
		status: (...names) => {
			for (const n of names) if (status_dates[n]) return status_dates[n];
			return null;
		},
		submit: () => submit_date,
	};
}

function short_date(d) {
	if (!d) return "";
	try {
		return moment(d).format("D MMM");
	} catch (e) {
		return frappe.datetime.str_to_user(String(d).split(" ")[0]);
	}
}

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
		const contact =
			doc.contact_display && doc.contact_display !== title ? doc.contact_display : null;
		const id_part =
			doc.name === title ? __(this.frm.doctype) : __(this.frm.doctype) + " " + doc.name;
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

		const top_html = `
			<div class="form-hero-top">
				<div class="form-hero-avatar">${frappe.utils.escape_html(initial)}</div>
				<div class="form-hero-id">
					<div class="form-hero-title">${frappe.utils.escape_html(title)}</div>
					<div class="form-hero-sub">${frappe.utils.escape_html(sub)}</div>
				</div>
				${value_html}
			</div>`;

		// stepper — only for submittable doctypes (masters have no lifecycle)
		const conf = HERO_REGISTRY[this.frm.doctype] || (meta.is_submittable ? DEFAULT_PIPELINE : null);
		if (!conf) {
			this.$wrapper.html(top_html);
			return;
		}

		const tx = extract_transitions(this.frm);
		const steps = conf.steps(doc, tx);
		const rank = conf.rank(doc);
		const seg = steps
			.map((step, i) => {
				const n = i + 1;
				let cls = "todo";
				if (rank === -1) cls = "dead";
				else if (n < rank) cls = "done";
				else if (n === rank) cls = "current";

				const bubble =
					cls === "done"
						? `<svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2.2"
							stroke-linecap="round" stroke-linejoin="round"><path d="M3.5 8.5l3 3 6-6.5"/></svg>`
						: `${n}`;

				let when = "";
				if (cls === "done") when = short_date(step.when);
				else if (cls === "current") when = __("In progress");
				else if (cls === "todo") when = "—";

				return `
					<div class="form-hero-step ${cls}">
						<div class="bubble">${bubble}</div>
						<div class="lbl">${frappe.utils.escape_html(step.label)}</div>
						<div class="when">${frappe.utils.escape_html(when)}</div>
					</div>`;
			})
			.join("");

		this.$wrapper.html(`${top_html}<div class="form-hero-steps">${seg}</div>`);
	}
};
