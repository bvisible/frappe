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
// action(rank, frm): optional tiny CTA under the CURRENT step — the one
//   move that completes this stage (Validate / Send / Create next doc).
//   Return null when not applicable (permissions checked here).
//
// NB: Quotation "Accepted"/"Invoiced" statuses are Neoffice additions
// (Property Setter on the status DocField), not stock v15.

// shared step-action builders (each may return null — filtered out).
// Tiny lucide-style icons inlined: the CTAs are icon + short label pills.
const STEP_ICONS = {
	check: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M20 6 9 17l-5-5"/></svg>',
	send: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M14.5 9.5 21 3m0 0h-6m6 0v6M21 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h6"/></svg>',
	mail: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="20" height="16" x="2" y="4" rx="2"/><path d="m22 7-8.97 5.7a1.94 1.94 0 0 1-2.06 0L2 7"/></svg>',
	print: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M6 18H4a2 2 0 0 1-2-2v-5a2 2 0 0 1 2-2h16a2 2 0 0 1 2 2v5a2 2 0 0 1-2 2h-2"/><path d="M6 9V3a1 1 0 0 1 1-1h10a1 1 0 0 1 1 1v6"/><rect x="6" y="14" width="12" height="8" rx="1"/></svg>',
	plus: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 12h14"/><path d="M12 5v14"/></svg>',
	edit: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21.174 6.812a1 1 0 0 0-3.986-3.987L3.842 16.174a2 2 0 0 0-.5.83l-1.321 4.352a.5.5 0 0 0 .623.622l4.353-1.32a2 2 0 0 0 .83-.497z"/></svg>',
	clock: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/><path d="M12 7v5l4 2"/></svg>',
	stamp: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M5 22h14"/><path d="M19.27 13.73A2.5 2.5 0 0 0 17.5 13h-11A2.5 2.5 0 0 0 4 15.5V17a1 1 0 0 0 1 1h14a1 1 0 0 0 1-1v-1.5c0-.66-.26-1.3-.73-1.77"/><path d="M14 13V8.5C14 7 15 7 15 5a3 3 0 0 0-3-3 3 3 0 0 0-3 3c0 2 1 2 1 3.5V13"/></svg>',
};

// Generic step-anchored actions. Apps register a provider per doctype that
// returns { label, icon|icon_svg, step, run } (or null) to surface a CTA
// under a specific pipeline step — and it stays there even once that step is
// done (e.g. WebStamp under "Validated" on a paid invoice). Keeps the hero
// decoupled from app-specific features: the swisspost_barcode app owns all
// the WebStamp logic and just registers here.
//   frappe.ui.form.add_hero_step_action("Sales Invoice", (frm) => ({...}))
// `step` is the 1-based pipeline index to anchor under (default: current).
frappe.ui.form.hero_step_actions = frappe.ui.form.hero_step_actions || {};
frappe.ui.form.add_hero_step_action = function (doctype, provider) {
	(frappe.ui.form.hero_step_actions[doctype] =
		frappe.ui.form.hero_step_actions[doctype] || []).push(provider);
};
function submit_action(frm) {
	if (frm.doc.docstatus !== 0) return null;
	if (!frm.perm || !frm.perm[0] || !frm.perm[0].submit) return null;
	return { label: __("Validate"), icon: "check", run: () => frm.savesubmit() };
}
function send_action(frm) {
	if (frm.doc.docstatus !== 1) return null;
	return { label: __("Send"), icon: "mail", run: () => frm.email_doc() };
}
function print_action(frm) {
	if (frm.doc.docstatus !== 1) return null;
	return { label: __("Print"), icon: "print", run: () => frm.print_doc() };
}
function create_menu_action(frm) {
	if (frm.doc.docstatus !== 1) return null;
	return {
		label: __("Create"),
		icon: "plus",
		menu: true,
		run: (anchor_el) => show_create_menu(frm, anchor_el),
	};
}
function show_create_menu(frm, anchor_el) {
	$(".step-cta-menu").remove();
	const $group = frm.page.inner_toolbar.find(
		`.inner-group-button[data-label="${encodeURIComponent(__("Create"))}"]`
	);
	const $items = $group.find(".dropdown-item");
	if (!$items.length) {
		frappe.show_alert({ message: __("No create action available"), indicator: "orange" });
		return;
	}
	const $menu = $('<div class="step-cta-menu"></div>');
	$items.each(function () {
		const $it = $(this);
		const label = $it.text().trim();
		if (!label) return;
		$('<button class="item"></button>')
			.text(label)
			.on("click", (e) => {
				e.preventDefault();
				e.stopPropagation();
				$menu.remove();
				$it.trigger("click");
			})
			.appendTo($menu);
	});
	const rect = anchor_el.getBoundingClientRect();
	$menu.css({ position: "fixed", top: rect.bottom + 6, left: rect.left + rect.width / 2 });
	$("body").append($menu);
	setTimeout(() => {
		$(document).one("mousedown.stepCtaMenu", (e) => {
			if (!$menu[0].contains(e.target)) $menu.remove();
		});
	}, 0);
}
function make_action(frm, label, method, target_doctype) {
	if (frm.doc.docstatus !== 1) return null;
	if (!frappe.model.can_create(target_doctype)) return null;
	return { label, icon: "plus", run: () => frappe.model.open_mapped_doc({ method, frm }) };
}

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
		actions(rank, frm) {
			if (rank === 1) return [submit_action(frm)];
			if (rank === 2) return [send_action(frm), print_action(frm), create_menu_action(frm)];
			if (rank === 3)
				return [
					make_action(
						frm,
						__("Create order"),
						"erpnext.selling.doctype.quotation.quotation.make_sales_order",
						"Sales Order"
					),
					print_action(frm),
				];
			return [];
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
		actions(rank, frm) {
			if (rank === 1) return [submit_action(frm)];
			if (rank === 2)
				return [
					make_action(
						frm,
						__("Create delivery"),
						"erpnext.selling.doctype.sales_order.sales_order.make_delivery_note",
						"Delivery Note"
					),
					print_action(frm),
					create_menu_action(frm),
				];
			if (rank === 3)
				return [
					make_action(
						frm,
						__("Create invoice"),
						"erpnext.selling.doctype.sales_order.sales_order.make_sales_invoice",
						"Sales Invoice"
					),
					print_action(frm),
					create_menu_action(frm),
				];
			return [];
		},
	},
	"Sales Invoice": {
		steps: (doc, tx) => [
			{ label: __("Draft"), when: doc.creation },
			{ label: __("Submitted"), when: tx.submit() },
			{ label: __("Paid"), when: tx.status("Paid") || (doc.status === "Paid" ? doc.modified : null) },
		],
		rank(doc) {
			if (doc.docstatus === 2 || doc.status === "Cancelled") return -1;
			if (doc.docstatus === 0) return 1;
			if (doc.status === "Paid") return 4;
			return 2;
		},
		actions(rank, frm) {
			if (rank === 1) return [submit_action(frm)];
			if (rank === 2) return [send_action(frm), print_action(frm), create_menu_action(frm)];
			return [];
		},
	},
	"Purchase Invoice": {
		steps: (doc, tx) => [
			{ label: __("Draft"), when: doc.creation },
			{ label: __("Submitted"), when: tx.submit() },
			{ label: __("Paid"), when: tx.status("Paid") || (doc.status === "Paid" ? doc.modified : null) },
		],
		rank(doc) {
			if (doc.docstatus === 2 || doc.status === "Cancelled") return -1;
			if (doc.docstatus === 0) return 1;
			if (doc.status === "Paid") return 4;
			return 2;
		},
		actions(rank, frm) {
			if (rank === 1) return [submit_action(frm)];
			if (rank === 2) return [print_action(frm), create_menu_action(frm)];
			return [];
		},
	},
	"Payment Entry": {
		value_field: "paid_amount",
		value_label: "Paid Amount",
		steps: (doc, tx) => [
			{ label: __("Draft"), when: doc.creation },
			{ label: __("Submitted"), when: tx.submit() },
		],
		rank(doc) {
			if (doc.docstatus === 2) return -1;
			return doc.docstatus === 1 ? 3 : 1;
		},
		actions(rank, frm) {
			if (rank === 1) return [submit_action(frm)];
			return [];
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
		actions(rank, frm) {
			if (rank === 1) return [submit_action(frm)];
			if (rank === 2)
				return [
					make_action(
						frm,
						__("Create invoice"),
						"erpnext.stock.doctype.delivery_note.delivery_note.make_sales_invoice",
						"Sales Invoice"
					),
					print_action(frm),
					create_menu_action(frm),
				];
			return [];
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
	actions(rank, frm) {
		return rank === 1 ? [submit_action(frm)] : [];
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
		// Validation date from the version history; fall back to the document
		// date (posting/transaction) for docs without a tracked version yet —
		// seeded/imported records have no versions, so the step would read "—".
		submit: () =>
			submit_date ||
			(cint(frm.doc.docstatus) >= 1
				? frm.doc.posting_date || frm.doc.transaction_date || null
				: null),
	};
}

function short_date(d) {
	if (!d) return "";
	try {
		// Intl, not moment: frappe pins moment to an "en" locale internally,
		// so moment-formatted month names ignore the user's language.
		return new Intl.DateTimeFormat(frappe.boot.lang || "en", {
			day: "numeric",
			month: "short",
		}).format(frappe.datetime.str_to_obj(String(d)));
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
		const conf_entry = HERO_REGISTRY[this.frm.doctype] || {};
		const vf =
			conf_entry.value_field ||
			(meta.fields.some((f) => f.fieldname === "grand_total") ? "grand_total" : null);
		if (vf && doc[vf] != null) {
			const amount = format_number(flt(doc[vf]), null, 2);
			const currency =
				doc.currency ||
				doc.paid_to_account_currency ||
				frappe.boot.sysdefaults.currency ||
				"";
			value_html = `
				<div class="form-hero-value">
					<div class="form-hero-amount">${amount}</div>
					<div class="form-hero-currency">${frappe.utils.escape_html(currency)} · ${__(conf_entry.value_label || "Grand Total")}</div>
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
			this.render_extras();
			return;
		}

		const tx = extract_transitions(this.frm);
		const steps = conf.steps(doc, tx);
		const rank = conf.rank(doc);
		// Assemble all step CTAs into one flat list, each tagged with the
		// step it anchors under (1-based). Native actions sit on the CURRENT
		// step, clamped to the last step so they still show when the pipeline
		// is complete (e.g. SI Paid: rank 4 > 3 steps). App-registered actions
		// (e.g. WebStamp) carry their own anchor and stay even on a done step.
		const native = (rank >= 1 && conf.actions ? conf.actions(rank, this.frm) : []).filter(
			Boolean
		);
		const clamp = (s) => Math.min(Math.max(s, 1), steps.length);
		const native_anchor = clamp(rank);
		const all_actions = native.map((a) => ({ ...a, _anchor: native_anchor }));
		const providers = frappe.ui.form.hero_step_actions[this.frm.doctype] || [];
		providers.forEach((p) => {
			let a;
			try {
				a = p(this.frm);
			} catch (e) {
				a = null;
			}
			if (a) all_actions.push({ ...a, _anchor: clamp(a.step || rank) });
		});

		// View/Print stays anchored to the validated step for the whole
		// validated life of the document — you often re-open a paid invoice
		// to view or reprint it. Like WebStamp, it persists once validated;
		// deduped against the current-step actions so it never doubles up.
		if (cint(this.frm.doc.docstatus) === 1) {
			const p = print_action(this.frm);
			if (p) {
				const at = clamp(2);
				if (!all_actions.some((a) => a.label === p.label && a._anchor === at)) {
					all_actions.push({ ...p, _anchor: at });
				}
			}
		}

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

				const my = all_actions.filter((a) => a._anchor === n);
				const cta = my.length
					? `<div class="step-ctas">${my
							.map((a) => {
								const gidx = all_actions.indexOf(a);
								const icon = STEP_ICONS[a.icon] || a.icon_svg || "";
								return `
							<button class="step-cta" data-idx="${gidx}" title="${frappe.utils.escape_html(a.label)}">
								${icon}<span>${frappe.utils.escape_html(a.label)}${a.menu ? " ▾" : ""}</span>
							</button>`;
							})
							.join("")}</div>`
					: "";

				return `
					<div class="form-hero-step ${cls}">
						<div class="bubble">${bubble}</div>
						<div class="lbl">${frappe.utils.escape_html(step.label)}</div>
						<div class="when">${frappe.utils.escape_html(when)}</div>
						${cta}
					</div>`;
			})
			.join("");

		this.$wrapper.html(`${top_html}<div class="form-hero-steps">${seg}</div>`);
		if (all_actions.length) {
			this.$wrapper.find(".step-cta").on("click", (e) => {
				e.preventDefault();
				e.stopPropagation();
				const idx = cint($(e.currentTarget).attr("data-idx"));
				if (all_actions[idx]) all_actions[idx].run(e.currentTarget);
			});
		}
		this.render_extras();
	}

	render_extras() {
		if (this.frm.doctype === "Item") this.render_item_extras();
		else if (this.frm.doctype === "Customer") this.render_customer_extras();
	}

	// Customer: party dashboard stats on the right (annual billing, unpaid
	// total, loyalty points — all already loaded by erpnext in
	// __onload.dashboard_info) + an email row with the two key actions:
	// History (important, accented) and Change Emails. The native head
	// buttons are hidden since the hero carries them now.
	render_customer_extras() {
		const doc = this.frm.doc;
		const onload = doc.__onload || {};
		let info = onload.dashboard_info || [];
		if (!Array.isArray(info)) info = [info];
		// one entry per company — prefer the session default company
		const default_company = frappe.defaults.get_default("company");
		const stats =
			info.find((d) => d && d.company === default_company) || info[0] || null;

		if (stats) {
			const currency = stats.currency || frappe.boot.sysdefaults.currency || "";
			const unpaid = flt(stats.total_unpaid);
			const loyalty =
				stats.loyalty_points != null ? cint(stats.loyalty_points) : null;
			const stats_html = `
				<div class="form-hero-value form-hero-prices form-hero-custstats">
					<div class="price-line">
						<span class="pl">${__("Annual billing")}</span>
						<span class="pv">${format_currency(flt(stats.billing_this_year), currency)}</span>
					</div>
					<div class="price-line ${unpaid > 0 ? "bad" : "muted"}">
						<span class="pl">${__("Unpaid")}</span>
						<span class="pv">${format_currency(unpaid, currency)}</span>
					</div>
					${
						loyalty != null
							? `<div class="price-line muted">
								<span class="pl">${__("Loyalty points")}</span>
								<span class="pv">${loyalty}</span>
							</div>`
							: ""
					}
				</div>`;
			this.$wrapper.find(".form-hero-top").append(stats_html);
		}

		const email = (doc.email_id || "").trim();
		const $row = $('<div class="form-hero-stockrow form-hero-custrow"></div>').appendTo(
			this.$wrapper
		);
		$row.html(`
			<span class="stock-chip email-chip ${email ? "" : "empty"}">
				${
					email
						? frappe.utils.escape_html(email)
						: __("No email")
				}
			</span>
			<button class="step-cta hero-change-emails">${STEP_ICONS.edit || ""}<span>${__(
				"Change Emails"
			)}</span></button>
			<button class="step-cta accent hero-history">${STEP_ICONS.clock || ""}<span>${__(
				"History"
			)}</span></button>
		`);
		$row.find(".hero-change-emails").on("click", (e) => {
			e.preventDefault();
			this.trigger_custom_button(__("Change Emails"));
		});
		$row.find(".hero-history").on("click", (e) => {
			e.preventDefault();
			frappe.set_route("history", "Customer", doc.name);
		});

		// hide the head buttons the hero now carries. The theme re-adds
		// them on its own refresh/onload_post_render — possibly AFTER this
		// render — so watch the actions area and re-hide on every addition
		// (childList only: .hide() mutates attributes, no feedback loop).
		const hide_native = () => {
			["Change Emails", "History"].forEach((label) => {
				const $btn = this.frm.custom_buttons && this.frm.custom_buttons[__(label)];
				if ($btn) $btn.hide();
			});
		};
		hide_native();
		if (!this._native_btn_mo && window.MutationObserver) {
			const actions = this.frm.page.wrapper.find(".page-actions").get(0);
			if (actions) {
				this._native_btn_mo = new MutationObserver(hide_native);
				this._native_btn_mo.observe(actions, { childList: true, subtree: true });
			}
		}
	}

	// trigger a frm custom button by (translated) label, even when hidden
	trigger_custom_button(label) {
		const $btn = this.frm.custom_buttons && this.frm.custom_buttons[label];
		if ($btn) {
			$btn.trigger("click");
		} else {
			frappe.show_alert({ message: __("Action not available yet"), indicator: "orange" });
		}
	}

	// Item: selling/buying prices on the right (Neoffice stores them on the
	// item itself) + per-warehouse stock chips, with tiny CTAs that trigger
	// the theme's native buttons (.add_price / .add_stock) — which we hide
	// from the page head since the hero carries them now.
	render_item_extras() {
		const doc = this.frm.doc;
		const currency = frappe.boot.sysdefaults.currency || "";
		const sell = flt(doc.view_item_price) || flt(doc.standard_rate);
		const buy = flt(doc.view_item_buy_price) || flt(doc.last_purchase_rate);

		const price_html = `
			<div class="form-hero-value form-hero-prices">
				<div class="price-line">
					<span class="pl">${__("Selling price")}</span>
					<span class="pv">${format_currency(sell, currency)}</span>
				</div>
				<div class="price-line muted">
					<span class="pl">${__("Buying price")}</span>
					<span class="pv">${format_currency(buy, currency)}</span>
				</div>
				<button class="step-cta hero-edit-prices">${STEP_ICONS.edit || ""}<span>${__("Add/Change Price")}</span></button>
			</div>`;
		this.$wrapper.find(".form-hero-top").append(price_html);
		this.$wrapper.find(".hero-edit-prices").on("click", (e) => {
			e.preventDefault();
			this.trigger_native_button(".add_change_price");
		});

		// hide the head buttons the hero now carries (retry: business scripts
		// may add them after this render)
		const hide_native = () => {
			this.frm.page.wrapper.find(".add_change_price").hide();
			if (cint(doc.is_stock_item)) this.frm.page.wrapper.find(".add_stock").hide();
		};
		hide_native();
		setTimeout(hide_native, 600);

		if (!cint(doc.is_stock_item)) return;
		const $stock = $('<div class="form-hero-stockrow"></div>').appendTo(this.$wrapper);
		frappe
			.call({
				method: "frappe.client.get_list",
				args: {
					doctype: "Bin",
					filters: { item_code: doc.name },
					fields: ["warehouse", "actual_qty"],
					limit_page_length: 20,
				},
			})
			.then((r) => {
				const bins = (r.message || []).filter((b) => flt(b.actual_qty) !== 0);
				const uom = doc.stock_uom ? " " + __(doc.stock_uom) : "";
				let chips = bins
					.map(
						(b) => `
						<span class="stock-chip" title="${frappe.utils.escape_html(b.warehouse)}">
							<span class="w">${frappe.utils.escape_html(b.warehouse)}</span>
							<span class="q">${flt(b.actual_qty)}${frappe.utils.escape_html(uom)}</span>
						</span>`
					)
					.join("");
				if (!bins.length) {
					chips = `<span class="stock-chip empty">${__("No stock")}</span>`;
				}
				$stock.html(`
					<span class="stock-label">${__("Stock")}</span>
					${chips}
					<button class="step-cta hero-add-stock">${STEP_ICONS.plus || ""}<span>${__("Add stock")}</span></button>
				`);
				$stock.find(".hero-add-stock").on("click", (e) => {
					e.preventDefault();
					this.trigger_native_button(".add_stock");
				});
			});
	}

	// click a (possibly hidden / overflowed) native button by selector —
	// lazy DOM read: theme scripts register them on their own refresh
	trigger_native_button(selector) {
		const $btn = this.frm.page.wrapper.find(selector).first();
		if ($btn.length) {
			$btn.trigger("click");
		} else {
			frappe.show_alert({ message: __("Action not available yet"), indicator: "orange" });
		}
	}
};
