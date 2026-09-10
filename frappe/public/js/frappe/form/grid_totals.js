//// Neoffice — added file, no upstream equivalent (v15 or develop). Net Total / Taxes / Grand Total
//// band under the items grid of the NeoCockpit form chrome: 64f4614afc (2026-06-10 "feat(cockpit):
//// grid redesign — airy rows, icon footer buttons, in-table Add Row CTA, totals band"), then the same
//// day e901cfb70a (first paint: observe the items control wrapper, skip an identical render so the
//// observer cannot feed itself), ad5920a2c4 (band hugging the grid card) and c663378f5f (band moved
//// to a sibling right after .form-grid + bounded 300 ms retries, because Purchase Invoice and other
//// slow-meta doctypes ran the first toolbar refresh before fields_dict.items existed and never got a
//// band at all). Imported by form/form.js, instantiated by form/toolbar.js refresh_hero(); styles in
//// public/css/cockpit.css. The NEOFFICE PATCH note below is the original one.
// //// NEOFFICE PATCH — items grid totals band (NeoCockpit content redesign).
//
// Transactional documents (Quotation, Sales Order, Sales Invoice, …) get a
// soft band right under their items table: Net Total · Taxes · Grand Total
// (serif, like the hero key value). Generic: renders for any doctype whose
// meta carries an `items` table plus net_total/grand_total fields — nothing
// doctype-specific hardcoded.
//
// Liveness: the first toolbar refresh can run BEFORE the field layout is
// in the DOM, so a MutationObserver on the items control wrapper (re)tries
// the band and re-reads frm.doc whenever the grid re-renders (which is how
// recalculated totals surface). The band hugs the grid card (last child of
// .form-grid) — inside the observed subtree, so render() skips identical
// html to keep the observer from feeding itself.

frappe.provide("frappe.ui.form");

//// Neoffice — extension point, same shape as form_hero's hero_step_actions: an app
//// registers what IT knows and frappe keeps knowing nothing about it. The gross
//// margin needs the buying price of each item, which lives in neoffice_theme (the
//// mirror of the reference buying price list); frappe must not reach into an app to
//// find it, and the band must not hardcode a business figure.
////
//// A provider is called on every render with the form and returns either null or
////   { pairs: [{label, value, serif?, muted?}], note?: "...", action?: {label, run} }
//// `action` renders one small button at the left of the band — where a per-line
//// breakdown belongs. It is a button and not a grid column on purpose: a column
//// would have to fight the grid's own width arithmetic, and a real field would
//// follow the line into the print format, where the customer would read our
//// buying price.
//// `pairs` join the band; `note` is a second, quieter line under it — that is where
//// the COVERAGE goes ("margin known on 4 of 9 lines"), because a percentage without
//// its coverage is a wrong number when half the catalogue has no buying price.
//// Providers must be synchronous: fetch in the background, render what you have.
frappe.ui.form.grid_total_providers = frappe.ui.form.grid_total_providers || [];
frappe.ui.form.add_grid_total_provider = function (provider) {
	frappe.ui.form.grid_total_providers.push(provider);
};

frappe.ui.form.GridTotals = class GridTotals {
	constructor(frm) {
		this.frm = frm;
	}

	meta_applies() {
		const meta = this.frm.meta;
		if (!meta || meta.istable || meta.issingle) return false;
		const has = (f) => meta.fields.some((df) => df.fieldname === f);
		return has("items") && has("net_total") && has("grand_total");
	}

	refresh() {
		if (!document.body.classList.contains("neoffice-cockpit")) return;
		if (!this.meta_applies()) return;
		if (!this.frm.fields_dict.items) {
			// the first toolbar refresh can beat the field layout — retry a
			// few times, the observer takes over once the control exists
			this._retries = (this._retries || 0) + 1;
			if (this._retries <= 10) setTimeout(() => this.refresh(), 300);
			return;
		}
		this._retries = 0;
		this.observe();
		this.ensure_band();
		this.render();
	}

	observe() {
		const wrapper_el = this.frm.fields_dict.items.$wrapper[0];
		if (this._observed_el === wrapper_el) return;
		this._observer && this._observer.disconnect();
		this._observed_el = wrapper_el;
		this._observer = new MutationObserver(
			frappe.utils.debounce(() => {
				this.ensure_band();
				this.render();
			}, 200)
		);
		this._observer.observe(wrapper_el, {
			childList: true,
			subtree: true,
			characterData: true,
		});
	}

	ensure_band() {
		const $grid = this.frm.fields_dict.items.$wrapper.find(".form-grid").first();
		if (!$grid.length) return; // layout not built yet
		if (this.$band && $.contains(document.body, this.$band[0])) return;
		// sibling right AFTER the grid card: outside the table but glued to
		// its bottom edge (the CSS squares the card's bottom corners via
		// :has). Still inside the observed subtree — the _last_html guard
		// in render() keeps the observer->render cycle from feeding itself.
		this.$band = $('<div class="grid-totals-band"></div>').insertAfter($grid);
		this._last_html = null; // fresh band must always take the next render
	}

	render() {
		if (!this.$band) return;
		const doc = this.frm.doc;
		if (doc.__islocal && !(doc.items || []).length) {
			this.$band.addClass("hide");
			return;
		}
		const currency = doc.currency || frappe.boot.sysdefaults.currency || "";
		const fmt = (v) => format_number(flt(v), null, 2);
		const taxes = flt(doc.total_taxes_and_charges);
		//// Neoffice — show the figure that will actually be settled, exactly as the
		//// hero does. With CHF rounding to 0.05, grand_total and rounded_total differ,
		//// and the band printed 2'709.09 three centimetres under a hero reading
		//// 2'709.10 — two totals for one document on one screen. rounded_total is what
		//// the ledger, the outstanding amount and the payment file all use; it is 0
		//// when rounding is disabled, in which case grand_total IS the settled amount.
		const settled =
			!cint(doc.disable_rounded_total) && flt(doc.rounded_total)
				? flt(doc.rounded_total)
				: flt(doc.grand_total);
		const html = `
			<span class="tot-pair">
				<span class="tot-label">${__("Net Total")}</span>
				<span class="tot-value">${fmt(doc.net_total)}</span>
			</span>
			${
				taxes
					? `<span class="tot-pair">
						<span class="tot-label">${__("Taxes")}</span>
						<span class="tot-value">${fmt(taxes)}</span>
					</span>`
					: ""
			}
			<span class="tot-pair tot-grand">
				<span class="tot-label">${__("Total")} ${frappe.utils.escape_html(currency)}</span>
				<span class="tot-value tot-serif">${fmt(settled)}</span>
			</span>
		`;
		//// Neoffice — registered providers (gross margin, today) add their pairs after
		//// the grand total and may add one quieter note line under the band. A provider
		//// that throws is skipped: a broken add-on must never take the totals down.
		let extra_pairs = "";
		let notes = [];
		this._actions = [];
		(frappe.ui.form.grid_total_providers || []).forEach((provider) => {
			let extra;
			try {
				extra = provider(this.frm);
			} catch (e) {
				extra = null;
			}
			if (!extra) return;
			(extra.pairs || []).forEach((pair) => {
				extra_pairs += `
					<span class="tot-pair${pair.muted ? " tot-muted" : ""}">
						<span class="tot-label">${frappe.utils.escape_html(pair.label || "")}</span>
						<span class="tot-value${pair.serif ? " tot-serif" : ""}">${frappe.utils.escape_html(
							pair.value == null ? "" : String(pair.value)
						)}</span>
					</span>`;
			});
			if (extra.note) notes.push(extra.note);
			if (extra.action && typeof extra.action.run === "function") {
				this._actions.push(extra.action);
			}
		});
		const actions_html = this._actions.length
			? `<div class="grid-totals-actions">${this._actions
					.map(
						(a, i) =>
							`<button class="grid-totals-action" data-action-idx="${i}">${frappe.utils.escape_html(
								a.label || ""
							)}</button>`
					)
					.join("")}</div>`
			: "";
		const note_html = notes.length
			? `<div class="grid-totals-note">${notes
					.map((n) => frappe.utils.escape_html(n))
					.join(" · ")}</div>`
			: "";
		const pairs_html = html + extra_pairs;
		const full =
			note_html || actions_html
				? `<div class="grid-totals-row">${actions_html}<div class="grid-totals-pairs">${pairs_html}</div></div>${note_html}`
				: pairs_html;
		if (this._last_html === full) return;
		this._last_html = full;
		this.$band.toggleClass("has-note", Boolean(note_html || actions_html));
		this.$band.removeClass("hide").html(full);
		this.$band.find(".grid-totals-action").on("click", (e) => {
			e.preventDefault();
			const action = this._actions[cint($(e.currentTarget).attr("data-action-idx"))];
			action && action.run(this.frm);
		});
	}
};
