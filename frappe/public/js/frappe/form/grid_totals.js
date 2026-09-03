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
				<span class="tot-value tot-serif">${fmt(doc.grand_total)}</span>
			</span>
		`;
		if (this._last_html === html) return;
		this._last_html = html;
		this.$band.removeClass("hide").html(html);
	}
};
