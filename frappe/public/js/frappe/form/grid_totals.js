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

	applies() {
		const meta = this.frm.meta;
		if (!meta || meta.istable || meta.issingle) return false;
		const has = (f) => meta.fields.some((df) => df.fieldname === f);
		return Boolean(
			has("items") && has("net_total") && has("grand_total") && this.frm.fields_dict.items
		);
	}

	refresh() {
		if (!document.body.classList.contains("neoffice-cockpit")) return;
		if (!this.applies()) return;
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
		// last child of the grid card so it hugs the table (rounded corners
		// come from the card's overflow:hidden). The band lives inside the
		// observed subtree — the _last_html guard in render() keeps the
		// observer->render cycle from feeding itself.
		this.$band = $('<div class="grid-totals-band"></div>').appendTo($grid);
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
