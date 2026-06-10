// //// NEOFFICE PATCH — items grid totals band (NeoCockpit content redesign).
//
// Transactional documents (Quotation, Sales Order, Sales Invoice, …) get a
// soft band right under their items table: Net Total · Taxes · Grand Total
// (serif, like the hero key value). Generic: renders for any doctype whose
// meta carries an `items` table plus net_total/grand_total fields — nothing
// doctype-specific hardcoded. Kept live with the same MutationObserver
// pattern the theme uses for per-row subtotals: any DOM change in the grid
// (triggered by recalculations re-rendering rows) re-reads frm.doc.
// Cockpit chrome only.

frappe.provide("frappe.ui.form");

frappe.ui.form.GridTotals = class GridTotals {
	constructor(frm) {
		this.frm = frm;
	}

	applies() {
		const meta = this.frm.meta;
		if (!meta || meta.istable || meta.issingle) return false;
		const has = (f) => meta.fields.some((df) => df.fieldname === f);
		return (
			has("items") && has("net_total") && has("grand_total") && this.frm.fields_dict.items
		);
	}

	refresh() {
		if (!document.body.classList.contains("neoffice-cockpit")) return;
		if (!this.applies()) return;

		const $grid = this.frm.fields_dict.items.$wrapper.find(".form-grid-container").first();
		const $host = $grid.length
			? $grid
			: this.frm.fields_dict.items.$wrapper.find(".form-grid").first();
		if (!$host.length) return;

		if (!this.$band || !$.contains(this.frm.$wrapper[0], this.$band[0])) {
			this.$band = $('<div class="grid-totals-band"></div>').insertAfter($host);
			this.observe();
		}
		this.render();
	}

	observe() {
		const grid_el = this.frm.fields_dict.items.$wrapper[0];
		this._observer && this._observer.disconnect();
		this._observer = new MutationObserver(
			frappe.utils.debounce(() => this.render(), 200)
		);
		this._observer.observe(grid_el, { childList: true, subtree: true, characterData: true });
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
		this.$band.removeClass("hide").html(`
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
		`);
	}
};
