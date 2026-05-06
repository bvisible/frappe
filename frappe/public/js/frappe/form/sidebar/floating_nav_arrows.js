////////////////////////////////////////////////////////////////////////
//// Neoffice ▼▼▼ floating-nav-arrows (NEW FILE — entire module)
////
//// Patch: Floating Navigation Arrows (Neoffice fork on bvisible/frappe v15)
//// Status: companion patch to form-header-compact, ships together
//// Spec : Obsidian → Neoffice/Form-Header-Compact/00-README.md
////
//// Moves the prev-doc / next-doc chevrons out of the cramped
//// .page-actions and into two floating buttons pinned to the left
//// and right edges of the form, vertically centred — like a record
//// carousel. Frees up horizontal space for custom buttons (Purchase
//// Invoice / Journal Entry / etc.) on the right.
////
//// Active only when the compact form header mode is on
//// (frappe.compact_form_header). Defers to frm.navigate_records()
//// which is already exposed by Frappe core, so no business logic
//// is duplicated. The native prev-doc / next-doc icons in
//// .standard-actions are hidden by SCSS in the same mode.
////
//// Maintenance contract: this module only READS frm + DOM and calls
//// the existing frm.navigate_records() API. If upstream renames
//// `navigate_records`, that's the only line to adapt.
////////////////////////////////////////////////////////////////////////

frappe.ui.form.FloatingNavArrows = class FloatingNavArrows {
	constructor(opts) {
		Object.assign(this, opts);
		this.make();
	}

	make() {
		// The form body is rendered into .layout-main-section-wrapper.
		// We anchor the arrows there so they track vertical scrolling
		// of the field area rather than the entire viewport.
		const $anchor =
			(this.frm.$wrapper &&
				this.frm.$wrapper.find(".layout-main-section-wrapper").first()) ||
			$();
		if (!$anchor.length) return;

		// Idempotent: drop any previous arrows from a re-render
		this.frm.$wrapper.find(".form-floating-nav").remove();

		// Hide the floating buttons on new / single docs — there's
		// nothing to navigate to.
		if (this.frm.is_new() || this.frm.meta.issingle) return;

		const me = this;
		this.$prev = $(`
			<button class="form-floating-nav form-floating-nav-prev"
				type="button" title="${__("Previous Document")}"
				aria-label="${__("Previous Document")}">
				<svg viewBox="0 0 24 24" width="18" height="18"
					fill="none" stroke="currentColor" stroke-width="2"
					stroke-linecap="round" stroke-linejoin="round">
					<path d="m15 18-6-6 6-6"/>
				</svg>
			</button>
		`);
		this.$next = $(`
			<button class="form-floating-nav form-floating-nav-next"
				type="button" title="${__("Next Document")}"
				aria-label="${__("Next Document")}">
				<svg viewBox="0 0 24 24" width="18" height="18"
					fill="none" stroke="currentColor" stroke-width="2"
					stroke-linecap="round" stroke-linejoin="round">
					<path d="m9 18 6-6-6-6"/>
				</svg>
			</button>
		`);

		// frm.navigate_records(1) → previous, (0) → next
		this.$prev.on("click", () => me.frm.navigate_records(1));
		this.$next.on("click", () => me.frm.navigate_records(0));

		$anchor.append(this.$prev).append(this.$next);
	}

	refresh() {
		// Hide arrows on transient states without destroying the DOM.
		if (!this.$prev || !this.$next) return;
		const should_show = !this.frm.is_new() && !this.frm.meta.issingle;
		this.$prev.toggle(should_show);
		this.$next.toggle(should_show);
	}
};

//// Neoffice ▲▲▲ floating-nav-arrows (END OF NEOFFICE MODULE)
////////////////////////////////////////////////////////////////////////
