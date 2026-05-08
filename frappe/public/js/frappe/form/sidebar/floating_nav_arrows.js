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

		// The previous arrow used to be a fixed `left: 56px` which sits
		// fine when the module rail is collapsed (≈ 50 px) but ends up
		// hidden behind the rail when it's expanded (≈ 140-200 px).
		// Recompute the left edge dynamically based on the form body
		// container so the arrow is always flush against the form.
		this.bind_position_handlers();
		this.position_prev_arrow();
	}

	bind_position_handlers() {
		// Debounced repositioning on viewport resize and on rail
		// collapse/expand (the rail toggle does not emit a custom event,
		// so a 250 ms cushion catches its CSS transition end).
		let timer = null;
		this._position_handler = () => {
			clearTimeout(timer);
			timer = setTimeout(() => this.position_prev_arrow(), 60);
		};
		window.addEventListener("resize", this._position_handler);
		// The module rail toggle button lives in the navbar; intercept
		// any click in the navbar area as a cheap heuristic to re-run
		// positioning shortly after, when the rail width has settled.
		$(document).on("click.compact-nav", ".navbar, .toggle-sidebar", () => {
			setTimeout(() => this.position_prev_arrow(), 350);
		});
	}

	position_prev_arrow() {
		if (!this.$prev || !this.$prev.is(":visible")) return;
		// Anchor the prev arrow to the page-body's left edge with a
		// small 8 px gap. Falls back gracefully if the wrapper is not
		// in the DOM yet (e.g. during the first refresh).
		const $page_body = $(".container.page-body, .page-body").first();
		if (!$page_body.length) return;
		const left = Math.max(8, $page_body[0].getBoundingClientRect().left + 8);
		this.$prev.css("left", left + "px");
	}

	refresh() {
		// Hide arrows on transient states without destroying the DOM.
		if (!this.$prev || !this.$next) return;
		const should_show = !this.frm.is_new() && !this.frm.meta.issingle;
		this.$prev.toggle(should_show);
		this.$next.toggle(should_show);
		if (should_show) this.position_prev_arrow();
	}
};

//// Neoffice ▲▲▲ floating-nav-arrows (END OF NEOFFICE MODULE)
////////////////////////////////////////////////////////////////////////
