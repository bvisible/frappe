////////////////////////////////////////////////////////////////////////
//// Neoffice ▼▼▼ floating-nav-arrows (NEW FILE — entire module)
////
//// Patch: Floating Navigation Arrows (Neoffice fork on bvisible/frappe v15)
//// Status: companion patch to form-header-compact, ships together
//// Spec : Obsidian → Neoffice/Form-Header-Compact/00-README.md
////
//// Moves the prev-doc / next-doc chevrons out of the cramped
//// .page-actions and into two floating buttons.
////
//// Two placement modes:
//// - HERO mode (Neoffice cockpit, .form-hero rendered): bare, very
////   low-key chevrons flanking the hero card at title level, centred
////   in the gutters between the nav rail / viewport edge and the
////   card. position:absolute inside .layout-main-section-wrapper so
////   they scroll away with the head. Falls back inside the card edge
////   when a gutter is too narrow.
//// - LEGACY mode (no hero): viewport-centred fixed carousel arrows,
////   the original behaviour.
////
//// Defers to frm.navigate_records() which is already exposed by
//// Frappe core, so no business logic is duplicated. The native
//// prev-doc / next-doc icons in .standard-actions are hidden by SCSS
//// in the same mode.
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

		this.bind_position_handlers();
		this.position_arrows();
	}

	bind_position_handlers() {
		// Debounced repositioning on viewport resize and on rail
		// collapse/expand (the rail toggle does not emit a custom event,
		// so a 250 ms cushion catches its CSS transition end).
		let timer = null;
		this._position_handler = () => {
			clearTimeout(timer);
			timer = setTimeout(() => this.position_arrows(), 60);
		};
		window.addEventListener("resize", this._position_handler);
		// The module rail toggle button lives in the navbar; intercept
		// any click in the navbar area as a cheap heuristic to re-run
		// positioning shortly after, when the rail width has settled.
		$(document).on("click.compact-nav", ".navbar, .toggle-sidebar", () => {
			setTimeout(() => this.position_arrows(), 350);
		});
	}

	position_arrows() {
		if (!this.$prev || !this.$next || !this.$prev.is(":visible")) return;
		const $hero = this.frm.$wrapper.find(".form-hero").first();
		if ($hero.length && $hero.is(":visible")) {
			this.position_hero_mode($hero);
		} else {
			this.position_legacy_mode();
		}
	}

	// HERO mode — bare chevrons flanking the hero card at title level.
	// Coordinates are relative to .layout-main-section-wrapper, which
	// the cockpit SCSS turns into the positioning context.
	position_hero_mode($hero) {
		const $wrap = this.frm.$wrapper.find(".layout-main-section-wrapper").first();
		if (!$wrap.length) return;
		const wr = $wrap[0].getBoundingClientRect();
		const hr = $hero[0].getBoundingClientRect();
		// Vertical anchor: the identity row (avatar + title), not the
		// whole card — with a pipeline the card is much taller.
		const $top = $hero.find(".form-hero-top").first();
		const tr = ($top.length ? $top[0] : $hero[0]).getBoundingClientRect();
		const mid = Math.round(tr.top + tr.height / 2 - wr.top);

		const OUT = 18; // how far the 22px button sticks out from the card edge
		// Anchor each chevron just OUTSIDE the card edge (fixed offset, so
		// the distance to the card never depends on viewport width). When
		// the gutter (rail→card on the left, card→viewport on the right)
		// can't fit it without being clipped/covered, tuck it inside.
		const $rail = $(".neocockpit aside").first();
		const rail_right = $rail.length ? $rail[0].getBoundingClientRect().right : 0;
		const lgut = hr.left - Math.max(rail_right, 0);
		const rgut = window.innerWidth - hr.right;
		const left = lgut >= OUT ? -OUT : 6;
		const right = rgut >= OUT ? -OUT : 6;

		this.$prev.add(this.$next).addClass("form-floating-nav--hero");
		this.$prev.css({ top: mid + "px", left: left + "px", right: "" });
		this.$next.css({ top: mid + "px", right: right + "px", left: "" });

		// The hero re-renders (and can change height) on every frm
		// refresh; track it once so the arrows follow.
		if (window.ResizeObserver && !this._hero_ro) {
			this._hero_ro = new ResizeObserver(() => this.position_arrows());
			this._hero_ro.observe($hero[0]);
		}
	}

	// LEGACY mode — original viewport-centred fixed carousel. The prev
	// arrow hugs the page-body's left edge so it never hides behind an
	// expanded module rail.
	position_legacy_mode() {
		this.$prev.add(this.$next).removeClass("form-floating-nav--hero");
		this.$next.css({ top: "", right: "", left: "" });
		this.$prev.css({ top: "", right: "" });
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
		if (!should_show) return;
		this.position_arrows();
		// The hero of this refresh cycle may not be rendered yet when
		// the sidebar refreshes — re-measure right after paint.
		requestAnimationFrame(() => this.position_arrows());
	}
};

//// Neoffice ▲▲▲ floating-nav-arrows (END OF NEOFFICE MODULE)
////////////////////////////////////////////////////////////////////////
