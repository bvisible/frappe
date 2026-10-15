//// Neoffice — added file, no upstream equivalent (v15 or develop). One bar gliding along the form
//// tabs baseline instead of a static per-tab underline: 885fefba8d (2026-06-10 "feat(cockpit):
//// sliding tab indicator — one bar gliding along the tabs baseline") and, the same day, 80865fd081 -
//// frappe switches tabs without firing bootstrap's shown.bs.tab, so a MutationObserver on the .active
//// class drives the bar instead of the event. Imported by form/form.js, instantiated by
//// form/toolbar.js refresh_hero(); styles in public/css/cockpit.css. The NEOFFICE PATCH note below is
//// the original one.
//// 2026-10-15 — a strip longer than its box (the company settings have 19 tabs) was a trap: the bar
//// lived OUTSIDE the scrolling strip and was placed with offsetLeft, which ignores the strip's scroll,
//// so once the strip had moved the bar sat under another tab (or under none) while the browser's own
//// scrollbar, arrows included, said nothing about where the reader was. The bar now lives INSIDE the
//// strip, so it scrolls with the tabs and is clipped with them; the native scrollbar is hidden and
//// replaced by two chevrons and a fade at the ends that still hold tabs; the active tab is brought
//// into the middle of the strip whenever it changes.
// //// NEOFFICE PATCH — sliding tab indicator (NeoCockpit content redesign).
//
// One absolutely-positioned bar (.nc-tab-indicator) glides along the
// form-tabs baseline to the active tab — instead of a static per-tab
// underline. Repositioned on tab switch (the .active class, watched with a
// MutationObserver), form refresh and a resize of the strip. Cockpit chrome
// only.

frappe.provide("frappe.ui.form");

frappe.ui.form.TabSlider = class TabSlider {
	constructor(frm) {
		this.frm = frm;
	}

	refresh() {
		if (!document.body.classList.contains("neoffice-cockpit")) return;
		const $list = this.frm.$wrapper.find(".form-tabs-list").first();
		const $tabs = $list.find(".form-tabs").first();
		if (!$list.length || !$tabs.find(".nav-link").length) return;

		if (!this.$bar || !$.contains($tabs[0], this.$bar[0])) {
			this.teardown();
			this.$list = $list;
			this.$tabs = $tabs;
			// Inside the strip, not beside it: the bar scrolls with the tabs and is clipped with them.
			this.$bar = $('<span class="nc-tab-indicator"></span>').appendTo($tabs);
			this.make_nav();
			// frappe switches tabs itself (no bootstrap shown.bs.tab event):
			// watch the .active class instead, whatever flips it
			this._observer = new MutationObserver(() => this.schedule());
			this._observer.observe($tabs[0], {
				subtree: true,
				attributes: true,
				// The class only: the bar's own style changes must not call position() again.
				attributeFilter: ["class"],
			});
			// The width of the strip changes with the window, the sidebar and the form width: what
			// is hidden at its ends changes with it.
			if (window.ResizeObserver) {
				this._resize = new ResizeObserver(() => this.schedule());
				this._resize.observe($tabs[0]);
			}
			$tabs.on("scroll.nc-tab-slider", () => this.update_nav());
		}
		this._active = null;
		// let the layout settle (fonts, tab visibility) before measuring
		requestAnimationFrame(() => this.position());
	}

	teardown() {
		this._observer && this._observer.disconnect();
		this._resize && this._resize.disconnect();
		this.$tabs && this.$tabs.off(".nc-tab-slider");
		this.$nav && this.$nav.remove();
		this._raf && cancelAnimationFrame(this._raf);
		this._raf = null;
		this._observer = this._resize = this.$nav = this.$prev = this.$next = null;
	}

	schedule() {
		if (this._raf) return;
		this._raf = requestAnimationFrame(() => {
			this._raf = null;
			this.position();
		});
	}

	// The two chevrons over the ends of the strip. Shown only on the side that still holds tabs.
	make_nav() {
		const chevron = (d) =>
			`<svg viewBox="0 0 16 16" width="14" height="14" aria-hidden="true"><path d="${d}" fill="none" ` +
			'stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>';
		const button = (cls, label, d, direction) => {
			const text = frappe.utils.escape_html(label);
			return $(
				`<button type="button" class="nc-tabs-nav ${cls}" tabindex="-1" ` +
					`title="${text}" aria-label="${text}">${chevron(d)}</button>`
			).on("click", () => {
				const strip = this.$tabs[0];
				strip.scrollBy({ left: direction * strip.clientWidth * 0.7, behavior: "smooth" });
			});
		};
		this.$prev = button("nc-tabs-prev", __("Previous"), "M10 3 5 8l5 5", -1);
		this.$next = button("nc-tabs-next", __("Next"), "M6 3l5 5-5 5", 1);
		this.$nav = this.$prev.add(this.$next);
		this.$nav.appendTo(this.$list);
	}

	update_nav() {
		if (!this.$tabs || !this.$tabs[0].isConnected) return;
		const strip = this.$tabs[0];
		const more = strip.scrollWidth - strip.clientWidth;
		this.$list
			.toggleClass("nc-tabs-more-left", strip.scrollLeft > 1)
			.toggleClass("nc-tabs-more-right", more > 1 && strip.scrollLeft < more - 1);
		// Over the ends of the strip, whatever sits beside it (the Simple / Advanced switch).
		this.$prev.css("left", strip.offsetLeft + 2 + "px");
		this.$next.css(
			"left",
			strip.offsetLeft + strip.clientWidth - this.$next.outerWidth() - 2 + "px"
		);
	}

	position() {
		if (!this.$bar || !this.$tabs || !this.$tabs[0].isConnected) return;
		const link = this.$tabs.find(".nav-link.active")[0];
		if (!link || !link.offsetWidth) {
			this.$bar.css("opacity", 0);
			this.update_nav();
			return;
		}
		this.$bar.css({
			left: link.offsetLeft + "px",
			width: link.offsetWidth + "px",
			opacity: 1,
		});
		// A tab chosen, or brought forward by a link (?focus=…), is shown: in the middle of the strip
		// when it was out of it or under a chevron. A strip the reader moved by hand is left alone.
		if (link !== this._active) {
			this._active = link;
			const strip = this.$tabs[0];
			const margin = 40;
			const from = strip.scrollLeft;
			const seen =
				link.offsetLeft - margin >= from &&
				link.offsetLeft + link.offsetWidth + margin <= from + strip.clientWidth;
			if (!seen && strip.scrollWidth > strip.clientWidth) {
				strip.scrollTo({
					left: Math.max(
						0,
						link.offsetLeft - (strip.clientWidth - link.offsetWidth) / 2
					),
					behavior: "smooth",
				});
			}
		}
		this.update_nav();
	}
};
