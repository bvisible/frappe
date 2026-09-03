//// Neoffice - added file, no upstream equivalent (v15 or develop). One bar gliding along the form
//// tabs baseline instead of a static per-tab underline: 885fefba8d (2026-06-10 "feat(cockpit):
//// sliding tab indicator - one bar gliding along the tabs baseline") and, the same day, 80865fd081 -
//// frappe switches tabs without firing bootstrap's shown.bs.tab, so a MutationObserver on the .active
//// class drives the bar instead of the event. Imported by form/form.js, instantiated by
//// form/toolbar.js refresh_hero(); styles in public/css/cockpit.css. The NEOFFICE PATCH note below is
//// the original one.
// //// NEOFFICE PATCH — sliding tab indicator (NeoCockpit content redesign).
//
// One absolutely-positioned bar (.nc-tab-indicator) glides along the
// form-tabs baseline to the active tab — instead of a static per-tab
// underline. Repositioned on tab switch (bootstrap shown.bs.tab), form
// refresh and window resize. Cockpit chrome only.

frappe.provide("frappe.ui.form");

frappe.ui.form.TabSlider = class TabSlider {
	constructor(frm) {
		this.frm = frm;
	}

	refresh() {
		if (!document.body.classList.contains("neoffice-cockpit")) return;
		const $list = this.frm.$wrapper.find(".form-tabs-list").first();
		if (!$list.length || !$list.find(".form-tabs .nav-link").length) return;

		if (!this.$bar || !$.contains($list[0], this.$bar[0])) {
			this.$bar = $('<span class="nc-tab-indicator"></span>').appendTo($list);
			// frappe switches tabs itself (no bootstrap shown.bs.tab event):
			// watch the .active class instead, whatever flips it
			this._observer && this._observer.disconnect();
			this._observer = new MutationObserver(() => {
				if (this._raf) return;
				this._raf = requestAnimationFrame(() => {
					this._raf = null;
					this.position();
				});
			});
			this._observer.observe($list[0], {
				subtree: true,
				attributes: true,
				attributeFilter: ["class"],
			});
			$(window).on(
				"resize",
				frappe.utils.debounce(() => this.position(), 150)
			);
		}
		// let the layout settle (fonts, tab visibility) before measuring
		requestAnimationFrame(() => this.position());
	}

	position() {
		if (!this.$bar) return;
		const link = this.frm.$wrapper.find(".form-tabs-list .nav-link.active")[0];
		if (!link || !link.offsetWidth) {
			this.$bar.css("opacity", 0);
			return;
		}
		this.$bar.css({
			left: link.offsetLeft + "px",
			width: link.offsetWidth + "px",
			opacity: 1,
		});
	}
};
