//// Neoffice — added file (no upstream equivalent in v15.120 or develop): the NeoCockpit filter
//// pill controller. Upstream lists and query-reports keep their filter sidebar as a fixed 220px
//// column of .layout-side-section; this turns it into a collapsible glass pill floating over the
//// content so the datatable takes the full width. Served raw from /assets (declared in hooks.py
//// with a ?v= cache-bust, no bench build), positioning lives in public/css/cockpit.css.
//// Written on 2026-06-13 by, in order: 08b9dfa77b (the pill itself, collapsed chip with the
//// active-filter count, state in localStorage, mobile FAB) · fe9272b98e (poll for .list-sidebar
//// instead of a fixed timeout — the single delayed enhance() raced the async sidebar render on
//// hard load and the header chip was never injected) · 0c0e42fa6d (gate on frappe.get_route(),
//// not the body .nf-list class, which desk.js sets on the very event we listen to) · 5d1e46dfe8
//// (pin the injected header with order:-1 — Frappe prepends its own controls after us and buried
//// it; one-click filter reset; active glow) · 5af084e625 / 066172fa05 / 7a68794110 (the injected
//// SVG feDisplacementMap filter behind the liquid-glass refraction, then toned down) ·
//// a55398c766 (rehomeStrays + MutationObserver: on back-navigation Frappe re-renders the filter
//// controls as a direct child of .layout-side-section, outside the pill).
//// Reads Frappe internals (.list-sidebar, .filter-x-button, the sidebar DOM shape) — a change to
//// the list sidebar markup upstream breaks it silently; it degrades to the stock sidebar.
// NeoCockpit — list/report filter sidebar → floating glass pill.
//
// The stock Frappe list sidebar (.layout-side-section > .list-sidebar) eats a
// fixed 220px column on every list/report view. This controller turns it into a
// collapsible glass pill floating in the top-right of the content area; the
// datatable reclaims the full width underneath (all positioning is in
// cockpit.css — this file only adds the header chip, the collapse toggle, the
// active-filter count and persists the open/closed state).
//
// Scope: only when <body> carries .nf-list (List / query-report / dashboard —
// set by desk.js update_route_class). Forms keep their own sidebar untouched.

(function () {
	"use strict";

	var STORAGE = "nf_filter_pill_collapsed"; // "1" collapsed, "0" expanded
	var ENHANCED = "nf-pill-enhanced"; // marker on .list-sidebar

	// Default to collapsed (small pill) so the view opens uncluttered; the
	// choice is remembered per browser afterwards.
	function isCollapsed() {
		var v = localStorage.getItem(STORAGE);
		return v === null ? true : v === "1";
	}
	function persist(val) {
		try {
			localStorage.setItem(STORAGE, val ? "1" : "0");
		} catch (e) {
			/* private mode / quota — non-fatal */
		}
	}
	function setCollapsed(side, val) {
		side.classList.toggle("nf-pill-collapsed", val);
		persist(val);
	}

	// Active filter count. The stock filter button carries a localized title
	// like "1 Filter Applied" / "1 Filtre appliqué" — parse the integer (the
	// only locale-agnostic signal). No number → 0 applied filters.
	function activeCount(side) {
		var btn = side.querySelector(".filter-button");
		if (!btn) return 0;
		var m = (btn.getAttribute("title") || "").match(/\d+/);
		return m ? parseInt(m[0], 10) : 0;
	}

	function refreshCount(side) {
		var badge = side.querySelector(".nf-pill-count");
		if (!badge) return;
		var n = activeCount(side);
		badge.textContent = n;
		badge.style.display = n > 0 ? "" : "none";
		var reset = side.querySelector(".nf-pill-reset");
		if (reset) reset.style.display = n > 0 ? "" : "none";
		// drives the glow (CSS) + the reset/badge visibility
		side.classList.toggle("nf-pill-active", n > 0);
	}

	// Clear every applied filter in one click. The stock clear-filters button
	// (.filter-x-button) is the native path; fall back to the list API.
	function clearFilters(side) {
		var x = side.querySelector(".filter-x-button");
		if (x) {
			x.click();
			return;
		}
		try {
			if (window.cur_list && cur_list.filter_area) cur_list.filter_area.clear();
		} catch (e) {
			/* no-op */
		}
	}

	function buildHead() {
		var head = document.createElement("div");
		head.className = "nf-pill-head";
		// Inline SVGs (no sprite dependency): funnel + reset (×) + chevron.
		head.innerHTML =
			'<svg class="nf-pill-funnel" viewBox="0 0 24 24" aria-hidden="true">' +
			'<path d="M3 5h18l-7 8.5V20l-4-2v-4.5z"/></svg>' +
			'<span class="nf-pill-label">' +
			__("Filters") +
			"</span>" +
			'<span class="nf-pill-count" style="display:none">0</span>' +
			'<button type="button" class="nf-pill-reset" style="display:none" title="' +
			__("Clear filters") +
			'" aria-label="' +
			__("Clear filters") +
			'"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 6l12 12M18 6L6 18"/></svg></button>' +
			'<svg class="nf-pill-chevron" viewBox="0 0 24 24" aria-hidden="true">' +
			'<path d="M6 9l6 6 6-6"/></svg>';
		return head;
	}

	// The side section of the page that is actually visible (offsetParent guards
	// against a stale hidden page-container in the desk SPA).
	function visibleSide() {
		var sides = document.querySelectorAll(".layout-side-section");
		for (var i = 0; i < sides.length; i++) {
			if (sides[i].offsetParent !== null) return sides[i];
		}
		return null;
	}

	function isListRoute() {
		var r = (frappe.get_route && frappe.get_route()) || [];
		return ["List", "query-report", "dashboard-view"].indexOf(r[0]) !== -1;
	}

	// Frappe sometimes re-renders the filter/sort controls as DIRECT children of
	// .layout-side-section (a sibling of .list-sidebar) instead of inside it —
	// notably on back-navigation — which makes them spill OUT of the glass pill.
	// Move any such stray back into the pill, just after the header.
	function rehomeStrays(side, sidebar) {
		var kids = Array.prototype.slice.call(side.children);
		for (var i = 0; i < kids.length; i++) {
			var c = kids[i];
			if (c === sidebar) continue;
			var head = sidebar.querySelector(".nf-pill-head");
			if (head && head.nextSibling) sidebar.insertBefore(c, head.nextSibling);
			else sidebar.insertBefore(c, sidebar.firstChild);
		}
	}

	function enhance() {
		// Gate on the route, not the body .nf-list class: desk.js sets that class
		// on the same page-change event we listen to, so it can lag our handler.
		if (!isListRoute()) return;
		var side = visibleSide();
		if (!side) return;
		var sidebar = side.querySelector(".list-sidebar");
		if (!sidebar) return;

		// pull any spilled controls back into the pill (runs every time, so it
		// also heals the back-navigation case where we're already enhanced)
		rehomeStrays(side, sidebar);

		if (sidebar.classList.contains(ENHANCED)) {
			refreshCount(side);
			return;
		}
		sidebar.classList.add(ENHANCED);
		// keep healing if Frappe re-adds controls outside the pill later
		if (window.MutationObserver) {
			new MutationObserver(function () {
				var sb = side.querySelector(".list-sidebar");
				if (sb) rehomeStrays(side, sb);
			}).observe(side, { childList: true });
		}

		var head = buildHead();
		sidebar.insertBefore(head, sidebar.firstChild);
		head.addEventListener("click", function () {
			setCollapsed(side, !side.classList.contains("nf-pill-collapsed"));
		});
		// reset (×) clears filters without toggling the pill open/closed
		var reset = head.querySelector(".nf-pill-reset");
		reset.addEventListener("click", function (e) {
			e.stopPropagation();
			clearFilters(side);
		});

		setCollapsed(side, isCollapsed());
		refreshCount(side);

		// Keep the badge live when the user adds/clears filters (the filter
		// button's title attribute changes). Observe the top controls row.
		var top = side.querySelector(".filter-section");
		if (top && window.MutationObserver) {
			var mo = new MutationObserver(function () {
				refreshCount(side);
			});
			mo.observe(top, { attributes: true, childList: true, subtree: true });
		}
	}

	// The list sidebar renders asynchronously after the page is shown, so a
	// single delayed call races the render. Poll for the sidebar (up to ~5s)
	// each time we land on a list view, then enhance once it exists.
	function scheduleEnhance() {
		var tries = 0;
		(function attempt() {
			var route = (frappe.get_route && frappe.get_route()) || [];
			if (["List", "query-report", "dashboard-view"].indexOf(route[0]) !== -1) {
				var side = visibleSide();
				if (side && side.querySelector(".list-sidebar")) {
					enhance();
					return;
				}
			} else if (route.length) {
				return; // route resolved to a non-list page → nothing to do
			}
			// route not a list yet, or sidebar not rendered → keep polling (~5s)
			if (tries++ < 25) setTimeout(attempt, 200);
		})();
	}

	// Inject the SVG displacement filter once. This is what gives the pill REAL
	// glass refraction (the backdrop bends/ripples through the glass) — the
	// effect a pure backdrop-blur can't produce. It's a static filter (no
	// per-frame recompute, no html2canvas, no WebGL), referenced from CSS via
	// backdrop-filter: url(#nf-glass-distort). Chrome renders the refraction;
	// browsers that ignore url() in backdrop-filter fall back to plain frost.
	function injectGlassFilter() {
		if (document.getElementById("nf-glass-svg")) return;
		var ns = "http://www.w3.org/2000/svg";
		var svg = document.createElementNS(ns, "svg");
		svg.id = "nf-glass-svg";
		svg.setAttribute("width", "0");
		svg.setAttribute("height", "0");
		svg.style.cssText = "position:absolute;width:0;height:0;pointer-events:none";
		// Apple liquid-glass distortion (lucasromerodb): very low-frequency
		// fractal noise → a smooth, large-scale coherent warp, softened by a
		// blur, then used to displace the backdrop. Applied via CSS as
		// filter:url(#nf-glass-distort) on the pill's ::before.
		svg.innerHTML =
			'<defs><filter id="nf-glass-distort" x="0%" y="0%" width="100%" height="100%" ' +
			'filterUnits="objectBoundingBox">' +
			'<feTurbulence type="fractalNoise" baseFrequency="0.001 0.005" numOctaves="1" seed="17" result="turb"/>' +
			'<feGaussianBlur in="turb" stdDeviation="3" result="softMap"/>' +
			'<feDisplacementMap in="SourceGraphic" in2="softMap" scale="140" ' +
			'xChannelSelector="R" yChannelSelector="G"/>' +
			"</filter></defs>";
		document.body.appendChild(svg);
	}

	// page-change fires once the new page is shown (covers SPA navigation);
	// ready covers the initial hard load.
	$(document).ready(injectGlassFilter);
	$(document).on("page-change", scheduleEnhance);
	$(document).ready(scheduleEnhance);
})();
