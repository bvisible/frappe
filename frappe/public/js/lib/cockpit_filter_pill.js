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
		side.classList.toggle("nf-pill-active", n > 0);
	}

	function buildHead() {
		var head = document.createElement("div");
		head.className = "nf-pill-head";
		// Inline SVGs (no sprite dependency): funnel + chevron.
		head.innerHTML =
			'<svg class="nf-pill-funnel" viewBox="0 0 24 24" aria-hidden="true">' +
			'<path d="M3 5h18l-7 8.5V20l-4-2v-4.5z"/></svg>' +
			'<span class="nf-pill-label">' +
			__("Filters") +
			"</span>" +
			'<span class="nf-pill-count" style="display:none">0</span>' +
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

	function enhance() {
		// Gate on the route, not the body .nf-list class: desk.js sets that class
		// on the same page-change event we listen to, so it can lag our handler.
		if (!isListRoute()) return;
		var side = visibleSide();
		if (!side) return;
		var sidebar = side.querySelector(".list-sidebar");
		if (!sidebar) return;

		if (sidebar.classList.contains(ENHANCED)) {
			refreshCount(side);
			return;
		}
		sidebar.classList.add(ENHANCED);

		var head = buildHead();
		sidebar.insertBefore(head, sidebar.firstChild);
		head.addEventListener("click", function () {
			setCollapsed(side, !side.classList.contains("nf-pill-collapsed"));
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

	// page-change fires once the new page is shown (covers SPA navigation);
	// ready covers the initial hard load.
	$(document).on("page-change", scheduleEnhance);
	$(document).ready(scheduleEnhance);
})();
