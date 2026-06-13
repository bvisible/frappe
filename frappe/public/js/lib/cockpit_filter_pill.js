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

	function enhance() {
		if (!document.body.classList.contains("nf-list")) return;
		// The visible page's side section (offsetParent guards against a stale
		// hidden page-container in the desk SPA).
		var sides = document.querySelectorAll(".layout-side-section");
		var side = null;
		for (var i = 0; i < sides.length; i++) {
			if (sides[i].offsetParent !== null) {
				side = sides[i];
				break;
			}
		}
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

	// Re-run after every page render (page-change fires once the new page is
	// shown), on list refreshes, and once at boot.
	$(document).on("page-change", function () {
		setTimeout(enhance, 60);
	});
	$(document).on("list_view_loaded", function () {
		setTimeout(enhance, 60);
	});
	$(document).ready(function () {
		setTimeout(enhance, 250);
	});
})();
