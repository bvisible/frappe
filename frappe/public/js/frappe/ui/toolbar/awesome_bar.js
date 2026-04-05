// Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt
frappe.provide("frappe.search");
frappe.provide("frappe.tags");

// ════════════════════════════════════════════════════════════
// Search Analytics — localStorage-based user search profiling
// ════════════════════════════════════════════════════════════
frappe.search.Analytics = class Analytics {
	constructor() {
		this._key = `search_profile_${frappe.session.user}`;
		this._data = null;
	}

	_load() {
		if (this._data) return this._data;
		try {
			this._data = JSON.parse(localStorage.getItem(this._key)) || this._empty();
		} catch (e) {
			this._data = this._empty();
		}
		return this._data;
	}

	_empty() {
		return { recent_searches: [], recent_clicks: [], doctype_freq: {} };
	}

	_save() {
		try {
			localStorage.setItem(this._key, JSON.stringify(this._data));
		} catch (e) {
			// Storage full — clear old data
			this._data.recent_searches = this._data.recent_searches.slice(0, 10);
			this._data.recent_clicks = this._data.recent_clicks.slice(0, 20);
			try {
				localStorage.setItem(this._key, JSON.stringify(this._data));
			} catch (e2) {
				// pass
			}
		}
	}

	track_search(term) {
		const d = this._load();
		// Deduplicate — remove old occurrence of same term
		d.recent_searches = d.recent_searches.filter((s) => s.term !== term);
		d.recent_searches.unshift({ term, ts: Date.now() });
		d.recent_searches = d.recent_searches.slice(0, 30);
		this._save();
	}

	track_click(doctype, name, label) {
		const d = this._load();
		// Track click for frequency
		d.doctype_freq[doctype] = (d.doctype_freq[doctype] || 0) + 1;
		// Track recent click
		d.recent_clicks = d.recent_clicks.filter(
			(c) => !(c.doctype === doctype && c.name === name)
		);
		d.recent_clicks.unshift({ doctype, name, label, ts: Date.now() });
		d.recent_clicks = d.recent_clicks.slice(0, 50);
		this._save();
	}

	get_recent_searches() {
		return this._load().recent_searches.slice(0, 8);
	}

	get_recent_clicks() {
		return this._load().recent_clicks.slice(0, 8);
	}

	get_doctype_boost() {
		return this._load().doctype_freq;
	}
};

// ════════════════════════════════════════════════════════════
// AwesomeBar — Main search bar class
// ════════════════════════════════════════════════════════════
frappe.search.AwesomeBar = class AwesomeBar {
	setup(element) {
		$(".search-bar").removeClass("hidden");
		this.$input = $(element);
		this.input = this.$input.get(0);
		this.options = [];
		this.global_results = [];
		this._search_id = 0;
		this._search_pending = false;
		this._selected = -1;
		this._all_items = [];
		this._current_txt = "";
		this.analytics = new frappe.search.Analytics();

		this._create_panel();
		this._bind_events();
		frappe.search.utils.setup_recent();
	}

	// ── Panel creation ─────────────────────────────────────
	_create_panel() {
		this.$panel = $(`<div class="search-mega-panel"></div>`);
		// Append to body for absolute positioning freedom
		$("body").append(this.$panel);
	}

	_position_panel() {
		const rect = this.$input.get(0).getBoundingClientRect();
		const panel_width = 680;
		// Center panel under the search bar, but don't go off-screen
		let left = rect.left + rect.width / 2 - panel_width / 2;
		left = Math.max(12, Math.min(left, window.innerWidth - panel_width - 12));

		this.$panel.css({
			top: rect.bottom + 8,
			left: left,
			width: panel_width,
		});
	}

	// ── Event binding ──────────────────────────────────────
	_bind_events() {
		const me = this;

		this.$input.on(
			"input",
			frappe.utils.debounce(function (e) {
				me._handle_input(e.target.value);
			}, 150)
		);

		this.$input.on("focus", () => {
			const val = this.$input.val().trim();
			if (!val) {
				// Show recent searches/clicks on empty focus
				this._show_home();
			} else if (val.length > 1) {
				this._position_panel();
				this.$panel.addClass("active");
			}
		});

		this.$input.on("keydown", (e) => this._handle_keydown(e));

		// Close on outside click
		$(document).on("mousedown", (e) => {
			if (
				!$(e.target).closest(".search-bar").length &&
				!$(e.target).closest(".search-mega-panel").length
			) {
				this._close();
			}
		});
	}

	// ── Main input handler ─────────────────────────────────
	_handle_input(raw) {
		const txt = raw.trim().replace(/\s\s+/g, " ");
		this._current_txt = txt;

		if (!txt || txt.length < 2) {
			if (!txt) this._show_home();
			else this.$panel.removeClass("active"); // Hide panel but don't clear input
			return;
		}

		// Track search term
		this.analytics.track_search(txt);

		// Pre-load help context (async, cached per doctype)
		this._load_help_context();

		// Build synchronous local options
		this.options = [];
		this._add_defaults(txt);
		this.options = this.options.concat(this._build_nav_options(txt));

		// Clear previous global results
		this.global_results = [];

		// Render local options immediately
		this._render(txt);

		// Fire async global search (skipped for math expressions)
		this._search_global(txt);
	}

	// ── Show home state (recent searches + recently visited) ──
	_show_home() {
		this._position_panel();
		this.$panel.empty();
		this._all_items = [];
		this._selected = -1;

		const $body = $(`<div class="search-panel-body">
			<div class="search-panel-main"></div>
			<div class="search-panel-sidebar"></div>
		</div>`);

		const $main = $body.find(".search-panel-main");
		const $sidebar = $body.find(".search-panel-sidebar");

		// ── LEFT: Recent searches (max 4) ──
		const recent_searches = this.analytics.get_recent_searches().slice(0, 4);
		if (recent_searches.length) {
			const $section = $(`<div class="search-section">
				<div class="search-section-header">${__("Recent Searches")}</div>
			</div>`);
			recent_searches.forEach((s) => {
				const $item = $(
					`<div class="search-item" role="option">
						<div class="search-item-icon">
							<svg class="icon icon-sm"><use href="#icon-search"></use></svg>
						</div>
						<div class="search-item-content">
							<div class="search-item-label">${frappe.utils.xss_sanitise(s.term)}</div>
						</div>
					</div>`
				);
				const term = s.term;
				$item.on("click", () => {
					this.$input.val(term);
					this._handle_input(term);
				});
				$item.attr("data-idx", this._all_items.length);
				$section.append($item);
				this._all_items.push({ $el: $item, data: { onclick: () => {
					this.$input.val(term);
					this._handle_input(term);
				}}});
			});
			$main.append($section);
		}

		// ── LEFT: Recently visited (from frappe.route_history, live) ──
		const recent_pages = this._get_route_history().slice(0, 6);
		if (recent_pages.length) {
			const $section = $(`<div class="search-section">
				<div class="search-section-header">${__("Recently Visited")}</div>
			</div>`);
			recent_pages.forEach((r) => {
				const $item = $(
					`<div class="search-item" role="option">
						<div class="search-item-icon">
							<svg class="icon icon-sm"><use href="#icon-${r.icon}"></use></svg>
						</div>
						<div class="search-item-content">
							<div class="search-item-label">${frappe.utils.xss_sanitise(r.label)}</div>
							<div class="search-item-desc">${__(r.type)}</div>
						</div>
					</div>`
				);
				const route = r.route;
				$item.on("click", () => {
					frappe.set_route(...route);
					this._close();
				});
				$item.attr("data-idx", this._all_items.length);
				$section.append($item);
				this._all_items.push({ $el: $item, data: {
					onclick: () => { frappe.set_route(...route); this._close(); }
				}});
			});
			$main.append($section);
		}

		// ── RIGHT: Help context first, tips only as fallback ──
		this._render_help_into_sidebar($sidebar);
		this._load_help_context();

		// Tips only if no Learn/Doc available
		const has_help = this._help_cache &&
			((this._help_cache.learns && this._help_cache.learns.length) ||
			 (this._help_cache.resources && this._help_cache.resources.length));
		if (!has_help) {
			$sidebar.append(
				`<div class="search-section-header">${__("Tips")}</div>
				<div class="search-sidebar-tip">${__("Type a name, ID, or keyword to search")}</div>
				<div class="search-sidebar-tip">${__("Math expressions:")} <code>55+434/4</code></div>
				<div class="search-sidebar-tip">${__("Type")} <code>random</code> ${__("for a password")}</div>`
			);
		}

		this.$panel.append($body);
		this.$panel.addClass("active");
	}

	// ── Get live route history ──────────────────────────────
	_get_route_history() {
		const seen = new Set();
		const results = [];
		const history = (frappe.route_history || []).slice().reverse();

		for (const route of history) {
			if (!route || route.length < 2) continue;
			const [type, ...rest] = route;
			let label, icon, rtype;

			if (type === "Form" && rest.length >= 2) {
				const key = `Form:${rest[0]}:${rest[1]}`;
				if (seen.has(key)) continue;
				seen.add(key);
				label = `${rest[1]}`;
				icon = "file";
				rtype = __(rest[0]);
			} else if (type === "List" && rest.length >= 1) {
				const qualifier = rest[1] || "List";
				const key = `List:${rest[0]}:${qualifier}`;
				if (seen.has(key)) continue;
				seen.add(key);
				label = `${__(rest[0])} ${qualifier !== "List" ? qualifier : ""}`.trim();
				icon = "list";
				rtype = __(qualifier === "Report" ? "Report" : "List");
			} else if (type === "Workspaces" && rest.length >= 1) {
				const key = `WS:${rest[0]}`;
				if (seen.has(key)) continue;
				seen.add(key);
				label = rest[0];
				icon = "grid";
				rtype = __("Workspace");
			} else {
				continue;
			}

			results.push({ label, icon, type: rtype, route });
			if (results.length >= 8) break;
		}
		return results;
	}

	// ── Load help context (Nora Learn + Nora Help Resource) ──
	// Cached per doctype to survive re-renders
	_load_help_context() {
		const route = frappe.get_route();
		if (!route || route.length < 2) return;

		const doctype =
			route[0] === "Form" || route[0] === "List" ? route[1] : null;
		if (!doctype) return;

		// Skip if already cached for this doctype
		if (this._help_cache && this._help_cache._doctype === doctype) return;

		frappe.xcall("neoffice_theme.api.get_help_context", { doctype })
			.then((data) => {
				if (!data) return;
				this._help_cache = { _doctype: doctype, ...data };
				// Re-render if panel is still active to show the help
				if (this.$panel.hasClass("active")) {
					if (this._current_txt) {
						this._render(this._current_txt);
					} else {
						this._show_home();
					}
				}
			})
			.catch(() => {
				// neoffice_theme not installed — silent fail
			});
	}

	// ── Render cached help into sidebar ──────────────────────
	_render_help_into_sidebar($sidebar) {
		if (!this._help_cache) return;
		const data = this._help_cache;

		if (data.learns && data.learns.length) {
			$sidebar.append(
				`<div class="search-section-header sidebar-section-spacer">${__("Learn")}</div>`
			);
			data.learns.forEach((l) => {
				const duration = l.estimated_duration
					? `<span class="sidebar-item-type">${l.estimated_duration} min</span>`
					: "";
				const $item = $(
					`<div class="search-sidebar-item">
						<a href="/app/${l.entry_route || "nora-learn/" + l.name}">
							<span class="sidebar-item-label">${frappe.utils.xss_sanitise(l.title)}</span>
							${duration}
						</a>
					</div>`
				);
				$item.find("a").on("click", (e) => {
					e.preventDefault();
					frappe.set_route(l.entry_route || "nora-learn/" + l.name);
					this._close();
				});
				$sidebar.append($item);
			});
		}

		if (data.resources && data.resources.length) {
			$sidebar.append(
				`<div class="search-section-header sidebar-section-spacer">${__("Documentation")}</div>`
			);
			data.resources.forEach((r) => {
				const $item = $(
					`<div class="search-sidebar-item">
						<a href="${frappe.utils.xss_sanitise(r.url || "#")}" target="_blank">
							<span class="sidebar-item-label">${frappe.utils.xss_sanitise(r.title)}</span>
							<span class="sidebar-item-type">${__(r.resource_type || "Doc")}</span>
						</a>
					</div>`
				);
				$sidebar.append($item);
			});
		}
	}

	// ── Local defaults (calculator, doc link, custom) ──────
	_add_defaults(txt) {
		this._make_calculator(txt);
		this._make_random(txt);
		this._make_doctype_action(txt);
		// Skip document link if we already matched a DocType name
		// (avoids "devis" matching all DEVIS-2025-* documents)
		const has_dt_action = this.options.some((o) => o.default === "DocTypeAction");
		if (!has_dt_action) {
			this._make_document_link(txt);
		}
		this._make_search_in_current(txt);

		// Custom search providers
		for (const provider of frappe.search.AwesomeBar.custom_providers) {
			try {
				const results = provider(txt);
				if (Array.isArray(results)) {
					this.options = this.options.concat(results);
				}
			} catch (e) {
				console.warn("Search provider error:", e);
			}
		}
	}

	// ── Navigation options ─────────────────────────────────
	_build_nav_options(txt) {
		if (txt.charAt(0) === "#") {
			return frappe.tags.utils.get_tags(txt);
		}
		let options = frappe.search.utils
			.get_creatables(txt)
			.concat(
				frappe.search.utils.get_search_in_list(txt),
				frappe.search.utils.get_doctypes(txt),
				frappe.search.utils.get_reports(txt),
				frappe.search.utils.get_pages(txt),
				frappe.search.utils.get_workspaces(txt),
				frappe.search.utils.get_dashboards(txt),
				frappe.search.utils.get_recent_pages(txt || ""),
				frappe.search.utils.get_executables(txt),
				frappe.search.utils.get_marketplace_apps(txt)
			);
		return this._deduplicate(options).sort((a, b) => b.index - a.index);
	}

	// ── Check if text is a math expression ─────────────────
	_is_math_expression(txt) {
		const first = txt.charAt(0);
		return first === "(" || first === "=" || /^\d/.test(first);
	}

	// ── Async global search ────────────────────────────────
	_search_global(txt) {
		if (txt.charAt(0) === "#") return;

		// Skip global search for math expressions
		if (this._is_math_expression(txt)) {
			this._search_pending = false;
			return;
		}

		// Sanitize boolean mode operators
		const safe = txt.replace(/[+\-<>~*"@()]/g, " ").trim();
		if (!safe || safe.length < 2) {
			this._search_pending = false;
			return;
		}

		const search_id = ++this._search_id;
		this._search_pending = true;

		frappe.xcall("frappe.utils.global_search.search", {
			text: safe,
			limit: 50,
		}).then((results) => {
			if (search_id !== this._search_id) return;
			this._search_pending = false;
			this.global_results = results || [];
			this._render(this._current_txt);
		});
	}

	// ── Main render ────────────────────────────────────────
	_render(txt) {
		this._position_panel();
		this.$panel.empty();
		this._all_items = [];
		this._selected = -1;

		const safe_txt = frappe.utils.xss_sanitise(txt);

		// Two-column layout
		const $body = $(`<div class="search-panel-body">
			<div class="search-panel-main"></div>
			<div class="search-panel-sidebar"></div>
		</div>`);
		const $main = $body.find(".search-panel-main");
		const $sidebar = $body.find(".search-panel-sidebar");

		// ── LEFT COLUMN: Results ──

		// 1. Direct document matches — ALWAYS first (highest priority, max 3)
		const doc_links = this.options.filter((o) => o.index >= 200).slice(0, 3);
		if (doc_links.length) {
			this._render_section_into($main, __("Go to Document"), doc_links, "goto");
		}

		// 2. DocType actions ("View all Items", "New Quotation")
		const dt_actions = this.options.filter((o) => o.default === "DocTypeAction");
		if (dt_actions.length) {
			this._render_section_into($main, "", dt_actions, "doctype-action");
		}

		// 3. Special results (calculator, random, search-in-current)
		const specials = this.options.filter(
			(o) => o.default === "Calculator" || o.default === "Current"
		);
		if (specials.length) {
			this._render_section_into($main, __("Quick Actions"), specials, "special");
		}

		// 3. Global search results grouped by DocType
		if (this.global_results.length) {
			// Filter: only show results where the search term actually appears
			// in the content (prevents irrelevant fulltext matches)
			const txt_lower = txt.toLowerCase();
			const txt_words = txt_lower.split(/\s+/).filter((w) => w.length > 2);
			const relevant = this.global_results.filter((r) => {
				if (!r.content) return false;
				const c = r.content.toLowerCase();
				// At least one significant word must appear in content
				return txt_words.length
					? txt_words.some((w) => c.includes(w))
					: c.includes(txt_lower);
			});

			const grouped = {};
			relevant.forEach((r) => {
				if (!grouped[r.doctype]) grouped[r.doctype] = [];
				grouped[r.doctype].push(r);
			});

			for (const [doctype, results] of Object.entries(grouped)) {
				const items = results.slice(0, 4).map((r) => ({
					label: r.name,
					description: this._format_content(r.content, txt),
					route: ["Form", r.doctype, r.name],
					image: r.image,
					doctype: r.doctype,
					_title: r.title || r.name,
				}));
				this._render_section_into($main, __(doctype), items, "global");
			}
		}

		// 4. Navigation options
		const nav = this.options.filter(
			(o) =>
				o.type &&
				["List", "Report", "New", "Page", "Workspace", "Dashboard"].includes(o.type)
		);
		if (nav.length) {
			this._render_section_into($main, __("Navigate"), nav.slice(0, 6), "nav");
		}

		// 5. Custom provider results
		const custom = this.options.filter(
			(o) => o.default === "Docs" || o.default === "Custom"
		);
		if (custom.length) {
			this._render_section_into($main, __("More"), custom, "custom");
		}

		// Loading indicator
		if (this._search_pending && !this.global_results.length) {
			$main.prepend(
				`<div class="search-loading">
					<span class="text-extra-muted">${__("Searching")}...</span>
				</div>`
			);
		}

		// ── RIGHT COLUMN: Sidebar ──
		this._render_sidebar($sidebar, txt);

		this.$panel.append($body);

		// Footer
		if (txt && txt.length > 1 && !this._is_math_expression(txt)) {
			const $footer = $(`<div class="search-panel-footer">
				<a href="#" class="search-for-link">
					<span>${__("Search for {0}", [safe_txt.bold()])}</span>
					<kbd>↵</kbd>
				</a>
			</div>`);
			$footer.find("a").on("click", (e) => {
				e.preventDefault();
				frappe.searchdialog.search.init_search(txt, "global_search");
				this._close();
			});
			this.$panel.append($footer);
		}

		this.$panel.addClass("active");

		// Auto-select first item
		if (this._all_items.length) {
			this._selected = 0;
			this._update_selection();
		}
	}

	// ── Render sidebar ─────────────────────────────────────
	_render_sidebar($sidebar, txt) {
		let has_context = false;

		// Learn + Documentation (from cache)
		if (this._help_cache) {
			const hc = this._help_cache;
			if ((hc.learns && hc.learns.length) || (hc.resources && hc.resources.length)) {
				has_context = true;
			}
		}
		this._render_help_into_sidebar($sidebar);
		this._load_help_context();

		// Recently visited — from live route history
		const recent = this._get_route_history().slice(0, 5);
		if (recent.length) {
			$sidebar.append(
				`<div class="search-section-header${has_context ? " sidebar-section-spacer" : ""}">${__("Recently Visited")}</div>`
			);
			recent.forEach((r) => {
				const route = r.route;
				const $item = $(
					`<div class="search-sidebar-item">
						<a href="#">
							<span class="sidebar-item-label">${frappe.utils.xss_sanitise(r.label)}</span>
							<span class="sidebar-item-type">${r.type}</span>
						</a>
					</div>`
				);
				$item.find("a").on("click", (e) => {
					e.preventDefault();
					frappe.set_route(...route);
					this._close();
				});
				$sidebar.append($item);
			});
		}

		// Top DocTypes from analytics
		const boost = this.analytics.get_doctype_boost();
		const top_types = Object.entries(boost)
			.sort((a, b) => b[1] - a[1])
			.slice(0, 5);
		if (top_types.length) {
			$sidebar.append(
				`<div class="search-section-header sidebar-section-spacer">${__("You search often")}</div>`
			);
			top_types.forEach(([dt, count]) => {
				const $item = $(
					`<div class="search-sidebar-stat">
						<span class="stat-label">${__(dt)}</span>
						<span class="stat-count">${count}</span>
					</div>`
				);
				$item.on("click", () => {
					frappe.set_route("List", dt);
					this._close();
				});
				$sidebar.append($item);
			});
		}
	}

	// ── Render section into a container ─────────────────────
	_render_section_into($container, title, items, type) {
		if (!items.length) return;

		const $section = $(`<div class="search-section search-section-${type}"></div>`);
		if (title) {
			$section.append(
				`<div class="search-section-header">${title}</div>`
			);
		}

		items.forEach((item) => {
			const idx = this._all_items.length;
			const $item = this._render_item(item, type);
			$item.attr("data-idx", idx);
			$item.on("click", (e) => {
				e.preventDefault();
				this._select_item(item, e);
			});
			$section.append($item);
			this._all_items.push({ $el: $item, data: item });
		});

		$container.append($section);
	}

	// ── Render a single result item ────────────────────────
	_render_item(item, type) {
		let inner = "";

		// Avatar for global results
		if (item.image !== undefined && type === "global") {
			inner += `<div class="search-item-avatar">
				${frappe.get_avatar("avatar-xs", item.label || "", item.image)}
			</div>`;
		}

		// Type badge for navigation
		if (item.type && type === "nav") {
			inner += `<span class="search-item-badge">${__(item.type)}</span>`;
		}

		// Main content
		inner += `<div class="search-item-content">`;
		const label = item.label || item.value || "";
		const clean_label = typeof label === "string" ? label.replace(/<[^>]*>/g, "") : label;
		let display_label = frappe.utils.xss_sanitise(clean_label);
		// Highlight search term in label
		if (this._current_txt && type === "global") {
			const words = this._current_txt.split(/\s+/).filter((w) => w.length > 1);
			words.forEach((w) => {
				const re = new RegExp(
					`(${w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`,
					"gi"
				);
				display_label = display_label.replace(re, "<mark>$1</mark>");
			});
		}
		inner += `<div class="search-item-label">${display_label}</div>`;
		if (item.description) {
			inner += `<div class="search-item-desc">${item.description}</div>`;
		}
		inner += `</div>`;

		// DocType badge for global results
		if (item.doctype && type === "global") {
			inner += `<span class="search-item-doctype">${__(item.doctype)}</span>`;
		}

		return $(`<div class="search-item" role="option">${inner}</div>`);
	}

	// ── Format global search content for display ───────────
	_format_content(content, txt) {
		if (!content) return "";
		const parts = content.split(" ||| ");
		const display = parts
			.filter((p) => !p.startsWith("ID :"))
			.slice(0, 3)
			.map((p) => {
				if (txt) {
					const words = txt.split(/\s+/).filter((w) => w.length > 1);
					words.forEach((w) => {
						const re = new RegExp(
							`(${w.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")})`,
							"gi"
						);
						p = p.replace(re, "<mark>$1</mark>");
					});
				}
				return p;
			});
		return display.join(" · ");
	}

	// ── Keyboard navigation ────────────────────────────────
	_handle_keydown(e) {
		const key = e.key;

		if (key === "Escape") {
			this._close();
			this.$input.trigger("blur");
			return;
		}

		if (key === "ArrowDown") {
			e.preventDefault();
			this._move_selection(1);
			return;
		}

		if (key === "ArrowUp") {
			e.preventDefault();
			this._move_selection(-1);
			return;
		}

		if (key === "Enter") {
			e.preventDefault();
			if (this._selected >= 0 && this._selected < this._all_items.length) {
				const item = this._all_items[this._selected];
				this._select_item(item.data, e);
			} else {
				const txt = this.$input.val().trim();
				if (txt) {
					frappe.searchdialog.search.init_search(txt, "global_search");
					this._close();
				}
			}
			return;
		}
	}

	_move_selection(delta) {
		if (!this._all_items.length) return;
		this._selected = Math.max(
			0,
			Math.min(this._all_items.length - 1, this._selected + delta)
		);
		this._update_selection();
	}

	_update_selection() {
		this.$panel.find(".search-item").removeClass("selected");
		if (this._selected >= 0 && this._selected < this._all_items.length) {
			const $el = this._all_items[this._selected].$el;
			$el.addClass("selected");
			// Scroll into view within the main column
			const scrollParent = $el.closest(".search-panel-main").get(0);
			const item = $el.get(0);
			if (scrollParent) {
				if (item.offsetTop < scrollParent.scrollTop) {
					scrollParent.scrollTop = item.offsetTop - 8;
				} else if (
					item.offsetTop + item.offsetHeight >
					scrollParent.scrollTop + scrollParent.clientHeight
				) {
					scrollParent.scrollTop =
						item.offsetTop + item.offsetHeight - scrollParent.clientHeight + 8;
				}
			}
		}
	}

	// ── Select / navigate ──────────────────────────────────
	_select_item(item, e) {
		if (item.route_options) {
			frappe.route_options = item.route_options;
		}

		// Track click in analytics
		if (item.doctype && item.route) {
			this.analytics.track_click(
				item.doctype,
				item.route[2] || item.label,
				item._title || item.label
			);
		} else if (item.route && item.route[0] === "Form") {
			this.analytics.track_click(
				item.route[1],
				item.route[2],
				item.label
			);
		}

		if (item.onclick) {
			item.onclick(item.match);
		} else if (item.route) {
			if (e && (e.ctrlKey || e.metaKey)) {
				frappe.open_in_new_tab = true;
			}
			if (item.route[0] && item.route[0].startsWith("https://")) {
				window.open(item.route[0], "_blank");
			} else {
				frappe.set_route(item.route);
			}
		}

		this._close();
	}

	// ── Close panel ────────────────────────────────────────
	_close() {
		this.$panel.removeClass("active");
		// Wait for animation to finish before clearing
		setTimeout(() => {
			if (!this.$panel.hasClass("active")) {
				this.$panel.empty();
			}
		}, 200);
		this.$input.val("");
		this._all_items = [];
		this._selected = -1;
	}

	// ── Deduplication ──────────────────────────────────────
	_deduplicate(options) {
		var out = [],
			routes = [];
		options.forEach(function (option) {
			if (option.route) {
				if (
					option.route[0] === "List" &&
					option.route[2] !== "Report" &&
					option.route[2] !== "Inbox"
				) {
					option.route.splice(2);
				}
				var str_route =
					typeof option.route === "string"
						? option.route
						: option.route.join("/");
				if (routes.indexOf(str_route) === -1) {
					out.push(option);
					routes.push(str_route);
				} else {
					var old = routes.indexOf(str_route);
					if (out[old].index < option.index && !option.recent) {
						out[old] = option;
					}
				}
			} else {
				out.push(option);
				routes.push("");
			}
		});
		return out;
	}

	// ── Calculator ─────────────────────────────────────────
	_make_calculator(txt) {
		var first = txt.substr(0, 1);
		if (first == parseInt(first) || first === "(" || first === "=") {
			if (first === "=") {
				txt = txt.substr(1);
			}
			try {
				var val = eval(txt);
				var formatted_value = __("{0} = {1}", [
					frappe.utils.xss_sanitise(txt),
					(val + "").bold(),
				]);
				this.options.push({
					label: formatted_value,
					value: __("{0} = {1}", [frappe.utils.xss_sanitise(txt), val]),
					match: val,
					index: 80,
					default: "Calculator",
					onclick: function () {
						frappe.msgprint(formatted_value, __("Result"));
					},
				});
			} catch (e) {
				// pass
			}
		}
	}

	// ── Random password ────────────────────────────────────
	_make_random(txt) {
		if (txt.toLowerCase().includes("random")) {
			this.options.push({
				label: __("Generate Random Password"),
				value: frappe.utils.get_random(16),
				default: "Calculator",
				onclick: function () {
					frappe.msgprint(frappe.utils.get_random(16), __("Result"));
				},
			});
		}
	}

	// ── Search in current list ─────────────────────────────
	_make_search_in_current(txt) {
		var route = frappe.get_route();
		if (route[0] === "List" && txt.indexOf(" in") === -1) {
			const doctype = frappe.container.page?.list_view?.doctype;
			if (!doctype) return;
			var meta = frappe.get_meta(doctype);
			var search_field = meta.title_field || "name";
			var options = {};
			options[search_field] = ["like", "%" + txt + "%"];
			this.options.push({
				label: __("Find {0} in {1}", [
					frappe.utils.xss_sanitise(txt).bold(),
					__(route[1]).bold(),
				]),
				value: __("Find {0} in {1}", [
					frappe.utils.xss_sanitise(txt),
					__(route[1]),
				]),
				route_options: options,
				onclick: function () {
					cur_list.show();
				},
				index: 90,
				default: "Current",
				match: txt,
			});
		}
	}

	// ── DocType action (type "devis"/"article"/"item" → "View all") ──
	_make_doctype_action(txt) {
		if (!txt || txt.length < 3) return;
		const txt_lower = txt.toLowerCase();

		// Match against all readable DocTypes (English name + translated name)
		let best_match = null;
		let best_score = 0;

		for (const dt of frappe.boot.user.can_read || []) {
			if (frappe.boot.single_types && frappe.boot.single_types.includes(dt)) continue;

			const en = dt.toLowerCase();
			const tr = __(dt).toLowerCase();

			// Exact match is best
			if (en === txt_lower || tr === txt_lower) {
				best_match = dt;
				best_score = 100;
				break;
			}
			// startsWith is good
			if (en.startsWith(txt_lower) && txt_lower.length >= 3) {
				const score = txt_lower.length / en.length * 90;
				if (score > best_score) { best_match = dt; best_score = score; }
			}
			if (tr.startsWith(txt_lower) && txt_lower.length >= 3) {
				const score = txt_lower.length / tr.length * 90;
				if (score > best_score) { best_match = dt; best_score = score; }
			}
		}

		if (!best_match || best_score < 40) return;

		const doctype = best_match;
		const translated = __(doctype);
		this.options.push({
			label: __("View all {0}", [translated]),
			value: __("View all {0}", [translated]),
			description: doctype !== translated ? doctype : "",
			route: ["List", doctype],
			index: 190,
			default: "DocTypeAction",
		});

		// Also offer "New" if user can create
		if ((frappe.boot.user.can_create || []).includes(doctype)) {
			this.options.push({
				label: __("New {0}", [translated]),
				value: __("New {0}", [translated]),
				route: null,
				index: 189,
				default: "DocTypeAction",
				onclick: () => frappe.new_doc(doctype, true),
			});
		}
	}

	// ── Direct document navigation ─────────────────────────
	_make_document_link(txt) {
		// Trigger for document IDs: must start with a letter, 3+ chars, no spaces
		if (!txt || txt.length < 3 || /\s/.test(txt) || !/^[A-Za-z]/.test(txt)) {
			return;
		}

		const me = this;
		const request_id = ++frappe.search.AwesomeBar._resolve_request_id;

		frappe.xcall("frappe.desk.search.resolve_document", { name: txt }).then(
			(results) => {
				if (request_id !== frappe.search.AwesomeBar._resolve_request_id) return;
				if (!results || !results.length) return;

				results.forEach((r) => {
					const display_name =
						r.title && r.title !== r.name
							? `${r.title} (${r.name})`
							: r.name;
					me.options.push({
						label: display_name,
						value: display_name,
						description: __(r.doctype),
						route: ["Form", r.doctype, r.name],
						index: 200,
						match: r.name,
					});
				});

				me._render(me._current_txt);
			}
		);
	}

	// Legacy compat
	deduplicate(options) {
		return this._deduplicate(options);
	}
};

// Extension point for custom search providers
frappe.search.AwesomeBar.custom_providers = [];

// Internal counter for discarding stale resolve_document responses
frappe.search.AwesomeBar._resolve_request_id = 0;
