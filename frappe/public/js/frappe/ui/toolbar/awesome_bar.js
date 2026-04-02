// Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt
frappe.provide("frappe.search");
frappe.provide("frappe.tags");

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

		this._create_panel();
		this._bind_events();
		frappe.search.utils.setup_recent();
	}

	// ── Panel creation ─────────────────────────────────────
	_create_panel() {
		this.$panel = $(`<div class="search-results-panel"></div>`);
		this.$input.closest(".search-bar").css("position", "relative").append(this.$panel);
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
			if (val && val.length > 1) {
				this.$panel.show();
			}
			// Don't show anything on empty focus — wait for typing
		});

		this.$input.on("keydown", (e) => this._handle_keydown(e));

		// Close on outside click
		$(document).on("click", (e) => {
			if (!$(e.target).closest(".search-bar").length) {
				this._close();
			}
		});
	}

	// ── Main input handler ─────────────────────────────────
	_handle_input(raw) {
		const txt = raw.trim().replace(/\s\s+/g, " ");

		if (!txt || txt.length < 2) {
			this.$panel.hide();
			return;
		}

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

	// ── Show recent pages on focus ─────────────────────────
	_show_recent() {
		this.options = [];
		const recent = frappe.search.utils.get_recent_pages("") || [];
		const frequent = frappe.search.utils.get_frequent_links() || [];
		this.options = this._deduplicate(recent.concat(frequent));
		this.global_results = [];
		this._render("");
	}

	// ── Local defaults (calculator, doc link, custom) ──────
	_add_defaults(txt) {
		this._make_calculator(txt);
		this._make_random(txt);
		this._make_document_link(txt);
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

		// Skip global search for math expressions — they crash MATCH AGAINST
		if (this._is_math_expression(txt)) {
			this._search_pending = false;
			return;
		}

		// Sanitize boolean mode operators that crash fulltext search
		const safe = txt.replace(/[+\-<>~*"@()]/g, " ").trim();
		if (!safe || safe.length < 2) {
			this._search_pending = false;
			return;
		}

		const search_id = ++this._search_id;
		this._search_pending = true;

		frappe.xcall("frappe.utils.global_search.search", {
			text: safe,
			limit: 20,
		}).then((results) => {
			if (search_id !== this._search_id) return;
			this._search_pending = false;
			this.global_results = results || [];
			this._render(txt);
		});
	}

	// ── Render ─────────────────────────────────────────────
	_render(txt) {
		this.$panel.empty();
		this._all_items = [];
		this._selected = -1;

		const safe_txt = frappe.utils.xss_sanitise(txt);

		// 1. Special results (calculator, random, search-in-current)
		const specials = this.options.filter(
			(o) => o.default === "Calculator" || o.default === "Current"
		);
		if (specials.length) {
			this._render_section(__("Quick Actions"), specials, "special");
		}

		// 2. Direct document matches (from resolve_document)
		const doc_links = this.options.filter((o) => o.index >= 200);
		if (doc_links.length) {
			this._render_section(__("Go to Document"), doc_links, "goto");
		}

		// 3. Global search results grouped by DocType
		if (this.global_results.length) {
			const grouped = {};
			this.global_results.forEach((r) => {
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
				}));
				this._render_section(__(doctype), items, "global");
			}
		}

		// 4. Navigation options (limited to top 6)
		const nav = this.options.filter(
			(o) =>
				o.type &&
				["List", "Report", "New", "Page", "Workspace", "Dashboard"].includes(o.type)
		);
		if (nav.length) {
			this._render_section(__("Navigate"), nav.slice(0, 6), "nav");
		}

		// 5. Custom provider results (wiki search, etc.)
		const custom = this.options.filter(
			(o) => o.default === "Docs" || o.default === "Custom"
		);
		if (custom.length) {
			this._render_section(__("More"), custom, "custom");
		}

		// 6. Fallback: "Search for X" link (not for math expressions)
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

		// Loading indicator while global search is pending
		if (this._search_pending && !this.global_results.length) {
			const $loading = $(
				`<div class="search-loading text-muted text-center">
					<span class="text-extra-muted">${__("Searching")}...</span>
				</div>`
			);
			this.$panel.prepend($loading);
		}

		this.$panel.show();

		// Auto-select first item
		if (this._all_items.length) {
			this._selected = 0;
			this._update_selection();
		}
	}

	// ── Render a section with header + items ────────────────
	_render_section(title, items, type) {
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

		this.$panel.append($section);
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

		// Label
		const label = item.label || item.value || "";
		const clean_label = typeof label === "string" ? label.replace(/<[^>]*>/g, "") : label;
		inner += `<div class="search-item-label">${frappe.utils.xss_sanitise(clean_label)}</div>`;

		// Description
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
		// Show first 3 meaningful fields, skip the ID field
		const display = parts
			.filter((p) => !p.startsWith("ID :"))
			.slice(0, 3)
			.map((p) => {
				// Highlight search term
				if (txt) {
					const re = new RegExp(`(${frappe.utils.xss_sanitise(txt)})`, "gi");
					p = p.replace(re, "<mark>$1</mark>");
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
				// Fallback: open search dialog
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
			// Scroll into view
			const panel = this.$panel.get(0);
			const item = $el.get(0);
			if (item.offsetTop < panel.scrollTop) {
				panel.scrollTop = item.offsetTop - 8;
			} else if (item.offsetTop + item.offsetHeight > panel.scrollTop + panel.clientHeight) {
				panel.scrollTop = item.offsetTop + item.offsetHeight - panel.clientHeight + 8;
			}
		}
	}

	// ── Select / navigate ──────────────────────────────────
	_select_item(item, e) {
		if (item.route_options) {
			frappe.route_options = item.route_options;
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
		this.$panel.hide().empty();
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

	// ── Direct document navigation ─────────────────────────
	_make_document_link(txt) {
		if (!txt || txt.length < 3 || !/^[A-Za-z].*-/.test(txt)) {
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

				me._render(me.$input.val().trim());
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
