////////////////////////////////////////////////////////////////////////
//// Neoffice ▼▼▼ desk sidebar rewrite (the whole file diverges from upstream v15.89.0)
////
//// Upstream v15 frappe.ui.Sidebar is a 70-line generic list helper (wrapper / css_class,
//// add_item, remove_item, get_section; the leading frappe.provide("frappe.ui") was dropped too).
//// This file is the v16-style workspace sidebar: an ADAPTED BACKPORT of frappe develop
//// frappe/public/js/frappe/ui/sidebar/sidebar.js at 6bf7d45e6e (2025-09-02 "refactor: Sidebar" —
//// 25 of our 33 methods and ~50% of the lines are verbatim from it), brought in by 9999364ec6
//// (2025-10-28 "Refonte de l'interface utilisateur avec nouvelle sidebar et apps switcher", 28
//// files) together with sidebar.html, apps_switcher.js and scss/desk/sidebar.scss (all marked).
//// It renders frappe.boot.sidebar_pages as a collapsible workspace tree (sidebar.html template,
//// Sortable ordering saved through workspace_settings.set_sequence) and builds the data maps
//// workspace.js consumes (frappe.workspaces / workspace_list / workspace_map).
//// Methods with no develop counterpart: setup_route_listener, expand_parent_item, is_nested_item,
//// get_sidebar_item, close_children_item (9999364ec6); get_app_from_current_route (84b6f7a10b);
//// build_modules_section / append_module_item (aa4938f2f0). Neoffice-only layers, marked inline:
////   - set_default_app / frappe.current_app + apps switcher + neoffice_theme logo (9999364ec6),
////     app resolved from the route via boot.workspace_to_app_map (84b6f7a10b, 2025-11-09),
////     copy-before-sort (5cabef2b72) and app_data_map null guard (a03d7f00db, 2026-03-11);
////   - is_route_in_sidebar matches item-name as well as the translated title (fb2a5ef11e,
////     2025-11-08 "Support virtual apps with translated workspace names");
////   - "Navigation" / "All Modules" sections mirroring the app switcher (aa4938f2f0, 2026-04-24);
////   - headless mode for the NeoCockpit chrome — data layer without DOM (d0268ef91a, 2026-06-10,
////     the NEOFFICE PATCH notes below).
//// v16 merge note: upstream deleted ui/sidebar.js and now ships ui/sidebar/sidebar.js +
//// sidebar_item.js + sidebar_manager.js (develop tip shares only ~18% of these lines) — expect a
//// modify/delete conflict on this path; resolve from the commits above, not line by line.
////////////////////////////////////////////////////////////////////////
frappe.ui.Sidebar = class Sidebar {
	// //// NEOFFICE PATCH — headless mode for the NeoCockpit chrome.
	// WHY: workspace.js consumes this class as its DATA source (setup_pages,
	// all_pages, frappe.workspaces / workspace_map). With the cockpit chrome
	// the native sidebar DOM must never render, but the data layer must stay.
	constructor({ headless = false } = {}) {
		this.headless = headless;
		this.items = {};
		this.parent_items = [];
		this.sidebar_expanded = false;

		if (!frappe.boot.setup_complete) {
			// no sidebar if setup is not complete
			return;
		}

		this.set_all_pages();
		if (!this.headless) this.make_dom();
		this.sidebar_items = {
			public: {},
			private: {},
		};
		this.indicator_colors = [
			"green",
			"cyan",
			"blue",
			"orange",
			"yellow",
			"gray",
			"grey",
			"red",
			"pink",
			"darkgrey",
			"purple",
			"light-blue",
		];

		this.setup_pages();
		if (!this.headless) {
			this.apps_switcher.populate_apps_menu();
			this.handle_outside_click();
			this.setup_route_listener();
		}
	}

	setup_route_listener() {
		// Listen to route changes to update active sidebar item
		frappe.router.on("change", () => {
			this.set_active_workspace_item();
		});
	}

	make_dom() {
		this.set_default_app();
		this.wrapper = $(frappe.render_template("sidebar")).prependTo("body");

		this.$sidebar = this.wrapper.find(".sidebar-items");

		this.wrapper.find(".body-sidebar .collapse-sidebar-link").on("click", () => {
			this.toggle_sidebar();
		});

		this.wrapper.find(".overlay").on("click", () => {
			this.close_sidebar();
		});
		this.apps_switcher = new frappe.ui.AppsSwitcher(this);
		this.apps_switcher.create_app_data_map();
	}

	set_hover() {
		$(".standard-sidebar-item > .item-anchor").on("mouseover", function (event) {
			if ($(this).parent().hasClass("active-sidebar")) return;
			$(this).parent().addClass("hover");
		});

		$(".standard-sidebar-item > .item-anchor").on("mouseleave", function () {
			$(this).parent().removeClass("hover");
		});
	}

	set_all_pages() {
		this.sidebar_pages = frappe.boot.sidebar_pages;
		this.all_pages = this.sidebar_pages.pages;
		this.has_access = this.sidebar_pages.has_access;
		this.has_create_access = this.sidebar_pages.has_create_access;
	}

	//// Neoffice — added (84b6f7a10b, 2025-11-09 "Optimize app detection and workspace mapping"):
	//// resolves the app owning the current workspace route — fast path through
	//// boot.workspace_to_app_map, fallback through frappe.workspace_map / boot.module_app.
	get_app_from_current_route() {
		const route = frappe.get_route();
		if (!route || route.length === 0) return null;

		// Handle private workspaces
		if (route[0] === "Workspaces") {
			if (route[1] === "private") {
				return "private";
			}

			// Get workspace name from route
			const workspace_name = route[1];
			if (!workspace_name) return null;

			// OPTIMIZATION: Try fast lookup in workspace_to_app_map first
			if (frappe.boot.workspace_to_app_map?.[workspace_name]) {
				return frappe.boot.workspace_to_app_map[workspace_name];
			}

			// Fallback: Look up workspace in workspace_map
			const workspace = frappe.workspace_map?.[workspace_name];
			if (!workspace) return null;

			// Return the app from workspace or module
			if (workspace.app) return workspace.app;
			if (workspace.module) {
				return frappe.boot.module_app?.[frappe.router.slug(workspace.module)];
			}
		}

		return null;
	}

	//// Neoffice — set_default_app: develop picks the app with the most workspaces; ours prefers the
	//// app of the current route (84b6f7a10b), sorts a COPY of boot.app_data so the App Customization
	//// sort_order is not mutated and guards an empty app_data (5cabef2b72, 2026-03-11), and puts the
	//// neoffice_theme logo in the navbar (9999364ec6) — a core → neoffice_theme asset dependency.
	set_default_app() {
		// Check if we're on a workspace route - use that app
		const route_app = this.get_app_from_current_route();
		if (route_app) {
			frappe.current_app = route_app;
		} else if (frappe.boot.app_data && frappe.boot.app_data.length) {
			// Use a copy to avoid mutating the original sort_order
			const sorted = [...frappe.boot.app_data].sort((a, b) => ((a.workspaces || []).length < (b.workspaces || []).length ? 1 : -1));
			frappe.current_app = sorted[0].app_name;
		}
		frappe.frappe_toolbar.set_app_logo("/assets/neoffice_theme/images/neoffice_logo.svg");
	}

	set_active_workspace_item() {
		if (!frappe.get_route()) return;
		let current_route = frappe.get_route();
		let current_route_str = frappe.get_route_str();
		let current_item;
		if (current_route[0] == "Workspaces") {
			current_item = current_route[1];
		} else if (frappe.breadcrumbs) {
			if (Object.keys(frappe.breadcrumbs.all).length == 0) return;
			if (frappe.breadcrumbs.all[current_route_str]) {
				current_item =
					frappe.breadcrumbs.all[current_route_str].workspace ||
					frappe.breadcrumbs.all[current_route_str].module;
			}
		}
		if (this.is_route_in_sidebar(current_item)) {
			this.active_item.addClass("active-sidebar");
		}
		if (this.active_item) {
			if (this.is_nested_item(this.active_item.parent())) {
				let current_item = this.active_item.parent();
				this.expand_parent_item(current_item);
			}
		}
		if (!this.sidebar_expanded) this.close_children_item();
	}
	expand_parent_item(item) {
		let parent_title = item.attr("item-parent");
		if (!parent_title) return;

		let parent = this.get_sidebar_item(parent_title);
		if (parent) {
			let $drop_icon = $(parent).find(".drop-icon");
			if ($($(parent).children()[1]).hasClass("hidden")) {
				$drop_icon[0].click();
				if (this.is_nested_item($(parent))) {
					this.expand_parent_item($(parent));
				}
			}
		}
	}
	is_nested_item(item) {
		if (item.attr("item-parent")) {
			return true;
		} else {
			return false;
		}
	}

	get_sidebar_item(name) {
		let sidebar_item = "";
		$(".sidebar-item-container").each(function () {
			if ($(this).attr("item-name") == name) {
				sidebar_item = this;
			}
		});
		return sidebar_item;
	}
	is_route_in_sidebar(active_module) {
		let match = false;
		const that = this;
		$(".item-anchor").each(function () {
			const title = $(this).attr("title");

			//// Neoffice — fb2a5ef11e (2025-11-08 "Support virtual apps with translated workspace names"):
			//// develop only compares the anchor title; the active item is also matched on the container's
			//// item-name so a translated title still highlights its workspace.
			// Quick check with title first (most common case)
			if (title == active_module) {
				match = true;
				if (that.active_item) that.active_item.removeClass("active-sidebar");
				that.active_item = $(this).parent();
				return false; // exit the each loop
			}

			// Check item-name only if title didn't match (for translated workspaces)
			const item_container = $(this).closest(".sidebar-item-container");
			const item_name = item_container.attr("item-name");

			if (item_name == active_module) {
				match = true;
				if (that.active_item) that.active_item.removeClass("active-sidebar");
				that.active_item = $(this).parent();
				return false; // exit the each loop
			}
		});
		return match;
	}

	setup_pages() {
		this.set_all_pages();
		this.all_pages.forEach((page) => {
			page.is_editable = !page.public || this.has_access;
			if (typeof page.content == "string") {
				page.content = JSON.parse(page.content);
			}
		});

		if (this.all_pages) {
			frappe.workspaces = {};
			frappe.workspace_list = [];
			frappe.workspace_map = {};
			for (let page of this.all_pages) {
				frappe.workspaces[frappe.router.slug(page.name)] = {
					name: page.name,
					public: page.public,
				};
				if (!page.app && page.module) {
					page.app = frappe.boot.module_app[frappe.router.slug(page.module)];
				}
				frappe.workspace_map[page.name] = page;
				frappe.workspace_list.push(page);
			}
			// //// NEOFFICE PATCH — headless (cockpit): data maps only, no DOM
			if (!this.headless) this.make_sidebar();
		}
		if (this.headless) return;
		this.set_hover();
		this.set_sidebar_state();
	}
	set_sidebar_state() {
		this.sidebar_expanded = true;
		if (localStorage.getItem("sidebar-expanded") !== null) {
			this.sidebar_expanded = JSON.parse(localStorage.getItem("sidebar-expanded"));
		}
		if (frappe.is_mobile()) {
			this.sidebar_expanded = false;
		}
		this.expand_sidebar();
	}
	make_sidebar() {
		if (this.wrapper.find(".standard-sidebar-section")[0]) {
			this.wrapper.find(".standard-sidebar-section").remove();
		}

		//// Neoffice — make_sidebar keeps only the root pages of the current app (boot.app_data_map, built
		//// by apps_switcher.create_app_data_map — 9999364ec6 / 84b6f7a10b); develop lists every public
		//// page. Null-guarded by a03d7f00db (2026-03-11).
		let app_entry = (frappe.boot.app_data_map || {})[frappe.current_app || "frappe"];
		let app_workspaces = app_entry ? app_entry.workspaces || [] : [];

		let parent_pages = this.all_pages.filter((p) => !p.parent_page).uniqBy((p) => p.name);
		if (frappe.current_app === "private") {
			parent_pages = parent_pages.filter((p) => !p.public);
		} else {
			parent_pages = parent_pages.filter((p) => p.public && app_workspaces.includes(p.name));
		}

		this.build_sidebar_section("All", parent_pages);
		//// Neoffice — aa4938f2f0 (2026-04-24): "All Modules" section under the workspace tree.
		this.build_modules_section();

		// Scroll sidebar to selected page if it is not in viewport.
		this.wrapper.find(".selected").length &&
			!frappe.dom.is_element_in_viewport(this.wrapper.find(".selected")) &&
			this.wrapper.find(".selected")[0].scrollIntoView();

		this.setup_sorting();
		this.set_active_workspace_item();
		this.set_hover();
	}

	build_sidebar_section(title, root_pages) {
		//// Neoffice — aa4938f2f0 (2026-04-24): "Navigation" heading above the workspace tree; develop
		//// renders the section without a label.
		// Render a section heading for the main navigation section ("All").
		// Additional sections (e.g. "Modules") render their own heading.
		const heading_text = title === "All" ? __("Navigation") : "";
		const heading_html = heading_text
			? `<div class="standard-sidebar-label">${heading_text}</div>`
			: "";

		let sidebar_section = $(
			`<div class="standard-sidebar-section nested-container" data-title="${title}">${heading_html}</div>`
		);

		this.prepare_sidebar(root_pages, sidebar_section, this.wrapper.find(".sidebar-items"));

		if (Object.keys(root_pages).length === 0) {
			sidebar_section.addClass("hidden");
		}

		$(".item-anchor").on("click", () => {
			$(".list-sidebar.hidden-xs.hidden-sm").removeClass("opened");
			// $(".close-sidebar").css("display", "none");
			$("body").css("overflow", "auto");
			if (frappe.is_mobile()) {
				this.close_sidebar();
			}
		});

		if (
			sidebar_section.find(".sidebar-item-container").length &&
			sidebar_section.find("> [item-is-hidden='0']").length == 0
		) {
			sidebar_section.addClass("hidden show-in-edit-mode");
		}
	}

	//// Neoffice — added (aa4938f2f0, 2026-04-24 "feat(sidebar): add 'All Modules' section mirroring
	//// the app-switcher dropdown"), build_modules_section + append_module_item: one entry per
	//// boot.app_data app (App Customization sort_order), the active app highlighted, click routed
	//// through apps_switcher.set_current_app. No develop equivalent.
	build_modules_section() {
		// Render a mirror of the top app-switcher dropdown as a sidebar section
		// so users can jump between modules directly, with the active one highlighted.
		const $items_container = this.wrapper.find(".sidebar-items");

		const apps = [...(frappe.boot.app_data || [])].sort((a, b) => {
			const order_a = a.sort_order !== undefined ? a.sort_order : 999;
			const order_b = b.sort_order !== undefined ? b.sort_order : 999;
			if (order_a !== order_b) return order_a - order_b;
			return (a.app_title || a.app_name || "").localeCompare(
				b.app_title || b.app_name || ""
			);
		});

		if (!apps.length) return;

		const $section = $(
			`<div class="standard-sidebar-section modules-section" data-title="Modules">
				<div class="standard-sidebar-label">${__("All Modules")}</div>
			</div>`
		);

		for (const app of apps) {
			this.append_module_item(app, $section);
		}

		$section.appendTo($items_container);
	}

	append_module_item(app, $section) {
		const is_active = app.app_name === frappe.current_app;
		const route = app.app_route || "/app";
		const title = __(app.app_title || app.app_name);
		const logo_url = app.app_logo_url || "/assets/frappe/images/frappe-framework-logo.svg";

		const $item = $(`
			<div
				class="sidebar-item-container module-item ${is_active ? "active-sidebar active-module" : ""}"
				data-app-name="${app.app_name}"
				data-app-route="${route}"
			>
				<div class="standard-sidebar-item">
					<a class="item-anchor" href="${route}" title="${title}">
						<span class="sidebar-item-icon app-logo-container">
							<img class="app-logo" src="${logo_url}" alt="${__("App Logo")}" />
						</span>
						<span class="sidebar-item-label">${title}</span>
					</a>
				</div>
			</div>
		`);

		$item.find(".item-anchor").on("click", (e) => {
			e.preventDefault();
			if (this.apps_switcher && typeof this.apps_switcher.set_current_app === "function") {
				this.apps_switcher.set_current_app(app.app_name);
			}
			if (route && route.startsWith("/app")) {
				frappe.set_route(route);
			}
		});

		$item.appendTo($section);
	}

	prepare_sidebar(items, child_container, item_container) {
		let last_item = null;
		for (let item of items) {
			if (item.public && last_item && !last_item.public) {
				$(`<div class="divider"></div>`).appendTo(child_container);
			}

			// visibility not explicitly set to 0
			if (item.visibility !== 0) {
				this.append_item(item, child_container);
			}
			last_item = item;
		}
		child_container.appendTo(item_container);
	}
	toggle_sidebar() {
		if (!this.sidebar_expanded) {
			this.open_sidebar();
		} else {
			this.close_sidebar();
		}
	}
	expand_sidebar() {
		let direction;
		if (this.sidebar_expanded) {
			this.wrapper.addClass("expanded");
			// this.sidebar_expanded = false
			direction = "left";
		} else {
			this.wrapper.removeClass("expanded");
			// this.sidebar_expanded = true
			direction = "right";
		}
		localStorage.setItem("sidebar-expanded", this.sidebar_expanded);
		this.wrapper
			.find(".body-sidebar .collapse-sidebar-link")
			.find("use")
			.attr("href", `#es-line-arrow-${direction}`);
	}

	append_item(item, container) {
		let is_current_page = false;

		item.selected = is_current_page;

		if (is_current_page) {
			this.current_page = { name: item.name, public: item.public };
		}

		let $item_container = this.sidebar_item_container(item);
		let sidebar_control = $item_container.find(".sidebar-item-control");

		let child_items = this.all_pages.filter(
			(page) => page.parent_page == item.name || page.parent_page == item.title
		);
		if (child_items.length > 0) {
			let child_container = $item_container.find(".sidebar-child-item");
			child_container.addClass("hidden");
			this.prepare_sidebar(child_items, child_container, $item_container);
			this.parent_items.push($item_container);
		}

		$item_container.appendTo(container);
		this.sidebar_items[item.public ? "public" : "private"][item.name] = $item_container;

		if ($item_container.parent().hasClass("hidden") && is_current_page) {
			$item_container.parent().toggleClass("hidden");
		}

		this.add_toggle_children(item, sidebar_control, $item_container);

		if (child_items.length > 0) {
			$item_container.find(".drop-icon").first().addClass("show-in-edit-mode");
		}
	}

	sidebar_item_container(item) {
		item.indicator_color =
			item.indicator_color || this.indicator_colors[Math.floor(Math.random() * 12)];
		let path;
		if (item.type === "Link") {
			if (item.link_type === "Report") {
				path = frappe.utils.generate_route({
					type: item.link_type,
					name: item.link_to,
					is_query_report: item.report.report_type === "Query Report",
					report_ref_doctype: item.report.ref_doctype,
				});
			} else {
				path = frappe.utils.generate_route({ type: item.link_type, name: item.link_to });
			}
		} else if (item.type === "URL") {
			path = item.external_link;
		} else {
			if (item.public) {
				path = "/app/" + frappe.router.slug(item.name);
			} else {
				path = "/app/private/" + frappe.router.slug(item.name.split("-")[0]);
			}
		}

		return $(`
			<div
				class="sidebar-item-container ${item.is_editable ? "is-draggable" : ""}"
				item-parent="${item.parent_page}"
				item-name="${item.name}"
				item-title="${item.title}"
				item-public="${item.public || 0}"
				item-is-hidden="${item.is_hidden || 0}"
			>
				<div class="standard-sidebar-item ${item.selected ? "selected" : ""}">
					<a
						href="${path}"
						target="${item.type === "URL" ? "_blank" : ""}"
						class="item-anchor ${item.is_editable ? "" : "block-click"}" title="${__(item.title)}"
					>
						<span class="sidebar-item-icon" item-icon=${item.icon || "folder-normal"}>
							${
								item.public || item.icon
									? frappe.utils.icon(item.icon || "folder-normal", "md")
									: `<span class="indicator ${item.indicator_color}"></span>`
							}
						</span>
						<span class="sidebar-item-label">${__(item.title)}<span>
					</a>
					<div class="sidebar-item-control"></div>
				</div>
				<div class="sidebar-child-item nested-container"></div>
			</div>
		`);
	}

	add_toggle_children(item, sidebar_control, item_container) {
		let drop_icon = "es-line-down";
		if (
			this.current_page &&
			item_container.find(`[item-name="${this.current_page.name}"]`).length
		) {
			drop_icon = "small-up";
		}

		let $child_item_section = item_container.find(".sidebar-child-item");
		let $drop_icon = $(`<button class="btn-reset drop-icon hidden">`)
			.html(frappe.utils.icon(drop_icon, "sm"))
			.appendTo(sidebar_control);

		if (
			this.all_pages.some(
				(e) =>
					(e.parent_page == item.title || e.parent_page == item.name) &&
					(e.is_hidden == 0 || !this.is_read_only)
			)
		) {
			$drop_icon.removeClass("hidden");
		}
		$drop_icon.on("click", () => {
			let opened = $drop_icon.find("use").attr("href") === "#es-line-down";

			if (!opened) {
				$drop_icon.attr("data-state", "closed").find("use").attr("href", "#es-line-down");
			} else {
				$drop_icon.attr("data-state", "opened").find("use").attr("href", "#es-line-up");
			}
			``;
			$child_item_section.toggleClass("hidden");
		});
	}
	toggle_sorting() {
		this.sorting_items.forEach((item) => {
			var state = item.option("disabled");
			item.option("disabled", !state);
		});
	}
	setup_sorting() {
		if (!this.has_access) return;
		this.sorting_items = [];
		for (let container of this.$sidebar.find(".nested-container")) {
			this.sorting_items[this.sorting_items.length] = Sortable.create(container, {
				group: "sidebar-items",
				disabled: true,
				onEnd: () => {
					let sidebar_items = [];
					for (let container of this.$sidebar.find(".nested-container")) {
						for (let item of $(container).children()) {
							let parent = "";
							if ($(item).parent().hasClass("sidebar-child-item")) {
								parent = $(item)
									.parent()
									.closest(".sidebar-item-container")
									.attr("item-name");
							}

							sidebar_items.push({
								name: item.getAttribute("item-name"),
								parent: parent,
							});
						}
					}
					frappe.xcall(
						"frappe.desk.doctype.workspace_settings.workspace_settings.set_sequence",
						{
							sidebar_items: sidebar_items,
						}
					);
				},
			});
		}
	}

	close_sidebar() {
		this.sidebar_expanded = false;
		this.expand_sidebar();
		this.close_children_item();
		if (frappe.is_mobile()) frappe.app.sidebar.prevent_scroll();
	}
	open_sidebar() {
		this.sidebar_expanded = true;
		this.expand_sidebar();
		this.set_active_workspace_item();
	}

	close_children_item() {
		this.parent_items.forEach((i) => {
			if (!$($(i).children()[1]).hasClass("hidden")) $(i).find(".drop-icon").click();
		});
	}

	reload() {
		return frappe.workspace.get_pages().then((r) => {
			frappe.boot.sidebar_pages = r;
			this.setup_pages();
		});
	}
	set_height() {
		$(".body-sidebar").css("height", window.innerHeight + "px");
		$(".overlay").css("height", window.innerHeight + "px");
		document.body.style.overflow = "hidden";
	}
	handle_outside_click() {
		document.addEventListener("click", (e) => {
			if (this.apps_switcher.drop_down_expanded) {
				if (!e.composedPath().includes(this.apps_switcher.app_switcher_dropdown)) {
					this.apps_switcher.toggle_app_menu();
				}
			}
		});
	}
	prevent_scroll() {
		let main_section = $(".main-section");
		if (this.sidebar_expanded) {
			main_section.css("overflow", "hidden");
		} else {
			main_section.css("overflow", "");
		}
	}
};
