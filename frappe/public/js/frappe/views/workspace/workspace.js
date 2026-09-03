import EditorJS from "@editorjs/editorjs";
import Undo from "editorjs-undo";

frappe.standard_pages["Workspaces"] = function () {
	var wrapper = frappe.container.add_page("Workspaces");

	frappe.ui.make_app_page({
		parent: wrapper,
		name: "Workspaces",
		title: __("Workspace"),
		//// Neoffice — added (cc297ec402, 2025-11-03 "Move sidebar to right and add mobile toggle"):
		//// upstream lets the Workspaces page keep the two-column layout so it can put its own sidebar
		//// in .layout-side-section. That sidebar is now the global one (see the header), so the page
		//// must not reserve a side column.
		single_column: true,
	});

	frappe.workspace = new frappe.views.Workspace(wrapper);
	$(wrapper).bind("show", function () {
		frappe.workspace.show();
	});
};

////////////////////////////////////////////////////////////////////////
//// Neoffice — Workspace view: ~50 hunks vs upstream v15.89.0, each marked below. The map:
////   9999364ec6 2025-10-28 + cc297ec402 2025-11-03 — the sidebar stopped belonging to this
////     view. Upstream Workspace builds its own .desk-sidebar inside .layout-side-section and
////     owns all_pages / public_pages / private_pages; ours delegates to the global
////     frappe.app.sidebar (ui/sidebar.js, marked there) and reads its data. Every
////     `this.sidebar.find(...)` therefore became `const $sidebar = this.sidebar.$sidebar ||
////     this.sidebar` — the sidebar object is no longer a jQuery set.
////   0634af137c 2025-11-13 + 84b6f7a10b 2025-11-09 — workspaces are scoped to the app they
////     belong to: get_pages() sends the current workspace, show_page() resolves the owning app
////     (boot.workspace_to_app_map › page.app › boot.module_app) and switches the app switcher,
////     and pages are keyed by NAME instead of TITLE throughout (two workspaces of different
////     apps can share a title).
////   d0268ef91a 2026-06-10 — under the NeoCockpit chrome frappe.ui.Sidebar runs HEADLESS (data
////     maps, no DOM), so every sidebar-DOM method here early-returns on this.sidebar.headless.
////   Older, no rationale in the commits: 755884e049 2024-01-17 (custom_link),
////     c3a2678c23 2024-01-24 (collapsed sections remembered in localStorage), 09d4ec114e /
////     06517453c7 2024-10-07, bd41f1e7a5 2025-02-26, 9cd3ad12a4 2025-03-17.
////   Later fixes: fb2a5ef11e 2025-11-08 (null current_page), a9959ef957 2026-01-31 (name !=
////     title), 388383af3a 2026-03-12 (Edit/New moved from the footer to a header bar),
////     07ee48dd37 2026-06-16 (deleting a widget from the UI).
//// TO REVIEW at the merge: this file ships console.log/console.warn debug traces added by
//// 0634af137c and d0268ef91a (reload_sidebar_pages, make_sidebar, build_sidebar_section,
//// prepare_sidebar) — they run on every workspace load in production.
//// v16 merge note: upstream develop rewrote this file (Manage Workspaces rail, hidden-notice
//// blocks, role-gated access) and ALSO moved to `this.sidebar = frappe.app.sidebar` — same
//// direction, different code. Expect a whole-file conflict; re-apply our intent, do not merge
//// line by line.
////////////////////////////////////////////////////////////////////////
frappe.views.Workspace = class Workspace {
	constructor(wrapper) {
		this.wrapper = $(wrapper);
		this.page = wrapper.page;
		this.blocks = frappe.workspace_block.blocks;
		this.is_read_only = true;
		this.pages = {};
		this.sorted_public_items = [];
		this.sorted_private_items = [];
		this.current_page = {};
		this.sidebar_items = {
			public: {},
			private: {},
		};
		this.sidebar_categories = [
			{ id: "Personal", label: __("Personal", null, "Workspace Category") },
			{ id: "Public", label: __("Public", null, "Workspace Category") },
		];
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

		this.prepare_container();
		//// Neoffice — upstream: `this.setup_pages();` alone. Rewritten by 9999364ec6 (2025-10-28,
		//// sidebar + apps-switcher rework): the pages now come from the global frappe.app.sidebar, and
		//// public_pages / private_pages / has_access / cached_pages are derived from it here so the
		//// rest of the class keeps working. this.show() is called explicitly because setup_pages() no
		//// longer ends with it.
		this.sidebar = frappe.app.sidebar;
		this.sidebar.setup_pages();
		this.cached_pages = $.extend(true, {}, frappe.boot.sidebar_pages);
		this.has_access = frappe.boot.sidebar_pages.has_access;
		this.has_create_access = frappe.boot.sidebar_pages.has_create_access;

		// Define public_pages and private_pages from sidebar
		this.public_pages = this.sidebar.all_pages.filter((page) => page.public);
		this.private_pages = this.sidebar.all_pages.filter((page) => !page.public);

		this.show();
		this.register_awesomebar_shortcut();
	}

	//// Neoffice — upstream creates the workspace's own sidebar DOM here (a .list-sidebar
	//// .overlay-sidebar wrapper appended to .layout-side-section, with this.sidebar pointing at its
	//// .desk-sidebar). Deleted by 9999364ec6 (2025-10-28): the sidebar is global now, this method
	//// only keeps the body and the Edit/New buttons.
	prepare_container() {
		this.body = this.wrapper.find(".layout-main-section");
		this.prepare_new_and_edit();
	}

	async setup_pages(reload) {
		!this.discard && this.create_page_skeleton();
		!this.discard && this.create_sidebar_skeleton();
		this.sidebar_pages = !this.discard ? await this.get_pages() : this.sidebar_pages;
		this.cached_pages = $.extend(true, {}, this.sidebar_pages);
		this.all_pages = this.sidebar_pages.pages;
		this.has_access = this.sidebar_pages.has_access;
		this.has_create_access = this.sidebar_pages.has_create_access;

		this.all_pages.forEach((page) => {
			page.is_editable = !page.public || this.has_access;
		});

		this.public_pages = this.all_pages.filter((page) => page.public);
		this.private_pages = this.all_pages.filter((page) => !page.public);

		if (this.all_pages) {
			frappe.workspaces = {};
			for (let page of this.all_pages) {
				frappe.workspaces[frappe.router.slug(page.name)] = {
					title: page.title,
					public: page.public,
				};
			}
			this.make_sidebar();

			//// Neoffice — added (0634af137c, 2025-11-13 "Enhance workspace and app customization logic"):
			//// upstream only ever SHOWS the sidebar edit controls (drag handles, per-item menus) when edit
			//// mode starts, and never hides them again, so after a save they stayed on a read-only
			//// sidebar. Every setup_pages now settles them both ways (hide_sidebar_actions is added
			//// further down).
			// Hide sidebar actions if not in edit mode
			if (this.body && this.body.hasClass('edit-mode')) {
				// In edit mode - show controls
				this.show_sidebar_actions();
			} else {
				// Not in edit mode - hide controls
				this.hide_sidebar_actions();
			}

			reload && this.show();
		}
	}

	prepare_new_and_edit() {
		this.$page = $(`
		//// Neoffice — upstream markup: <div class="editor-js-container"> first, then a
		//// <div class="workspace-footer"> holding the New and Edit buttons. Swapped by 388383af3a
		//// (2026-03-12 "move edit/new buttons from footer to header"): in the footer they sat behind
		//// the chat widgets and could not be clicked. The editor container now comes after this block,
		//// the class is .workspace-header-actions (scss/desk/desktop.scss, marked there) and the New
		//// button lost .ellipsis / gained .btn-sm to match the Edit one.
		<div class="workspace-header-actions">
			<button data-label="New" class="btn btn-default btn-sm btn-new-workspace">
				<svg class="es-icon es-line icon-xs" style="" aria-hidden="true">
					<use class="" href="#es-line-add"></use>
				</svg>
				<span class="hidden-xs" data-label="New">${__("New")}</span>
			</button>
			<button class="btn btn-default btn-sm btn-edit-workspace" data-label="Edit">
				<svg class="es-icon es-line icon-xs" style="" aria-hidden="true">
					<use class="" href="#es-line-edit"></use>
				</svg>
				<span class="hidden-xs" data-label="Edit">${__("Edit")}</span>
			</button>
		</div>
		<div class="editor-js-container"></div>
	`).appendTo(this.body);

		this.body.find(".btn-new-workspace").on("click", () => {
			this.initialize_new_page(true);
		});

		this.body.find(".btn-edit-workspace").on("click", async () => {
			if (!this.editor || !this.editor.readOnly) return;
			this.is_read_only = false;
			//// Neoffice — entering edit mode, rewritten by 0634af137c (2025-11-13); block runs to the end
			//// of the handler. Upstream: toggle_hidden_workspaces(true) BEFORE the toggle, then everything
			//// else inside `this.editor.isReady.then(...)`. Ours awaits isReady and, before showing the
			//// controls, reloads the pages from the server with the current workspace as context
			//// (reload_sidebar_pages) and rebuilds the sidebar — the app-scoped page list is not the one
			//// loaded at boot, and the edit controls only exist on freshly built items.
			//// toggle_hidden_workspaces is not removed, it moved after that rebuild.
			await this.editor.readOnly.toggle();
			await this.editor.isReady;
			this.body.addClass("edit-mode");
			this.initialize_editorjs_undo();
			this.setup_customization_buttons(this._page);
			// Reload pages from server with current workspace context
			await this.reload_sidebar_pages();
			// Rebuild sidebar to ensure controls are created, then show them
			this.make_sidebar();
			this.toggle_hidden_workspaces(true);
			this.show_sidebar_actions();
			this.make_blocks_sortable();
		});
	}

	//// Neoffice — get_pages rewritten and reload_sidebar_pages added (0634af137c, 2025-11-13; the
	//// block runs to the end of that method). Upstream calls
	//// frappe.desk.desktop.get_workspace_sidebar_items with no argument and gets every workspace
	//// the user may see. Ours passes the current workspace (this._page, else
	//// localStorage.current_page) so the server can return only the workspaces of the app that
	//// workspace belongs to, and reload_sidebar_pages refreshes that list plus the frappe.workspaces
	//// cache when the context changes. TO REVIEW: the console.log traces in reload_sidebar_pages
	//// run on every edit-mode entry in production.
	get_pages() {
		// Pass current workspace to backend so it can filter workspaces by app
		// Use localStorage.current_page as fallback if this._page is not set yet
		const current_workspace = (this._page && this._page.name) || localStorage.current_page || null;
		return frappe.xcall("frappe.desk.desktop.get_workspace_sidebar_items", {
			current_workspace: current_workspace
		});
	}

	async reload_sidebar_pages() {
		// Reload pages from server with current workspace context
		console.log("[reload_sidebar_pages] START - Before reload:", {
			public_pages_count: this.public_pages?.length || 0,
			all_pages_count: this.all_pages?.length || 0
		});

		this.sidebar_pages = await this.get_pages();
		this.all_pages = this.sidebar_pages.pages;
		this.public_pages = this.all_pages.filter((page) => page.public);
		this.private_pages = this.all_pages.filter((page) => !page.public);

		console.log("[reload_sidebar_pages] END - After reload:", {
			public_pages_count: this.public_pages?.length || 0,
			all_pages_count: this.all_pages?.length || 0,
			pages_names: this.public_pages?.map(p => p.title) || [],
			pages_with_debug: this.public_pages?.map(p => ({name: p.name, title: p.title, sort: p._debug_sort_order})) || []
		});

		// Update frappe.workspaces cache
		if (this.all_pages) {
			frappe.workspaces = {};
			for (let page of this.all_pages) {
				frappe.workspaces[frappe.router.slug(page.name)] = {
					title: page.title,
					public: page.public,
				};
			}
		}
	}

	sidebar_item_container(item) {
		item.indicator_color =
			item.indicator_color || this.indicator_colors[Math.floor(Math.random() * 12)];

		//// Neoffice — added (755884e049, 2024-01-17 "change link by view_interface", no message):
		//// upstream builds the href inline from item.public and the slugged TITLE. Hoisted into a
		//// `link` const so a workspace can override its destination with item.custom_link — used to
		//// point a sidebar entry at a page that is not the workspace itself. The bare //// note is the
		//// original author's; the href below reads that const.
		//// add link and change link in return
		const link = item.custom_link || (item.public ? frappe.router.slug(item.title) : "private/" + frappe.router.slug(item.title));

		return $(`
			<div
				class="sidebar-item-container ${item.is_editable ? "is-draggable" : ""}"
				item-parent="${item.parent_page}"
				item-name="${item.title}"
				item-public="${item.public || 0}"
				item-is-hidden="${item.is_hidden || 0}"
			>
				<div class="desk-sidebar-item standard-sidebar-item ${item.selected ? "selected" : ""}">
					<a
						href="/app/${link}"
						class="item-anchor ${item.is_editable ? "" : "block-click"}" title="${__(item.title)}"
					>
						<span class="sidebar-item-icon" item-icon=${item.icon || "folder-normal"}>
							${
								item.public
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

	make_sidebar() {
		//// Neoffice — make_sidebar, rewritten (block runs to remove_sidebar_skeleton below). Upstream
		//// does `this.sidebar.find(".standard-sidebar-section").remove()` — this.sidebar was its own
		//// jQuery set. Since 9999364ec6 it is the global frappe.ui.Sidebar object, hence the
		//// `this.sidebar.$sidebar || this.sidebar` resolution repeated in every sidebar method, and
		//// since d0268ef91a (2026-06-10) that object can be HEADLESS under the NeoCockpit chrome (data
		//// maps, no DOM) — hence the early return. The NEOFFICE PATCH note below is from that commit.
		//// TO REVIEW: the console.log traces added here by 0634af137c.
		// //// NEOFFICE PATCH — cockpit chrome: native sidebar DOM doesn't exist
		if (!this.sidebar || this.sidebar.headless) return;
		// Get the actual jQuery sidebar element
		const $sidebar = this.sidebar.$sidebar || this.sidebar;

		console.log("[make_sidebar] START:", {
			public_pages_count: this.public_pages?.length || 0,
			private_pages_count: this.private_pages?.length || 0,
			public_pages_names: this.public_pages?.map(p => p.title).slice(0, 5) || []
		});

		// Remove all existing sidebar sections before rebuilding
		$sidebar.find(".standard-sidebar-section").remove();

		this.sidebar_categories.forEach((category) => {
			let root_pages = this.public_pages.filter(
				(page) => page.parent_page == "" || page.parent_page == null
			);
			if (category.id != "Public") {
				root_pages = this.private_pages.filter(
					(page) => page.parent_page == "" || page.parent_page == null
				);
			}
			root_pages = root_pages.uniqBy((d) => d.title);
			console.log("[make_sidebar] Building section:", category.id, "with", root_pages.length, "root pages");
			this.build_sidebar_section(category, root_pages);
		});

		// Scroll sidebar to selected page if it is not in viewport.
		$sidebar.find(".selected").length &&
			!frappe.dom.is_element_in_viewport($sidebar.find(".selected")) &&
			$sidebar.find(".selected")[0].scrollIntoView();

		this.remove_sidebar_skeleton();
	}

	build_sidebar_section(category, root_pages) {
		//// Neoffice — added guard (0634af137c, 2025-11-13): build_sidebar_section can now be reached
		//// before the global sidebar exists (it is owned by frappe.app, not by this view), where
		//// upstream could assume its own DOM was already built.
		if (!this.sidebar) {
			console.warn('Workspace: sidebar not initialized in build_sidebar_section');
			return;
		}

		let sidebar_section = $(
			`<div class="standard-sidebar-section nested-container" data-title="${category.id}"></div>`
		);

		let $title = $(`<button class="btn-reset standard-sidebar-label">
			<span>${frappe.utils.icon("es-line-down", "xs")}</span>
			<span class="section-title">${category.label}<span>
		</div>`).appendTo(sidebar_section);
		$title.attr({
			"aria-label": __("Toggle Section: {0}", [category.label]),
			"aria-expanded": "true",
		});
		//// Neoffice — upstream: `this.prepare_sidebar(root_pages, sidebar_section, this.sidebar)` —
		//// the section is appended by prepare_sidebar into the sidebar itself. Ours appends the section
		//// to the resolved $sidebar first and passes the SECTION as the item container (9999364ec6 +
		//// 0634af137c), because the global sidebar hosts other apps' sections too and items must land
		//// inside their own section, not at the sidebar root.
		// Get the actual jQuery sidebar element and append section to it
		const $sidebar = this.sidebar.$sidebar || this.sidebar;

		sidebar_section.appendTo($sidebar);

		this.prepare_sidebar(root_pages, sidebar_section, sidebar_section);

		$title.on("click", (e) => {
			const $e = $(e.target);
			const href = $e.find("span use").attr("href");
			const isCollapsed = href === "#es-line-down";
			let icon = isCollapsed ? "#es-line-right-chevron" : "#es-line-down";
			$e.find("span use").attr("href", icon);
			$e.parent().find(".sidebar-item-container").toggleClass("hidden");
			$e.attr("aria-expanded", String(!isCollapsed));
		});

		if (Object.keys(root_pages).length === 0) {
			sidebar_section.addClass("hidden");
		}

		$(".item-anchor").on("click", () => {
			$(".list-sidebar.hidden-xs.hidden-sm").removeClass("opened");
			$(".close-sidebar").css("display", "none");
			$("body").css("overflow", "auto");
		});

		if (
			sidebar_section.find(".sidebar-item-container").length &&
			sidebar_section.find("> [item-is-hidden='0']").length == 0
		) {
			sidebar_section.addClass("hidden show-in-edit-mode");
		}
	}

	prepare_sidebar(items, child_container, item_container) {
		//// Neoffice — added guard (0634af137c, 2025-11-13): with the container now passed in by the
		//// caller (see build_sidebar_section above) an undefined container is possible; upstream always
		//// passed this.sidebar. Logs and returns instead of throwing mid-render.
		if (!item_container) {
			console.error('Workspace: item_container is undefined in prepare_sidebar');
			return;
		}
		items.forEach((item) => this.append_item(item, child_container));
		child_container.appendTo(item_container);
	}

	append_item(item, container) {
		let is_current_page =
			frappe.router.slug(item.title) == frappe.router.slug(this.get_page_to_show().name) &&
			item.public == this.get_page_to_show().public;
		item.selected = is_current_page;
		if (is_current_page) {
			this.current_page = { name: item.title, public: item.public };
		}

		let $item_container = this.sidebar_item_container(item);
		let sidebar_control = $item_container.find(".sidebar-item-control");

		this.add_sidebar_actions(item, sidebar_control);
		let pages = item.public ? this.public_pages : this.private_pages;

		let child_items = pages.filter((page) => page.parent_page == item.title);
		if (child_items.length > 0) {
			let child_container = $item_container.find(".sidebar-child-item");
			child_container.addClass("hidden");
			this.prepare_sidebar(child_items, child_container, $item_container);
		}

		$item_container.appendTo(container);
		this.sidebar_items[item.public ? "public" : "private"][item.title] = $item_container;

		if ($item_container.parent().hasClass("hidden") && is_current_page) {
			$item_container.parent().toggleClass("hidden");
		}

		this.add_drop_icon(item, sidebar_control, $item_container);

		if (child_items.length > 0) {
			$item_container.find(".drop-icon").first().addClass("show-in-edit-mode");
		}
	}

	add_drop_icon(item, sidebar_control, item_container) {
		let drop_icon = "es-line-down";
		if (item_container.find(`[item-name="${this.current_page.name}"]`).length) {
			drop_icon = "small-up";
		}

		let $child_item_section = item_container.find(".sidebar-child-item");
		let $drop_icon = $(`<button class="btn-reset drop-icon hidden">`)
			.html(frappe.utils.icon(drop_icon, "sm"))
			.appendTo(sidebar_control);
		let pages = item.public ? this.public_pages : this.private_pages;
		if (
			pages.some(
				(e) => e.parent_page == item.title && (e.is_hidden == 0 || !this.is_read_only)
			)
		) {
			$drop_icon.removeClass("hidden");
		}

		//// Neoffice — added (c3a2678c23, 2024-01-24 "updates to simplify menu", no message; block runs
		//// to the end of the click handler). Upstream always renders a nested sidebar section collapsed
		//// and forgets the choice on the next page load. The open sections are kept in the
		//// localStorage key list_sidebar_open (a JSON array of item titles) and restored here. Keyed by
		//// TITLE, so a translated or renamed workspace loses its state — TO REVIEW. The bare ////
		//// notes and the commented-out lines are the original author's.
		//// Check local storage for saved state
		let existingArray = JSON.parse(localStorage.getItem("list_sidebar_open") || '[]');
		if (existingArray.includes(item.title)) {
			$child_item_section.removeClass("hidden");
			$drop_icon.find("use").attr("href", "#es-line-up");
		} else {
			$child_item_section.addClass("hidden");
			$drop_icon.find("use").attr("href", "#es-line-down");
		}

		$drop_icon.on("click", () => {
			let existingArray = JSON.parse(localStorage.getItem("list_sidebar_open") || '[]');
			let icon =
				$drop_icon.find("use").attr("href") === "#es-line-down"
					? "#es-line-up"
					: "#es-line-down";
			$drop_icon.find("use").attr("href", icon);
			$child_item_section.toggleClass("hidden");
			//// Save state to local storage
			if($drop_icon.find("use").attr("href") === "#es-line-down") {
				if (existingArray.includes(item.title)) {
					existingArray.splice(existingArray.indexOf(item.title), 1);
					localStorage.setItem("list_sidebar_open", JSON.stringify(existingArray));
				}
				//localStorage.setItem(item.title, "closed");
			} else {
				if (!existingArray.includes(item.title)) {
					existingArray.push(item.title);
					localStorage.setItem("list_sidebar_open", JSON.stringify(existingArray));
				}
				//localStorage.setItem(item.title, "open");
			}
		});
	}

	show() {
		//// Neoffice — upstream waits on `!this.all_pages`; the pages live on the global sidebar since
		//// 9999364ec6, so the readiness test moved with them.
		if (!this.sidebar || !this.sidebar.all_pages) {
			// pages not yet loaded, call again after a bit
			setTimeout(() => this.show(), 100);
			return;
		}

		let page = this.get_page_to_show();

		if (!frappe.router.current_route[0]) {
			frappe.route_flags.replace_route = true;
			frappe.set_route(frappe.router.slug(page.public ? page.name : "private/" + page.name));
			return;
		}

		this.page.set_title(__(page.name));
		//// Neoffice — upstream calls update_selected_sidebar(old, false) then (new, true) here to move
		//// the .selected class. Dropped by 9999364ec6 (2025-10-28): the global sidebar highlights the
		//// active item itself, from the route (ui/sidebar.js is_route_in_sidebar, marked there).
		this.show_page(page);
	}

	update_selected_sidebar(page, add) {
		let section = page.public ? "public" : "private";
		if (
			this.sidebar &&
			this.sidebar_items[section] &&
			this.sidebar_items[section][page.name]
		) {
			let $sidebar = this.sidebar_items[section][page.name];
			let pages = page.public ? this.public_pages : this.private_pages;
			let sidebar_page = pages.find((p) => p.title == page.name);

			if (add) {
				$sidebar[0].firstElementChild.classList.add("selected");
				if (sidebar_page) sidebar_page.selected = true;

				// open child sidebar section if closed
				$sidebar.parent().hasClass("sidebar-child-item") &&
					$sidebar.parent().hasClass("hidden") &&
					$sidebar.parent().removeClass("hidden");

				this.current_page = { name: page.name, public: page.public };
				localStorage.current_page = page.name;
				localStorage.is_current_page_public = page.public;
			} else {
				$sidebar[0].firstElementChild.classList.remove("selected");
				if (sidebar_page) sidebar_page.selected = false;
			}
		}
	}

	get_data(page) {
		return frappe
			.call("frappe.desk.desktop.get_desktop_page", {
				page: page,
			})
			.then((data) => {
				this.page_data = data.message;

				// caching page data
				this.pages[page.name] && delete this.pages[page.name];
				this.pages[page.name] = data.message;

				if (!this.page_data || Object.keys(this.page_data).length === 0) return;
				if (this.page_data.charts && this.page_data.charts.items.length === 0) return;

				return frappe.dashboard_utils.get_dashboard_settings().then((settings) => {
					if (settings) {
						let chart_config = settings.chart_config
							? JSON.parse(settings.chart_config)
							: {};
						this.page_data.charts.items.map((chart) => {
							chart.chart_settings = chart_config[chart.chart_name] || {};
						});
						this.pages[page.name] = this.page_data;
					}
				});
			});
	}

	get_page_to_show() {
		let default_page;

		if (frappe.boot.user.default_workspace) {
			default_page = {
				//// Neoffice — get_page_to_show, keyed by NAME instead of TITLE (0634af137c, 2025-11-13; block
				//// runs to the end of the method). Upstream compares page TITLES here and in the two branches
				//// below (localStorage.current_page and all_pages[0]). Two workspaces of different apps can
				//// share a title, and a workspace whose name differs from its title ("Simple - Ventes" titled
				//// "Ventes") never matched — the view fell back to the first page. The pages themselves now
				//// come from the global sidebar. The blank line further down is from the same commit.
				name: frappe.boot.user.default_workspace.name,
				public: frappe.boot.user.default_workspace.public,
			};
		} else if (
			localStorage.current_page &&
			this.sidebar.all_pages.filter((page) => page.name == localStorage.current_page)
				.length != 0
		) {
			default_page = {
				name: localStorage.current_page,
				public: localStorage.is_current_page_public != "false",
			};
		} else if (Object.keys(this.sidebar.all_pages).length !== 0) {
			default_page = {
				name: this.sidebar.all_pages[0].name,
				public: this.sidebar.all_pages[0].public,
			};
		} else {
			default_page = { name: "Build", public: true };
		}

		const route = frappe.get_route();
		const page = (route[1] == "private" ? route[2] : route[1]) || default_page.name;
		const is_public = route[1] ? route[1] != "private" : default_page.public;

		return { name: page, public: is_public };
	}

	async show_page(page) {
		if (!this.body.find("#editorjs")[0]) {
			$(`
				<div id="editorjs" class="desk-page page-main-content"></div>
			`).appendTo(this.body.find(".editor-js-container"));
		}

		//// Neoffice — show_page rewritten (0634af137c 2025-11-13, 84b6f7a10b 2025-11-09; block runs
		//// ~30 lines to `this.content && this.add_custom_cards_in_content()`). Upstream picks the page
		//// out of public_pages/private_pages by TITLE and parses current_page.content on every call.
		//// Ours finds it by NAME in the global sidebar's all_pages, then resolves which APP owns it —
		//// boot.workspace_to_app_map (the fast map added by 84b6f7a10b) › page.app › boot.module_app of
		//// its module › "frappe" — and tells the apps switcher to switch to it, so opening a workspace
		//// from a search result or a direct URL also switches the sidebar to that app. content is
		//// parsed once and cached on the page object instead of re-parsed at each show.
		if (this.sidebar.all_pages.length) {
			this.create_page_skeleton();

			let current_page = this.sidebar.all_pages.find((p) => p.name == page.name);
			this._page = current_page;

			// set app
			let app;
			if (!this._page.public) {
				app = "private";
			} else {
				// First check if workspace is mapped to a virtual app
				if (frappe.boot.workspace_to_app_map && frappe.boot.workspace_to_app_map[this._page.name]) {
					app = frappe.boot.workspace_to_app_map[this._page.name];
				} else {
					app = this._page.app;
					if (!app && this._page.module) {
						app = frappe.boot.module_app[frappe.router.slug(this._page.module)];
					}
					if (!app) app = "frappe";
				}
			}

			// Update sidebar to show workspaces for this app
			if (app && frappe.current_app !== app && frappe.app.sidebar?.apps_switcher) {
				frappe.app.sidebar.apps_switcher.set_current_app(app);
			}

			if (typeof current_page.content == "string") {
				current_page.content = JSON.parse(current_page.content);
			}

			this.content = current_page.content;

			this.content && this.add_custom_cards_in_content();

			$(".item-anchor").addClass("disable-click");

			if (this.pages && this.pages[current_page.name]) {
				this.page_data = this.pages[current_page.name];
			} else {
				await frappe.after_ajax(() => this.get_data(current_page));
			}

			this.setup_actions(page);

			this.prepare_editorjs();
			$(".item-anchor").removeClass("disable-click");

			this.remove_page_skeleton();
		}
	}

	add_custom_cards_in_content() {
		let index = -1;
		this.content.find((item, i) => {
			if (item.type == "card") index = i;
		});
		if (index !== -1) {
			this.content.splice(index + 1, 0, {
				type: "card",
				data: { card_name: "Custom Documents", col: 4 },
			});
			this.content.splice(index + 2, 0, {
				type: "card",
				data: { card_name: "Custom Reports", col: 4 },
			});
		}
	}

	prepare_editorjs() {
		if (this.editor) {
			this.editor.isReady.then(() => {
				this.editor.configuration.tools.chart.config.page_data = this.page_data;
				this.editor.configuration.tools.shortcut.config.page_data = this.page_data;
				this.editor.configuration.tools.card.config.page_data = this.page_data;
				this.editor.configuration.tools.onboarding.config.page_data = this.page_data;
				this.editor.configuration.tools.quick_list.config.page_data = this.page_data;
				this.editor.configuration.tools.number_card.config.page_data = this.page_data;
				this.editor.configuration.tools.custom_block.config.page_data = this.page_data;
				this.editor.render({ blocks: this.content || [] });
			});
		} else {
			this.initialize_editorjs(this.content);
		}
	}

	setup_actions(page) {
		let pages = page.public ? this.public_pages : this.private_pages;
		//// Neoffice — added (a9959ef957, 2026-01-31 "Allow editing workspaces where name differs from
		//// title"): upstream matches on p.title only, so the Edit button never appeared on a workspace
		//// renamed after creation. Matches on either.
		// Use p.name instead of p.title to handle workspaces where name != title (e.g., "Simple - Ventes" with title "Ventes")
		let current_page = pages.filter((p) => p.name == page.name || p.title == page.name)[0];

		if (!this.is_read_only) {
			this.setup_customization_buttons(current_page);
			return;
		}

		this.clear_page_actions();
		//// Neoffice — two changes on this line and on has_create_access a few lines below. The null
		//// guard is fb2a5ef11e (2025-11-08 "Support virtual apps with translated workspace names"):
		//// current_page can be undefined when the lookup above misses, and upstream read .is_editable
		//// off it straight away. The `frappe.session.user == "Administrator"` condition is ours too
		//// (the trailing //// notes are the original author's, no commit message explains it): editing
		//// and creating workspaces is reserved to Administrator on our instances — upstream gates them
		//// on the Workspace Manager role alone. TO REVIEW at the merge: this is a UI-only gate; the
		//// server still authorises whoever holds the role.
		if (current_page && current_page.is_editable && frappe.session.user == "Administrator") { //// add check if user is administrator
			this.body.find(".btn-edit-workspace").removeClass("hide");
		} else {
			this.body.find(".btn-edit-workspace").addClass("hide");
		}

		// need to add option for icons in inner buttons as well
		if (this.has_create_access && frappe.session.user == "Administrator") { //// add check if user is administrator
			this.body.find(".btn-new-workspace").removeClass("hide");
		} else {
			this.body.find(".btn-new-workspace").addClass("hide");
		}
	}

	initialize_editorjs_undo() {
		this.undo = new Undo({ editor: this.editor });
		this.undo.initialize({ blocks: this.content || [] });
		this.undo.readOnly = false;
	}

	clear_page_actions() {
		this.page.clear_primary_action();
		this.page.clear_secondary_action();
		this.page.clear_inner_toolbar();
	}

	setup_customization_buttons(page) {
		this.clear_page_actions();

		page.is_editable &&
			this.page.set_primary_action(
				__("Save"),
				() => {
					this.clear_page_actions();
					this.body.removeClass("edit-mode");
					//// Neoffice — added (0634af137c, 2025-11-13): leaving edit mode through Save now also strips
					//// the sidebar edit controls; upstream only removed the edit-mode class from the body.
					this.hide_sidebar_actions();
					this.save_page(page).then((saved) => {
						if (!saved) return;
						this.undo.readOnly = true;
						this.editor.readOnly.toggle();
						this.is_read_only = true;
					});
				},
				null,
				__("Saving")
			);

		this.page.set_secondary_action(__("Discard"), async () => {
			this.body.removeClass("edit-mode");
			//// Neoffice — same as on Save above (0634af137c): Discard also strips the sidebar controls.
			this.hide_sidebar_actions();
			this.discard = true;
			this.clear_page_actions();
			this.toggle_hidden_workspaces(false);
			await this.editor.readOnly.toggle();
			this.is_read_only = true;
			this.sidebar_pages = this.cached_pages;
			this.reload();
			frappe.show_alert({ message: __("Customizations Discarded"), indicator: "info" });
		});

		if (page.name && this.has_access) {
			this.page.add_inner_button(__("Settings"), () => {
				frappe.set_route(`workspace/${page.name}`);
			});
		}
	}

	toggle_hidden_workspaces(show) {
		$(".desk-sidebar").toggleClass("show-hidden-workspaces", show);
	}

	//// Neoffice — upstream is one line, `this.sidebar.find(".standard-sidebar-section")
	//// .addClass("show-control")` + make_sidebar_sortable(). Guarded and re-resolved for the
	//// global / headless sidebar (9999364ec6, then d0268ef91a 2026-06-10).
	show_sidebar_actions() {
		if (!this.sidebar || this.sidebar.headless) return;
		// Get the actual jQuery sidebar element
		const $sidebar = this.sidebar.$sidebar || this.sidebar;
		$sidebar.find(".standard-sidebar-section").addClass("show-control");
		this.make_sidebar_sortable();
	}

	//// Neoffice — added method (0634af137c, 2025-11-13): no upstream equivalent — upstream has no
	//// way back out of edit mode for the sidebar, so drag handles, per-item menus and the Sortable
	//// instance survived the save on a read-only sidebar. Called from setup_pages, Save and
	//// Discard (all marked).
	hide_sidebar_actions() {
		if (!this.sidebar || this.sidebar.headless) return;
		// Get the actual jQuery sidebar element
		const $sidebar = this.sidebar.$sidebar || this.sidebar;
		$sidebar.find(".standard-sidebar-section").removeClass("show-control");

		// Remove all edit controls from the sidebar
		$sidebar.find(".drag-handle").remove();
		$sidebar.find(".setting-btn").remove();
		$sidebar.find(".dropdown-list").remove();

		// Destroy sortable if it exists
		if (this.sidebar_sortable) {
			this.sidebar_sortable.destroy();
			this.sidebar_sortable = null;
		}
	}

	add_sidebar_actions(item, sidebar_control, is_new) {
		if (!item.is_editable) {
			sidebar_control.parent().click(() => {
				!this.is_read_only &&
					frappe.show_alert(
						{
							message: __("Only Workspace Manager can sort or edit this page"),
							indicator: "info",
						},
						5
					);
			});

			frappe.utils.add_custom_button(
				frappe.utils.icon("es-line-duplicate", "sm"),
				() => this.duplicate_page(item),
				"duplicate-page",
				__("Duplicate Workspace"),
				null,
				sidebar_control
			);
		} else if (item.is_hidden) {
			frappe.utils.add_custom_button(
				frappe.utils.icon("es-line-preview", "sm"),
				(e) => this.unhide_workspace(item, e),
				"unhide-workspace-btn",
				__("Unhide Workspace"),
				null,
				sidebar_control
			);
		} else {
			frappe.utils.add_custom_button(
				frappe.utils.icon("es-line-drag", "xs"),
				null,
				"drag-handle",
				__("Drag"),
				null,
				sidebar_control
			);

			!is_new && this.add_settings_button(item, sidebar_control);
		}
	}

	get_parent_pages(page) {
		this.public_parent_pages = [
			"",
			...this.public_pages.filter((p) => !p.parent_page).map((p) => p.title),
		];
		this.private_parent_pages = [
			"",
			...this.private_pages.filter((p) => !p.parent_page).map((p) => p.title),
		];

		if (page) {
			return page.public ? this.public_parent_pages : this.private_parent_pages;
		}
	}

	edit_page(item) {
		var me = this;
		let old_item = item;
		let parent_pages = this.get_parent_pages(item);
		let idx = parent_pages.findIndex((x) => x == item.title);
		if (idx !== -1) parent_pages.splice(idx, 1);
		const d = new frappe.ui.Dialog({
			title: __("Update Details"),
			fields: [
				{
					label: __("Title"),
					fieldtype: "Data",
					fieldname: "title",
					reqd: 1,
					default: item.title,
				},
				{
					label: __("Parent"),
					fieldtype: "Select",
					fieldname: "parent",
					options: parent_pages,
					default: item.parent_page,
				},
				{
					label: __("Public"),
					fieldtype: "Check",
					fieldname: "is_public",
					depends_on: `eval:${this.has_access}`,
					default: item.public,
					onchange: function () {
						d.set_df_property(
							"parent",
							"options",
							this.get_value() ? me.public_parent_pages : me.private_parent_pages
						);
						d.set_df_property("icon", "hidden", this.get_value() ? 0 : 1);
						d.set_df_property("indicator_color", "hidden", this.get_value() ? 1 : 0);
					},
				},
				{
					fieldtype: "Column Break",
				},
				{
					label: __("Icon"),
					fieldtype: "Icon",
					fieldname: "icon",
					default: item.public && item.icon,
					hidden: !item.public,
				},
				{
					label: __("Indicator color"),
					fieldtype: "Select",
					fieldname: "indicator_color",
					options: this.indicator_colors,
					default: !item.public && item.indicator_color,
					hidden: item.public,
				},
			],
			primary_action_label: __("Update"),
			primary_action: (values) => {
				values.title = strip_html(values.title);
				let is_title_changed = values.title != old_item.title;
				let is_section_changed = Boolean(values.is_public) != Boolean(old_item.public);
				if (
					(is_title_changed || is_section_changed) &&
					!this.validate_page(values, old_item)
				)
					return;
				d.hide();

				frappe.call({
					method: "frappe.desk.doctype.workspace.workspace.update_page",
					args: {
						name: old_item.name,
						title: values.title,
						icon: values.icon || "",
						indicator_color: values.indicator_color || "",
						parent: values.parent || "",
						public: values.is_public || 0,
					},
					callback: function (res) {
						if (res.message) {
							let message = __("Workspace {0} Edited Successfully", [
								old_item.title.bold(),
							]);
							frappe.show_alert({ message: message, indicator: "green" });
						}
					},
				});

				this.update_sidebar(old_item, values);

				if (this.make_page_selected) {
					let pre_url = values.is_public ? "" : "private/";
					let route = pre_url + frappe.router.slug(values.title);
					frappe.set_route(route);

					this.make_page_selected = false;
				}

				this.make_sidebar();
				this.show_sidebar_actions();
			},
		});
		d.show();
	}

	update_sidebar(old_item, new_item) {
		let is_section_changed = old_item.public != (new_item.is_public || 0);
		let is_title_changed = old_item.title != new_item.title;
		let new_updated_item = { ...old_item };

		let pages = old_item.public ? this.public_pages : this.private_pages;

		let child_items = pages.filter((page) => page.parent_page == old_item.title);

		this.make_page_selected = old_item.selected;

		new_updated_item.title = new_item.title;
		new_updated_item.icon = new_item.icon;
		new_updated_item.indicator_color = new_item.indicator_color;
		new_updated_item.parent_page = new_item.parent || "";
		new_updated_item.public = new_item.is_public;

		if (is_title_changed || is_section_changed) {
			if (new_item.is_public) {
				new_updated_item.name = new_item.title;
				new_updated_item.label = new_item.title;
				new_updated_item.for_user = "";
			} else {
				let user = frappe.session.user;
				new_updated_item.name = `${new_item.title}-${user}`;
				new_updated_item.label = `${new_item.title}-${user}`;
				new_updated_item.for_user = user;
			}
		}
		this.update_cached_values(old_item, new_updated_item);

		if (child_items.length) {
			child_items.forEach((child) => {
				child.parent_page = new_item.title;
				is_section_changed && this.update_child_sidebar(child, new_item);
			});
		}
	}

	update_child_sidebar(child, new_item) {
		let old_child = { ...child };
		this.make_page_selected = child.selected;

		child.public = new_item.is_public;
		if (new_item.is_public) {
			child.name = child.title;
			child.label = child.title;
			child.for_user = "";
		} else {
			let user = frappe.session.user;
			child.name = `${child.title}-${user}`;
			child.label = `${child.title}-${user}`;
			child.for_user = user;
		}

		this.update_cached_values(old_child, child);
	}

	update_cached_values(old_item, new_item, duplicate, new_page) {
		let [from_pages, to_pages] = old_item.public
			? [this.public_pages, this.private_pages]
			: [this.private_pages, this.public_pages];

		let old_item_index = from_pages.findIndex((page) => page.title == old_item.title);
		duplicate && old_item_index++;

		// update frappe.workspaces
		if (frappe.workspaces[frappe.router.slug(old_item.name)] || new_page) {
			!duplicate && delete frappe.workspaces[frappe.router.slug(old_item.name)];
			if (new_item) {
				frappe.workspaces[frappe.router.slug(new_item.name)] = { title: new_item.title };
			}
		}

		// update page block data
		if ((this.pages && this.pages[old_item.name]) || new_page) {
			if (new_item) {
				this.pages[new_item.name] = this.pages[old_item.name] || {};
			}
			!duplicate && delete this.pages[old_item.name];
		}

		// update public and private pages
		if (new_item) {
			let is_section_changed =
				old_item.public != (new_item.is_public || new_item.public || 0);

			if (is_section_changed) {
				!duplicate && from_pages.splice(old_item_index, 1);
				to_pages.push(new_item);
			} else if (new_page) {
				from_pages.push(new_item);
			} else {
				from_pages.splice(old_item_index, duplicate ? 0 : 1, new_item);
			}
		} else {
			from_pages.splice(old_item_index, 1);
		}

		//// Neoffice — added fallback (0634af137c, 2025-11-13): sidebar_pages is no longer necessarily
		//// populated by this view (the global sidebar owns the fetch), and the line below assigns into
		//// it unconditionally.
		if (!this.sidebar_pages) {
			this.sidebar_pages = { pages: [], has_access: true };
		}
		this.sidebar_pages.pages = [...this.public_pages, ...this.private_pages];
		this.cached_pages = this.sidebar_pages;
	}

	add_settings_button(item, sidebar_control) {
		this.dropdown_list = [
			{
				label: __("Edit"),
				title: __("Edit Workspace"),
				icon: frappe.utils.icon("es-line-edit", "sm"),
				action: () => this.edit_page(item),
			},
			{
				label: __("Duplicate"),
				title: __("Duplicate Workspace"),
				icon: frappe.utils.icon("es-line-duplicate", "sm"),
				action: () => this.duplicate_page(item),
			},
			{
				label: __("Hide"),
				title: __("Hide Workspace"),
				icon: frappe.utils.icon("es-line-hide", "sm"),
				action: (e) => this.hide_workspace(item, e),
			},
		];

		if (this.is_item_deletable(item)) {
			this.dropdown_list.push({
				label: __("Delete"),
				title: __("Delete Workspace"),
				icon: frappe.utils.icon("delete-active", "sm"),
				action: () => this.delete_page(item),
			});
		}

		let $button = $(`
			<div class="btn btn-xs setting-btn dropdown-btn" title="${__("Setting")}">
				${frappe.utils.icon("es-line-dot-horizontal", "xs")}
			</div>
			<div class="dropdown-list hidden"></div>
		`);

		let dropdown_item = function (label, title, icon, action) {
			let html = $(`
				<div class="dropdown-item" title="${title}">
					<span class="dropdown-item-icon">${icon}</span>
					<span class="dropdown-item-label">${label}</span>
				</div>
			`);

			html.click((event) => {
				event.stopPropagation();
				action && action(event);
			});

			return html;
		};

		$button.filter(".dropdown-btn").click((event) => {
			event.stopPropagation();
			if ($button.filter(".dropdown-list.hidden").length) {
				$(".dropdown-list:not(.hidden)").addClass("hidden");
			}
			$button.filter(".dropdown-list").toggleClass("hidden");
		});

		sidebar_control.append($button);

		this.dropdown_list.forEach((i) => {
			$button
				.filter(".dropdown-list")
				.append(dropdown_item(i.label, i.title, i.icon, i.action));
		});
	}

	is_item_deletable(item) {
		// if item is private
		// if item is public but doesn't have module set
		// if item is public and has module set but developer mode is on
		// then item is deletable
		if (
			!item.public ||
			(item.public && (!item.module || (item.module && frappe.boot.developer_mode)))
		)
			return true;
		return false;
	}

	delete_page(page) {
		frappe.confirm(
			__("Are you sure you want to delete page {0}?", [page.title.bold()]),
			() => {
				frappe.call({
					method: "frappe.desk.doctype.workspace.workspace.delete_page",
					args: { page: page },
					callback: function (res) {
						if (res.message) {
							let page = res.message;
							let message = __("Workspace {0} Deleted Successfully", [
								page.title.bold(),
							]);
							frappe.show_alert({ message: message, indicator: "green" });
						}
					},
				});

				this.page.clear_primary_action();
				this.update_cached_values(page);

				if (
					this.current_page.name == page.title &&
					this.current_page.public == page.public
				) {
					frappe.set_route("/");
				}

				this.make_sidebar();
				this.show_sidebar_actions();
			}
		);
	}

	duplicate_page(page) {
		var me = this;
		let new_page = { ...page };
		if (!this.has_access && new_page.public) {
			new_page.public = 0;
		}
		let parent_pages = this.get_parent_pages({ public: new_page.public });
		const d = new frappe.ui.Dialog({
			title: __("Create Duplicate"),
			fields: [
				{
					label: __("Title"),
					fieldtype: "Data",
					fieldname: "title",
					reqd: 1,
				},
				{
					label: __("Parent"),
					fieldtype: "Select",
					fieldname: "parent",
					options: parent_pages,
					default: new_page.parent_page,
				},
				{
					label: __("Public"),
					fieldtype: "Check",
					fieldname: "is_public",
					depends_on: `eval:${this.has_access}`,
					default: new_page.public,
					onchange: function () {
						d.set_df_property(
							"parent",
							"options",
							this.get_value() ? me.public_parent_pages : me.private_parent_pages
						);
						d.set_df_property("icon", "hidden", this.get_value() ? 0 : 1);
						d.set_df_property("indicator_color", "hidden", this.get_value() ? 1 : 0);
					},
				},
				{
					fieldtype: "Column Break",
				},
				{
					label: __("Icon"),
					fieldtype: "Icon",
					fieldname: "icon",
					default: new_page.public && new_page.icon,
					hidden: !new_page.public,
				},
				{
					label: __("Indicator color"),
					fieldtype: "Select",
					fieldname: "indicator_color",
					options: this.indicator_colors,
					hidden: new_page.public,
					default: !new_page.public && new_page.indicator_color,
				},
			],
			primary_action_label: __("Duplicate"),
			primary_action: (values) => {
				if (!this.validate_page(values)) return;
				d.hide();
				frappe.call({
					method: "frappe.desk.doctype.workspace.workspace.duplicate_page",
					args: {
						page_name: page.name,
						new_page: values,
					},
					callback: function (res) {
						if (res.message) {
							let new_page = res.message;
							let message = __(
								"Duplicate of {0} named as {1} is created successfully",
								[page.title.bold(), new_page.title.bold()]
							);
							frappe.show_alert({ message: message, indicator: "green" });
						}
					},
				});

				new_page.title = values.title;
				new_page.public = values.is_public || 0;
				new_page.name = values.title + (new_page.public ? "" : "-" + frappe.session.user);
				new_page.label = new_page.name;
				new_page.icon = values.icon;
				new_page.indicator_color = values.indicator_color;
				new_page.parent_page = values.parent || "";
				new_page.for_user = new_page.public ? "" : frappe.session.user;
				new_page.is_editable = !new_page.public;
				new_page.selected = true;

				this.update_cached_values(page, new_page, true);

				let pre_url = values.is_public ? "" : "private/";
				let route = pre_url + frappe.router.slug(values.title);
				frappe.set_route(route);

				me.make_sidebar();
				me.show_sidebar_actions();
			},
		});
		d.show();
	}

	hide_unhide_workspace(page, event, hide) {
		page.is_hidden = hide;

		let sidebar_control = event.target.closest(".sidebar-item-control");
		let sidebar_item_container = sidebar_control.closest(".sidebar-item-container");
		$(sidebar_item_container).attr("item-is-hidden", hide);

		$(sidebar_control).empty();
		this.add_sidebar_actions(page, $(sidebar_control));

		this.add_drop_icon(page, $(sidebar_control), $(sidebar_item_container));

		let cached_page = this.cached_pages.pages.findIndex((p) => p.name === page.name);
		if (cached_page !== -1) {
			this.cached_pages.pages[cached_page].is_hidden = hide;
		}

		let method = hide ? "hide_page" : "unhide_page";
		frappe.call({
			method: "frappe.desk.doctype.workspace.workspace." + method,
			args: {
				page_name: page.name,
			},
			callback: (r) => {
				if (!r.message) return;

				let message = hide ? "{0} is hidden successfully" : "{0} is unhidden successfully";
				message = __(message, [page.title.bold()]);
				frappe.show_alert({ message: message, indicator: "green" });
			},
		});
	}

	hide_workspace(page, event) {
		this.hide_unhide_workspace(page, event, 1);
	}

	unhide_workspace(page, event) {
		this.hide_unhide_workspace(page, event, 0);
	}

	make_sidebar_sortable() {
		let me = this;
		$(".nested-container").each(function () {
			new Sortable(this, {
				handle: ".drag-handle",
				draggable: ".sidebar-item-container.is-draggable",
				group: "nested",
				animation: 150,
				fallbackOnBody: true,
				swapThreshold: 0.65,
				onEnd: function (evt) {
					let is_public = $(evt.item).attr("item-public") == "1";
					me.prepare_sorted_sidebar(is_public);
					me.update_sorted_sidebar();
				},
			});
		});
	}

	//// Neoffice — same global/headless sidebar resolution as make_sidebar (9999364ec6 +
	//// d0268ef91a): upstream calls this.sidebar.find(...) directly on the two lines below.
	prepare_sorted_sidebar(is_public) {
		if (!this.sidebar || this.sidebar.headless) return;
		// Get the actual jQuery sidebar element
		const $sidebar = this.sidebar.$sidebar || this.sidebar;

		let pages = is_public ? this.public_pages : this.private_pages;
		if (is_public) {
			this.sorted_public_items = this.sort_sidebar(
				$sidebar.find(".standard-sidebar-section").last(),
				pages
			);
		} else {
			this.sorted_private_items = this.sort_sidebar(
				$sidebar.find(".standard-sidebar-section").first(),
				pages
			);
		}

		this.sidebar_pages.pages = [...this.public_pages, ...this.private_pages];
		this.cached_pages = this.sidebar_pages;
	}

	sort_sidebar($sidebar_section, pages) {
		let sorted_items = [];
		Array.from($sidebar_section.find(".sidebar-item-container")).forEach((page, i) => {
			let parent_page = "";

			if (page.closest(".nested-container").classList.contains("sidebar-child-item")) {
				parent_page = page.parentElement.parentElement.attributes["item-name"].value;
			}

			sorted_items.push({
				title: page.attributes["item-name"].value,
				parent_page: parent_page,
				public: page.attributes["item-public"].value,
			});

			let $drop_icon = $(page).find(".sidebar-item-control .drop-icon").first();
			if ($(page).find(".sidebar-child-item > *").length != 0) {
				$drop_icon.removeClass("hidden");
			} else {
				$drop_icon.addClass("hidden");
			}

			let from_index = pages.findIndex((p) => p.title == page.attributes["item-name"].value);
			let element = pages[from_index];
			element.parent_page = parent_page;
			if (from_index != i) {
				pages.splice(from_index, 1);
				pages.splice(i, 0, element);
			}
		});
		return sorted_items;
	}

	update_sorted_sidebar() {
		if (this.sorted_public_items || this.sorted_private_items) {
			frappe.call({
				method: "frappe.desk.doctype.workspace.workspace.sort_pages",
				args: {
					sb_public_items: this.sorted_public_items,
					sb_private_items: this.sorted_private_items,
				},
				callback: function (res) {
					if (res.message) {
						let message = `Sidebar Updated Successfully`;
						frappe.show_alert({ message: __(message), indicator: "green" });
					}
				},
			});
		}
	}

	make_blocks_sortable() {
		let me = this;
		this.page_sortable = Sortable.create(
			this.page.main.find(".codex-editor__redactor").get(0),
			{
				handle: ".drag-handle",
				draggable: ".ce-block",
				animation: 150,
				onEnd: function (evt) {
					me.editor.blocks.move(evt.newIndex, evt.oldIndex);
				},
				setData: function () {
					//Do Nothing
				},
			}
		);
	}

	initialize_new_page() {
		var me = this;
		this.get_parent_pages();
		const d = new frappe.ui.Dialog({
			title: __("New Workspace"),
			fields: [
				{
					label: __("Title"),
					fieldtype: "Data",
					fieldname: "title",
					reqd: 1,
				},
				{
					label: __("Parent"),
					fieldtype: "Select",
					fieldname: "parent",
					options: this.private_parent_pages,
				},
				{
					label: __("Public"),
					fieldtype: "Check",
					fieldname: "is_public",
					depends_on: `eval:${this.has_access}`,
					onchange: function () {
						d.set_df_property(
							"parent",
							"options",
							this.get_value() ? me.public_parent_pages : me.private_parent_pages
						);
						d.set_df_property("icon", "hidden", this.get_value() ? 0 : 1);
						d.set_df_property("indicator_color", "hidden", this.get_value() ? 1 : 0);
					},
				},
				{
					fieldtype: "Column Break",
				},
				{
					label: __("Icon"),
					fieldtype: "Icon",
					fieldname: "icon",
					hidden: 1,
				},
				{
					label: __("Indicator color"),
					fieldtype: "Select",
					fieldname: "indicator_color",
					options: this.indicator_colors,
				},
			],
			primary_action_label: __("Create"),
			primary_action: (values) => {
				values.title = strip_html(values.title);
				if (!this.validate_page(values)) return;
				d.hide();
				this.initialize_editorjs_undo();
				this.setup_customization_buttons({ is_editable: true });

				let name = values.title + (values.is_public ? "" : "-" + frappe.session.user);
				let blocks = [
					{
						type: "header",
						data: { text: values.title },
					},
				];

				let new_page = {
					content: JSON.stringify(blocks),
					name: name,
					label: name,
					title: values.title,
					public: values.is_public || 0,
					for_user: values.is_public ? "" : frappe.session.user,
					icon: values.icon,
					indicator_color: values.indicator_color,
					parent_page: values.parent || "",
					is_editable: true,
					selected: true,
				};

				this.editor
					.render({
						blocks: blocks,
					})
					.then(async () => {
						if (this.editor.configuration.readOnly) {
							this.is_read_only = false;
							await this.editor.readOnly.toggle();
						}

						frappe.call({
							method: "frappe.desk.doctype.workspace.workspace.new_page",
							args: {
								new_page: new_page,
							},
							callback: function (res) {
								if (res.message) {
									let message = __("Workspace {0} Created Successfully", [
										new_page.title.bold(),
									]);
									frappe.show_alert({
										message: message,
										indicator: "green",
									});
								}
							},
						});

						this.update_cached_values(new_page, new_page, true, true);

						let pre_url = new_page.public ? "" : "private/";
						let route = pre_url + frappe.router.slug(new_page.title);
						frappe.set_route(route);

						this.make_sidebar();
						this.show_sidebar_actions();
						localStorage.setItem("new_workspace", JSON.stringify(new_page));
					});
			},
		});
		d.show();
	}

	validate_page(new_page, old_page) {
		let message = "";
		let [from_pages, to_pages] = new_page.is_public
			? [this.private_pages, this.public_pages]
			: [this.public_pages, this.private_pages];

		let section = this.sidebar_categories[new_page.is_public];

		if (to_pages && to_pages.filter((p) => p.title == new_page.title)[0]) {
			message = __("Page with title {0} already exist.", [new_page.title.bold()]);
		}

		if (frappe.router.doctype_route_exist(frappe.router.slug(new_page.title))) {
			message = __("Doctype with same route already exist. Please choose different title.");
		}

		let child_pages = old_page && from_pages.filter((p) => p.parent_page == old_page.title);
		if (child_pages) {
			child_pages.every((child_page) => {
				if (to_pages && to_pages.find((p) => p.title == child_page.title)) {
					message = __(
						"One of the child page with name {0} already exist in {1} Section. Please update the name of the child page first before moving",
						[child_page.title.bold(), section.bold()]
					);
					cur_dialog.hide();
					return false;
				}
				return true;
			});
		}

		if (message) {
			frappe.throw(__(message));
			return false;
		}
		return true;
	}

	add_page_to_sidebar(page) {
		let $sidebar = $(".standard-sidebar-section");
		let item = { ...page };

		item.selected = true;
		item.is_editable = true;

		let $sidebar_item = this.sidebar_item_container(item);

		this.add_sidebar_actions(item, $sidebar_item.find(".sidebar-item-control"), true);

		$sidebar_item.find(".sidebar-item-control .drag-handle").css("margin-right", "8px");

		let sidebar_section = item.is_public ? $sidebar[1] : $sidebar[0];

		if (!item.parent) {
			!item.is_public && $sidebar.first().removeClass("hidden");
			$sidebar_item.appendTo(sidebar_section);
		} else {
			let $item_container = $(sidebar_section).find(`[item-name="${item.parent}"]`);
			let $child_section = $item_container.find(".sidebar-child-item");
			let $drop_icon = $item_container.find(".drop-icon");
			if (!$child_section[0]) {
				$child_section = $(
					`<div class="sidebar-child-item hidden nested-container"></div>`
				).appendTo($item_container);
				$drop_icon.toggleClass("hidden");
			}
			$sidebar_item.appendTo($child_section);
			$child_section.removeClass("hidden");
			$item_container.find(".drop-icon.hidden").removeClass("hidden");
			$item_container.find(".drop-icon use").attr("href", "#es-line-up");
		}

		let section = item.is_public ? "public" : "private";
		if (
			this.sidebar_items &&
			this.sidebar_items[section] &&
			!this.sidebar_items[section][item.title]
		) {
			this.sidebar_items[section][item.title] = $sidebar_item;
		}
	}

	initialize_editorjs(blocks) {
		this.tools = {
			header: {
				class: this.blocks["header"],
				inlineToolbar: ["HeaderSize", "bold", "italic", "link"],
				config: {
					default_size: 4,
				},
			},
			paragraph: {
				class: this.blocks["paragraph"],
				inlineToolbar: ["HeaderSize", "bold", "italic", "link"],
				config: {
					placeholder: __("Choose a block or continue typing"),
				},
			},
			chart: {
				class: this.blocks["chart"],
				config: {
					page_data: this.page_data || [],
				},
			},
			card: {
				class: this.blocks["card"],
				config: {
					page_data: this.page_data || [],
				},
			},
			shortcut: {
				class: this.blocks["shortcut"],
				config: {
					page_data: this.page_data || [],
				},
			},
			onboarding: {
				class: this.blocks["onboarding"],
				config: {
					page_data: this.page_data || [],
				},
			},
			quick_list: {
				class: this.blocks["quick_list"],
				config: {
					page_data: this.page_data || [],
				},
			},
			number_card: {
				class: this.blocks["number_card"],
				config: {
					page_data: this.page_data || [],
				},
			},
			custom_block: {
				class: this.blocks["custom_block"],
				config: {
					page_data: this.page_data || [],
				},
			},
			spacer: this.blocks["spacer"],
			HeaderSize: frappe.workspace_block.tunes["header_size"],
		};

		this.editor = new EditorJS({
			data: {
				blocks: blocks || [],
			},
			tools: this.tools,
			autofocus: false,
			readOnly: true,
			logLevel: "ERROR",
		});
	}

	save_page(page) {
		let me = this;
		//// Neoffice — upstream: `{ name: page.title, ... }`. Keyed by NAME (9999364ec6 / 0634af137c),
		//// like get_page_to_show and show_page above — the title is not an identifier.
		this.current_page = { name: page.name, public: page.public };

		return this.editor
			.save()
			.then((outputData) => {
				let new_widgets = {};

				outputData.blocks.forEach((item) => {
					if (item.data.new) {
						if (!new_widgets[item.type]) {
							new_widgets[item.type] = [];
						}
						new_widgets[item.type].push(item.data.new);
						delete item.data["new"];
					}
				});

				let blocks = outputData.blocks.filter(
					(item) =>
						item.type != "card" ||
						(item.data.card_name !== "Custom Documents" &&
							item.data.card_name !== "Custom Reports")
				);

				if (
					page.content == JSON.stringify(blocks) &&
					Object.keys(new_widgets).length === 0
				) {
					this.setup_customization_buttons(page);
					frappe.show_alert({
						message: __("No changes made on the page"),
						indicator: "warning",
					});
					return false;
				}

				this.create_page_skeleton();
				page.content = JSON.stringify(blocks);
				frappe.call({
					method: "frappe.desk.doctype.workspace.workspace.save_page",
					args: {
						//// Neoffice — two changes in this call. `title: page.name` (9999364ec6 / 0634af137c): the
						//// server's save_page takes the workspace identifier, and upstream sent the display title.
						//// `deleted_widgets` (07ee48dd37, 2026-06-16, added below): the list of widgets the user
						//// explicitly deleted, so the server can drop those child rows before clean_up() re-adds them
						//// (see workspace/blocks/block.js record_widget_deletion, marked there).
						title: page.name,
						public: page.public || 0,
						new_widgets: new_widgets,
						blocks: JSON.stringify(blocks),
						deleted_widgets: JSON.stringify(frappe.workspace_deleted_widgets || {}),
					},
					callback: function (res) {
						if (res.message) {
							me.discard = true;
							me.update_cached_values(page, page);
							me.reload();
							frappe.show_alert({
								message: __("Page Saved Successfully"),
								indicator: "green",
							});
						}
					},
				});
				return true;
			})
			.catch((error) => {
				error;
				// console.log('Saving failed: ', error);
			});
	}

	reload() {
		//// Neoffice — reload(): the deleted-widget list is cleared for the next edit session
		//// (07ee48dd37, 2026-06-16), and `this.discard = false` MOVED above setup_pages (0634af137c,
		//// 2025-11-13 — upstream sets it after, so setup_pages still saw discard=true and skipped
		//// get_pages(), reloading nothing; the trailing comment is the original author's).
		frappe.workspace_deleted_widgets = {};
		this.sorted_public_items = [];
		this.sorted_private_items = [];
		this.discard = false;  // IMPORTANT: Set discard to false BEFORE setup_pages() so get_pages() is called
		this.setup_pages(true);
		this.undo.readOnly = true;
	}

	create_page_skeleton() {
		if (this.body.find(".workspace-skeleton").length) return;

		this.body.prepend(frappe.render_template("workspace_loading_skeleton"));
		this.body.find(".codex-editor").addClass("hidden");
	}

	remove_page_skeleton() {
		this.body.find(".codex-editor").removeClass("hidden");
		this.body.find(".workspace-skeleton").remove();
	}

	create_sidebar_skeleton() {
		//// Neoffice — added guard + $sidebar resolution (d0268ef91a, 2026-06-10 / 9999364ec6): there is
		//// no sidebar DOM to put a loading skeleton in front of under the NeoCockpit chrome, and
		//// upstream inserts it relative to this.sidebar as a jQuery set.
		if (this.sidebar?.headless) return;
		if ($(".workspace-sidebar-skeleton").length) return;

		const $sidebar = this.sidebar.$sidebar || this.sidebar;
		$(frappe.render_template("workspace_sidebar_loading_skeleton")).insertBefore($sidebar);
		$sidebar.addClass("hidden");
	}

	remove_sidebar_skeleton() {
		//// Neoffice — the mirror of create_sidebar_skeleton above (d0268ef91a / 9999364ec6).
		if (this.sidebar?.headless) return;
		const $sidebar = this.sidebar.$sidebar || this.sidebar;
		$sidebar.removeClass("hidden");
		$(".workspace-sidebar-skeleton").remove();
	}

	register_awesomebar_shortcut() {
		"abcdefghijklmnopqrstuvwxyz".split("").forEach((letter) => {
			const default_shortcut = {
				action: (e) => {
					$("#navbar-search").focus();
					return false; // don't prevent default = type the letter in awesomebar
				},
				page: this.page,
			};
			frappe.ui.keys.add_shortcut({ shortcut: letter, ...default_shortcut });
			frappe.ui.keys.add_shortcut({ shortcut: `shift+${letter}`, ...default_shortcut });
		});
	}
};
