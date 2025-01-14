// Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt
import ListFilter from "./list_filter";
frappe.provide("frappe.views");

// opts:
// stats = list of fields
// doctype
// parent

frappe.views.ListSidebar = class ListSidebar {
	constructor(opts) {
		$.extend(this, opts);
		this.make();
	}

	make() {
		var sidebar_content = frappe.render_template("list_sidebar", { doctype: this.doctype });

		this.sidebar = $('<div class="list-sidebar overlay-sidebar hidden-xs hidden-sm"></div>')
			.html(sidebar_content)
			.appendTo(this.page.sidebar.empty());

		this.setup_list_filter();
		this.setup_list_group_by();

		// do not remove
		// used to trigger custom scripts
		$(document).trigger("list_sidebar_setup");

		if (
			this.list_view.list_view_settings &&
			this.list_view.list_view_settings.disable_sidebar_stats
		) {
			this.sidebar.find(".list-tags").remove();
		} else {
			this.sidebar.find(".list-stats").on("show.bs.dropdown", (e) => {
				this.reload_stats();
			});
		}

		this.make_sidebar_menu(this.sidebar); //// added
		/* ////
		if (frappe.user.has_role("System Manager")) {
			this.add_insights_banner();
		}
		*/
	}

	setup_views() {
		var show_list_link = false;

		if (frappe.views.calendar[this.doctype]) {
			this.sidebar.find('.list-link[data-view="Calendar"]').removeClass("hide");
			this.sidebar.find('.list-link[data-view="Gantt"]').removeClass("hide");
			show_list_link = true;
		}
		//show link for kanban view
		this.sidebar.find('.list-link[data-view="Kanban"]').removeClass("hide");
		if (this.doctype === "Communication" && frappe.boot.email_accounts.length) {
			this.sidebar.find('.list-link[data-view="Inbox"]').removeClass("hide");
			show_list_link = true;
		}

		if (frappe.treeview_settings[this.doctype] || frappe.get_meta(this.doctype).is_tree) {
			this.sidebar.find(".tree-link").removeClass("hide");
		}

		this.current_view = "List";
		var route = frappe.get_route();
		if (route.length > 2 && frappe.views.view_modes.includes(route[2])) {
			this.current_view = route[2];

			if (this.current_view === "Kanban") {
				this.kanban_board = route[3];
			} else if (this.current_view === "Inbox") {
				this.email_account = route[3];
			}
		}

		// disable link for current view
		this.sidebar
			.find('.list-link[data-view="' + this.current_view + '"] a')
			.attr("disabled", "disabled")
			.addClass("disabled");

		//enable link for Kanban view
		this.sidebar
			.find('.list-link[data-view="Kanban"] a, .list-link[data-view="Inbox"] a')
			.attr("disabled", null)
			.removeClass("disabled");

		// show image link if image_view
		if (this.list_view.meta.image_field) {
			this.sidebar.find('.list-link[data-view="Image"]').removeClass("hide");
			show_list_link = true;
		}

		if (
			this.list_view.settings.get_coords_method ||
			(this.list_view.meta.fields.find((i) => i.fieldname === "latitude") &&
				this.list_view.meta.fields.find((i) => i.fieldname === "longitude")) ||
			this.list_view.meta.fields.find(
				(i) => i.fieldname === "location" && i.fieldtype == "Geolocation"
			)
		) {
			this.sidebar.find('.list-link[data-view="Map"]').removeClass("hide");
			show_list_link = true;
		}

		if (show_list_link) {
			this.sidebar.find('.list-link[data-view="List"]').removeClass("hide");
		}
	}

	setup_reports() {
		// add reports linked to this doctype to the dropdown
		var me = this;
		var added = [];
		var dropdown = this.page.sidebar.find(".reports-dropdown");
		var divider = false;

		var add_reports = function (reports) {
			$.each(reports, function (name, r) {
				if (!r.ref_doctype || r.ref_doctype == me.doctype) {
					var report_type =
						r.report_type === "Report Builder"
							? `List/${r.ref_doctype}/Report`
							: "query-report";

					var route = r.route || report_type + "/" + (r.title || r.name);

					if (added.indexOf(route) === -1) {
						// don't repeat
						added.push(route);

						if (!divider) {
							me.get_divider().appendTo(dropdown);
							divider = true;
						}

						$(
							'<li><a href="#' + route + '">' + __(r.title || r.name) + "</a></li>"
						).appendTo(dropdown);
					}
				}
			});
		};

		// from reference doctype
		if (this.list_view.settings.reports) {
			add_reports(this.list_view.settings.reports);
		}

		// Sort reports alphabetically
		var reports =
			Object.values(frappe.boot.user.all_reports).sort((a, b) =>
				a.title.localeCompare(b.title)
			) || [];

		// from specially tagged reports
		add_reports(reports);
	}

	setup_list_filter() {
		this.list_filter = new ListFilter({
			wrapper: this.page.sidebar.find(".list-filters"),
			doctype: this.doctype,
			list_view: this.list_view,
		});
	}

	setup_kanban_boards() {
		const $dropdown = this.page.sidebar.find(".kanban-dropdown");
		frappe.views.KanbanView.setup_dropdown_in_sidebar(this.doctype, $dropdown);
	}

	setup_keyboard_shortcuts() {
		this.sidebar.find(".list-link > a, .list-link > .btn-group > a").each((i, el) => {
			frappe.ui.keys.get_shortcut_group(this.page).add($(el));
		});
	}

	setup_list_group_by() {
		this.list_group_by = new frappe.views.ListGroupBy({
			doctype: this.doctype,
			sidebar: this,
			list_view: this.list_view,
			page: this.page,
		});
	}

	get_stats() {
		var me = this;

		let dropdown_options = me.sidebar.find(".list-stats-dropdown .stat-result");
		this.set_loading_state(dropdown_options);

		frappe.call({
			method: "frappe.desk.reportview.get_sidebar_stats",
			type: "GET",
			args: {
				stats: me.stats,
				doctype: me.doctype,
				// wait for list filter area to be generated before getting filters, or fallback to default filters
				filters:
					(me.list_view.filter_area
						? me.list_view.get_filters_for_args()
						: me.default_filters) || [],
			},
			callback: function (r) {
				let stats = (r.message.stats || {})["_user_tags"] || [];
				me.render_stat(stats);
				let stats_dropdown = me.sidebar.find(".list-stats-dropdown");
				frappe.utils.setup_search(stats_dropdown, ".stat-link", ".stat-label");
			},
		});
	}

	set_loading_state(dropdown) {
		dropdown.html(`<li>
			<div class="empty-state">
				${__("Loading...")}
			</div>
		</li>`);
	}

	render_stat(stats) {
		let args = {
			stats: stats,
			label: __("Tags"),
		};

		let tag_list = $(frappe.render_template("list_sidebar_stat", args)).on(
			"click",
			".stat-link",
			(e) => {
				let fieldname = $(e.currentTarget).attr("data-field");
				let label = $(e.currentTarget).attr("data-label");
				let condition = "like";
				let existing = this.list_view.filter_area.filter_list.get_filter(fieldname);
				if (existing) {
					existing.remove();
				}
				if (label == "No Tags") {
					label = "%,%";
					condition = "not like";
				}
				this.list_view.filter_area.add(this.doctype, fieldname, condition, label);
			}
		);

		this.sidebar.find(".list-stats-dropdown .stat-result").html(tag_list);
	}

	reload_stats() {
		this.sidebar.find(".stat-link").remove();
		this.sidebar.find(".stat-no-records").remove();
		this.get_stats();
	}

	add_insights_banner() {
		return; //// added
		try {
			if (this.list_view.view != "Report") {
				return;
			}

			if (localStorage.getItem("show_insights_banner") == "false") {
				return;
			}

			if (this.insights_banner) {
				this.insights_banner.remove();
			}

			const message = __("Get more insights with");
			const link = "https://frappe.io/s/insights";
			const cta = __("Frappe Insights");

			this.insights_banner = $(`
				<div style="position: relative;">
					<div class="pr-3">
						${message} <a href="${link}" target="_blank" style="color: var(--text-color)">${cta} &rarr; </a>
					</div>
					<div style="position: absolute; top: -1px; right: -4px; cursor: pointer;" title="Dismiss"
						onclick="localStorage.setItem('show_insights_banner', 'false') || this.parentElement.remove()">
						<svg class="icon  icon-sm" style="">
							<use class="" href="#icon-close"></use>
						</svg>
					</div>
				</div>
			`).appendTo(this.sidebar);
		} catch (error) {
			console.error(error);
		}
	}

	//// added function
	make_sidebar_menu(sidebar) {
		const sidebar_item_container = (item) => {
			const link = item.custom_link || (item.public ? frappe.router.slug(item.title) : "private/" + frappe.router.slug(item.title));
			return `
		  <div class="sidebar-item-container ${item.is_editable ? "is-draggable" : ""}" data-parent="${item.parent_page}" data-name="${item.title}" data-public="${item.public || 0}">
			<div class="desk-sidebar-item standard-sidebar-item ${item.selected ? "selected" : ""}">
			  <a href="/app/${link}" class="item-anchor ${item.is_editable ? "" : "block-click"}" title="${__(item.title)}">
				<span class="sidebar-item-icon" data-icon=${item.icon || "folder-normal"}>${frappe.utils.icon(item.icon || "folder-normal", "md")}</span>
				<span class="sidebar-item-label">${__(item.title)}<span>
			  </a>
			  <div class="sidebar-item-control"></div>
			</div>
			<div class="sidebar-child-item nested-container hidden"></div>
		  </div>`;
		};

		frappe.call({
			method: "frappe.desk.desktop.get_workspace_sidebar_items",
			callback: function (r) {
				const pages = r.message.pages;
				let html_sidebar_menu = '';
				pages.forEach(element => {
					html_sidebar_menu += sidebar_item_container(element);
				});
				$(sidebar).append(`<div class="desk-sidebar list-unstyled sidebar-menu"><div class="standard-sidebar-section nested-container" data-title="Public">${html_sidebar_menu}</div></div>`);

				$(sidebar).prepend('<button type="button" class="collapsible_btn"><span class="search-icon"><svg class="icon icon-md"><use xlink:href="#icon-search"></use></svg></span>' + __("Filter by") + '</button>');
				$(sidebar).find("button.collapsible_btn").on("click", function() {
					const content = $(sidebar).find('ul.sidebar-menu').last();
					if ($(content).css("display") === "block") {
						$(this).removeClass("active");
						$(content).css("display", "none");
					} else {
						$(this).addClass("active");
						$(content).css("display", "block");
					}
				});

				const $sidebarSections = $(sidebar).find('.standard-sidebar-section').not(".hidden");
				const $labelItems = $sidebarSections.children().not(".standard-sidebar-label");

				$labelItems.each(function () {
					const $currentElement = $(this);
					const itemname = $currentElement.data("name");
					const itemparent = $currentElement.data("parent");
					const $selectoritemname = $sidebarSections.find(`[data-name="${itemname}"].sidebar-item-container`);

					if (itemparent) {
						const $selectoritemparent = $sidebarSections.find(`[data-name="${itemparent}"].sidebar-item-container`);
						const $selectoritemparentcontent = $selectoritemparent.children('.sidebar-child-item.nested-container');
						const $selectoritemparentbtn = $selectoritemparent.find('.desk-sidebar-item > .sidebar-item-control');

						if ($selectoritemparentbtn.find('.drop-icon').length == 0) {
							const itemparentbtn = `<span class="drop-icon">${frappe.utils.icon("es-line-down", "sm")}</span>`;
							$selectoritemparentbtn.append(itemparentbtn);
						}
						$selectoritemname.appendTo($selectoritemparentcontent);
						$selectoritemparentcontent.addClass("hidden");
					}
				});

				$labelItems.find(".drop-icon").on("click", (e) => {
					const $drop_icon = $(e.target);
					const itemname = $drop_icon.parents(".sidebar-item-container").data("name");

					const $parentContainer = $drop_icon.parents(".sidebar-item-container");
					const $nestedContainer = $parentContainer.find(".sidebar-child-item.nested-container");
					let existingArray = JSON.parse(localStorage.getItem("list_sidebar_open") || '[]');
					let icon =
						$drop_icon.find("use").attr("href") === "#es-line-down"
							? "#es-line-up"
							: "#es-line-down";
					$drop_icon.find("use").attr("href", icon);
					$nestedContainer.toggleClass("hidden");
					//// Save state to local storage
					if($drop_icon.find("use").attr("href") === "#es-line-down") {
						if (existingArray.includes(itemname)) {
							existingArray.splice(existingArray.indexOf(itemname), 1);
							localStorage.setItem("list_sidebar_open", JSON.stringify(existingArray));
						}
						//localStorage.setItem(itemname, "closed");
					} else {
						if (!existingArray.includes(itemname)) {
							existingArray.push(itemname);
							localStorage.setItem("list_sidebar_open", JSON.stringify(existingArray));
						}
						//localStorage.setItem(itemname, "open");
					}
				});
			}
		});
	}
};
