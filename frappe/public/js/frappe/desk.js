// Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt
/* eslint-disable no-console */

// __('Modules') __('Domains') __('Places') __('Administration') # for translation, don't remove

frappe.start_app = function () {
	if (!frappe.Application) return;
	frappe.assets.check();
	frappe.provide("frappe.app");
	frappe.provide("frappe.desk");
	frappe.app = new frappe.Application();
};

$(document).ready(function () {
	if (!frappe.utils.supportsES6) {
		frappe.msgprint({
			indicator: "red",
			title: __("Browser not supported"),
			message: __(
				"Some of the features might not work in your browser. Please update your browser to the latest version."
			),
		});
	}
	frappe.start_app();
});

frappe.Application = class Application {
	constructor() {
		this.startup();
	}

	startup() {
		frappe.realtime.init();
		frappe.model.init();

		this.load_bootinfo();
		this.load_user_permissions();
		this.make_chrome();
		this.set_favicon();
		this.set_fullwidth_if_enabled();
		this.add_browser_class();
		this.setup_energy_point_listeners();
		this.setup_copy_doc_listener();
		this.setup_broadcast_listeners();
		this.setup_overlay_watchdog(); //// Neoffice — see setup_overlay_watchdog()

		frappe.ui.keys.setup();

		frappe.ui.keys.add_shortcut({
			shortcut: "shift+ctrl+g",
			description: __("Switch Theme"),
			action: () => {
				if (frappe.theme_switcher && frappe.theme_switcher.dialog.is_visible) {
					frappe.theme_switcher.hide();
				} else {
					frappe.theme_switcher = new frappe.ui.ThemeSwitcher();
					frappe.theme_switcher.show();
				}
			},
		});

		frappe.ui.add_system_theme_switch_listener();
		const root = document.documentElement;

		const observer = new MutationObserver(() => {
			frappe.ui.set_theme();
		});
		observer.observe(root, {
			attributes: true,
			attributeFilter: ["data-theme-mode"],
		});

		frappe.ui.set_theme();

		// page container
		this.make_page_container();
		// Onboarding tours intentionally disabled for Neoffice — the new
		// home workspace already provides a guided welcome (hero + tinted
		// applications), so the legacy "Awesomebar / Form basics / ..."
		// driver.js popovers only get in the way for new users.
		this.set_route();

		// trigger app startup
		$(document).trigger("startup");

		$(document).trigger("app_ready");

		if (frappe.boot.messages) {
			frappe.msgprint(frappe.boot.messages);
		}

		if (frappe.user_roles.includes("System Manager")) {
			// delayed following requests to make boot faster
			setTimeout(() => {
				this.show_change_log();
				this.show_update_available();
			}, 1000);
		}

		if (!frappe.boot.developer_mode) {
			let console_security_message = __(
				"Using this console may allow attackers to impersonate you and steal your information. Do not enter or paste code that you do not understand."
			);
			console.log(`%c${console_security_message}`, "font-size: large");
		}

		this.show_notes();

		if (frappe.ui.startup_setup_dialog && !frappe.boot.setup_complete) {
			frappe.ui.startup_setup_dialog.pre_show();
			frappe.ui.startup_setup_dialog.show();
		}

		frappe.realtime.on("version-update", function () {
			var dialog = frappe.msgprint({
				message: __(
					"The application has been updated to a new version, please refresh this page"
				),
				indicator: "green",
				title: __("Version Updated"),
			});
			dialog.set_primary_action(__("Refresh"), function () {
				location.reload(true);
			});
			dialog.get_close_btn().toggle(false);
		});

		// listen to build errors
		this.setup_build_events();

		if (frappe.sys_defaults.email_user_password) {
			var email_list = frappe.sys_defaults.email_user_password.split(",");
			for (var u in email_list) {
				if (email_list[u] === frappe.user.name) {
					this.set_password(email_list[u]);
				}
			}
		}

		// REDESIGN-TODO: Fix preview popovers
		this.link_preview = new frappe.ui.LinkPreview();

		frappe.broadcast.emit("boot", {
			csrf_token: frappe.csrf_token,
			user: frappe.session.user,
		});
	}

	set_route() {
		if (frappe.boot && localStorage.getItem("session_last_route")) {
			frappe.set_route(localStorage.getItem("session_last_route"));
			localStorage.removeItem("session_last_route");
		} else {
			// route to home page
			frappe.router.route();
		}
		frappe.router.on("change", () => {
			$(".tooltip").hide();
		});
	}

	set_password(user) {
		var me = this;
		frappe.call({
			method: "frappe.core.doctype.user.user.get_email_awaiting",
			args: {
				user: user,
			},
			callback: function (email_account) {
				email_account = email_account["message"];
				if (email_account) {
					var i = 0;
					if (i < email_account.length) {
						me.email_password_prompt(email_account, user, i);
					}
				}
			},
		});
	}

	email_password_prompt(email_account, user, i) {
		var me = this;
		const email_id = email_account[i]["email_id"];
		let d = new frappe.ui.Dialog({
			title: __("Password missing in Email Account"),
			fields: [
				{
					fieldname: "password",
					fieldtype: "Password",
					label: __(
						"Please enter the password for: <b>{0}</b>",
						[email_id],
						"Email Account"
					),
					reqd: 1,
				},
				{
					fieldname: "submit",
					fieldtype: "Button",
					label: __("Submit", null, "Submit password for Email Account"),
				},
			],
		});
		d.get_input("submit").on("click", function () {
			//setup spinner
			d.hide();
			var s = new frappe.ui.Dialog({
				title: __("Checking one moment"),
				fields: [
					{
						fieldtype: "HTML",
						fieldname: "checking",
					},
				],
			});
			s.fields_dict.checking.$wrapper.html('<i class="fa fa-spinner fa-spin fa-4x"></i>');
			s.show();
			frappe.call({
				method: "frappe.email.doctype.email_account.email_account.set_email_password",
				args: {
					email_account: email_account[i]["email_account"],
					password: d.get_value("password"),
				},
				callback: function (passed) {
					s.hide();
					d.hide(); //hide waiting indication
					if (!passed["message"]) {
						frappe.show_alert(
							{ message: __("Login Failed please try again"), indicator: "error" },
							5
						);
						me.email_password_prompt(email_account, user, i);
					} else {
						if (i + 1 < email_account.length) {
							i = i + 1;
							me.email_password_prompt(email_account, user, i);
						}
					}
				},
			});
		});
		d.show();
	}
	load_bootinfo() {
		if (frappe.boot) {
			this.setup_workspaces();
			frappe.model.sync(frappe.boot.docs);
			this.check_metadata_cache_status();
			this.set_globals();
			this.sync_pages();
			frappe.router.setup();
			this.setup_moment();
			if (frappe.boot.print_css) {
				frappe.dom.set_style(frappe.boot.print_css, "print-style");
			}

			frappe.boot.setup_complete = frappe.boot.sysdefaults["setup_complete"];
			frappe.user.name = frappe.boot.user.name;
			frappe.router.setup();
		} else {
			this.set_as_guest();
		}
	}

	setup_workspaces() {
		frappe.modules = {};
		frappe.workspaces = {};
		for (let page of frappe.boot.allowed_workspaces || []) {
			frappe.modules[page.module] = page;
			frappe.workspaces[frappe.router.slug(page.name)] = page;
		}
	}

	load_user_permissions() {
		frappe.defaults.load_user_permission_from_boot();

		frappe.realtime.on(
			"update_user_permissions",
			frappe.utils.debounce(() => {
				frappe.defaults.update_user_permissions();
			}, 500)
		);
	}

	check_metadata_cache_status() {
		if (frappe.boot.metadata_version != localStorage.metadata_version) {
			frappe.assets.clear_local_storage();
			frappe.assets.init_local_storage();
		}
	}

	set_globals() {
		frappe.session.user = frappe.boot.user.name;
		frappe.session.logged_in_user = frappe.boot.user.name;
		frappe.session.user_email = frappe.boot.user.email;
		frappe.session.user_fullname = frappe.user_info().fullname;

		frappe.user_defaults = frappe.boot.user.defaults;
		frappe.user_roles = frappe.boot.user.roles;
		frappe.sys_defaults = frappe.boot.sysdefaults;

		frappe.ui.py_date_format = frappe.boot.sysdefaults.date_format
			.replace("dd", "%d")
			.replace("mm", "%m")
			.replace("yyyy", "%Y");
		frappe.boot.user.last_selected_values = {};
	}
	sync_pages() {
		// clear cached pages if timestamp is not found
		if (localStorage["page_info"]) {
			frappe.boot.allowed_pages = [];
			var page_info = JSON.parse(localStorage["page_info"]);
			$.each(frappe.boot.page_info, function (name, p) {
				if (!page_info[name] || page_info[name].modified != p.modified) {
					delete localStorage["_page:" + name];
				}
				frappe.boot.allowed_pages.push(name);
			});
		} else {
			frappe.boot.allowed_pages = Object.keys(frappe.boot.page_info);
		}
		localStorage["page_info"] = JSON.stringify(frappe.boot.page_info);
	}
	set_as_guest() {
		frappe.session.user = "Guest";
		frappe.session.user_email = "";
		frappe.session.user_fullname = "Guest";

		frappe.user_defaults = {};
		frappe.user_roles = ["Guest"];
		frappe.sys_defaults = {};
	}
	make_page_container() {
		if ($("#body").length) {
			$(".splash").remove();
			frappe.temp_container = $("<div id='temp-container' style='display: none;'>").appendTo(
				"body"
			);
			frappe.container = new frappe.views.Container();
		}
	}
	// //// NEOFFICE PATCH — NeoCockpit unified chrome (single menu, no navbar).
	// Default chrome for every Neoffice desk: ONE React sidebar that absorbs the
	// header (search, notifications bell, NORA, user menu). The legacy navbar +
	// sidebar are NOT instantiated at all — no hidden DOM, no double chrome.
	// Emergency kill-switch (legacy chrome): site_config `neoffice_cockpit_disable`.
	make_chrome() {
		if (frappe.boot.neoffice_cockpit_disable || frappe.boot.home_page === "setup-wizard") {
			this.make_nav_bar();
			this.make_sidebar();
			return;
		}
		this.make_cockpit();
	}

	make_cockpit() {
		// headless sidebar: workspace.js consumes it as its data source
		// (setup_pages / all_pages / frappe.workspaces maps) — no DOM rendered.
		this.sidebar = new frappe.ui.Sidebar({ headless: true });

		document.body.classList.add("neoffice-cockpit");
		const root = document.createElement("div");
		root.id = "neoffice-cockpit-root";
		root.style.display = "contents";
		document.body.insertBefore(root, document.body.firstChild);

		const mount = () => {
			if (!window.NeoCockpit || !window.NeoCockpit.mount) {
				setTimeout(mount, 50);
				return;
			}
			// synk (Raven docked widget) and the help panel are optional apps:
			// only wire their rail buttons when the app ships the feature.
			const has_synk =
				frappe.boot.show_raven_chat_on_desk && frappe.user.has_role("Raven User");
			const has_help = !!(frappe.boot.versions && frappe.boot.versions.nora);

			window.NeoCockpit.mount(root, {
				env: "desk",
				layout: "sidebar",
				homeUrl: "/app/home",
				onNavigate: (r) => {
					frappe.set_route(String(r).replace(/^\/app\/?/, "") || "home");
				},
				// NORA Quick Chat is an OVERLAY, not a route
				onNora: () => {
					if (frappe.ui.NoraQuickChat && frappe.ui.NoraQuickChat.show) {
						frappe.ui.NoraQuickChat.show();
					} else {
						frappe.set_route("nora-chat");
					}
				},
				onBell: () => this.toggle_cockpit_notifications(),
				onSynk: has_synk ? () => this.toggle_cockpit_synk() : undefined,
				onHelp: has_help
					? () => {
							if (window.nora && nora.help_panel) nora.help_panel.toggle();
							else window.open("/wiki", "_blank");
					  }
					: undefined,
			});
			this.setup_cockpit_awesomebar();
			this.setup_cockpit_notifications();
			if (has_synk) this.setup_cockpit_synk();
		};
		mount();

		// route flags for surface-specific chrome (relaxed head on
		// workspaces, no head on the Home dashboard, flush container on
		// lists & reports)
		const update_route_class = () => {
			const route = frappe.get_route() || [];
			const is_ws = !route.length || route[0] === "Workspaces";
			document.body.classList.toggle("nf-ws", is_ws);
			document.body.classList.toggle(
				"nf-ws-home",
				is_ws && (route[1] || "Home") === "Home"
			);
			document.body.classList.toggle(
				"nf-list",
				["List", "query-report", "dashboard-view"].includes(route[0])
			);
		};
		// Drive these body flags off "page-change" (fired by container.js
		// AFTER the new page is shown and the old one hidden) rather than the
		// router "change" event (fires the instant the route updates, while
		// the OLD page is still on screen). The router-timed version stripped
		// e.g. nf-list before the form had rendered, so the still-visible list
		// briefly lost its flush padding — a padding flash on every row click.
		$(document).on("page-change", update_route_class);
		update_route_class();
	}

	setup_cockpit_synk() {
		// The Raven widget mounts itself DOCKED under the cockpit (see
		// raven.bundle.js) and reports its unread total — paint the rail badge.
		window.addEventListener("raven-chat:unread-count", (e) => {
			const badge = document.querySelector(".nc-synk .nc-count");
			if (badge) badge.textContent = e.detail > 0 ? String(e.detail) : "";
		});
	}

	toggle_cockpit_synk() {
		// position the docked widget next to the rail's synk icon, then toggle
		const anchor = document.querySelector(".nc-side .nc-synk") || document.querySelector(".nc-synk");
		const docked = document.querySelector(".raven-chat-docked");
		if (anchor && docked) {
			const r = anchor.getBoundingClientRect();
			docked.style.left = Math.round(r.right + 12) + "px";
			docked.style.top = Math.max(10, Math.round(r.top - 6)) + "px";
		}
		window.dispatchEvent(new CustomEvent("raven-chat:toggle"));
	}

	setup_cockpit_notifications() {
		// Reuse the NATIVE frappe.ui.Notifications (realtime, mark-read, events,
		// changelog tabs). It binds on `.navbar .dropdown-notifications`, so we
		// host that exact navbar markup in a hidden fixed shell the bell anchors.
		if (!frappe.boot.desk_settings.notifications || frappe.session.user === "Guest") return;
		this.$cockpit_notif_host = $(`
			<nav class="navbar cockpit-notifications-host">
				<li class="nav-item dropdown dropdown-notifications dropdown-mobile hidden">
					<button class="btn-reset nav-link notifications-icon text-muted"
						data-toggle="dropdown" aria-haspopup="true" aria-expanded="false"
						style="width:1px;height:1px;overflow:hidden;opacity:0;"></button>
					<div class="dropdown-menu notifications-list" role="menu">
						<div class="notification-list-header">
							<div class="header-items"></div>
							<div class="header-actions"></div>
						</div>
						<div class="notification-list-body">
							<div class="panel-notifications"></div>
							<div class="panel-events"></div>
							<div class="panel-changelog-feed"></div>
						</div>
					</div>
				</li>
			</nav>
		`).appendTo("body");
		this.cockpit_notifications = new frappe.ui.Notifications();

		// realtime unseen indicator on the cockpit bell
		frappe.realtime.on("notification", () => {
			$(".nc-bell").addClass("has-unseen");
		});
	}

	toggle_cockpit_notifications() {
		// Manual show/hide: triggering bootstrap's dropdown("toggle") from the
		// bell's own click gets undone by bootstrap's document clearMenus in the
		// same tick. The tab views fetch their data at construction, so plain
		// .show classes are all the dropdown needs.
		if (!this.$cockpit_notif_host) return;
		if (this.$cockpit_notif_host.find(".notifications-list").hasClass("show")) {
			this.close_cockpit_notifications();
			return;
		}
		const bell = document.querySelector(".nc-side .nc-bell") || document.querySelector(".nc-bell");
		if (bell) {
			const r = bell.getBoundingClientRect();
			this.$cockpit_notif_host.css({ left: r.right + 10, top: Math.max(8, r.top - 4) });
		}
		$(".nc-bell").removeClass("has-unseen");
		this.$cockpit_notif_host.find(".dropdown-notifications").addClass("show");
		this.$cockpit_notif_host.find(".notifications-list").addClass("show");
		setTimeout(() => {
			$(document).on("mousedown.cockpit-notif", (e) => {
				if (
					!$(e.target).closest(".cockpit-notifications-host").length &&
					!$(e.target).closest(".nc-bell").length
				) {
					this.close_cockpit_notifications();
				}
			});
			$(document).one("page-change.cockpit-notif", () => this.close_cockpit_notifications());
		}, 0);
	}

	close_cockpit_notifications() {
		if (!this.$cockpit_notif_host) return;
		this.$cockpit_notif_host.find(".dropdown-notifications").removeClass("show");
		this.$cockpit_notif_host.find(".notifications-list").removeClass("show");
		$(document).off("mousedown.cockpit-notif");
		$(document).off("page-change.cockpit-notif");
	}

	setup_cockpit_awesomebar() {
		// Drive-like centered search: the cockpit inputs are pure TRIGGERS; the
		// real Awesome Bar input lives in a centered overlay (backdrop + pill),
		// and the mega-panel anchors under it via the existing positioning.
		if (!frappe.boot.desk_settings.search_bar) return;

		// `search-bar` class keeps the box whitelisted by the Awesome Bar's
		// outside-click close handler.
		const $shell = $(`
			<div class="cockpit-search-overlay" style="display: none;">
				<div class="cockpit-search-box search-bar">
					<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor"
						stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
						<circle cx="11" cy="11" r="7"></circle><path d="m20 20-3.5-3.5"></path>
					</svg>
					<input type="text" class="cockpit-search-field"
						placeholder="${__("Search or type a command")}" />
					<button type="button" class="cockpit-search-nora" title="${__("Ask Nora")}" tabindex="-1">
						<svg width="17" height="17" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M9.94 14.66A2 2 0 0 0 8.5 13.2l-5.4-1.4a.5.5 0 0 1 0-1l5.4-1.4A2 2 0 0 0 9.94 8L11.34 2.6a.5.5 0 0 1 1 0L13.74 8a2 2 0 0 0 1.44 1.44l5.4 1.4a.5.5 0 0 1 0 1l-5.4 1.4a2 2 0 0 0-1.44 1.44l-1.4 5.4a.5.5 0 0 1-1 0z"></path><path d="M20 3v4"></path><path d="M22 5h-4"></path></svg>
					</button>
				</div>
			</div>
		`);
		$("body").append($shell);
		const field = $shell.find(".cockpit-search-field").get(0);

		const awesome_bar = new frappe.search.AwesomeBar();
		awesome_bar.setup(field);
		awesome_bar.on_close = () => $shell.css("display", "none");

		// "Ask Nora" button inside the search box → opens the Quick Chat with the query
		const $nora_btn = $shell.find(".cockpit-search-nora");
		if (!(frappe.ui.NoraQuickChat && frappe.ui.NoraQuickChat.ask)) {
			$nora_btn.remove();
		} else {
			$nora_btn.on("mousedown", (e) => e.preventDefault());
			$nora_btn.on("click", (e) => {
				e.preventDefault();
				e.stopPropagation();
				const q = (field.value || "").trim();
				$shell.css("display", "none");
				if (awesome_bar._close) awesome_bar._close();
				if (q) frappe.ui.NoraQuickChat.ask(q);
				else frappe.ui.NoraQuickChat.show();
			});
		}
		frappe.search.utils.make_function_searchable(
			frappe.utils.generate_tracking_url,
			__("Generate Tracking URL")
		);

		const open = () => {
			$shell.css("display", "flex");
			field.focus();
			// belt: re-assert after the browser settles default focus handling
			requestAnimationFrame(() => field.focus());
		};

		// Cmd/Ctrl+G opens the overlay directly (the cockpit input doesn't
		// exist in the collapsed rail, so focusing it can't be the only path).
		document.addEventListener("keydown", (e) => {
			if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "g") {
				e.preventDefault();
				open();
			}
		});

		// cockpit search boxes (desktop sidebar, drawer, mobile strip) appear
		// after the React render — poll briefly, then turn them into overlay
		// triggers. Bound on the CONTAINER so the collapsed rail (icon only,
		// no input) opens the overlay too.
		let tries = 0;
		const wire = () => {
			const boxes = document.querySelectorAll(".neocockpit .nc-search");
			if (!boxes.length) {
				if (tries++ < 60) setTimeout(wire, 50);
				return;
			}
			boxes.forEach((box) => {
				// mousedown + preventDefault: the trigger never takes focus, so
				// there is no focus/blur race with the overlay field.
				box.addEventListener("mousedown", (e) => {
					e.preventDefault();
					open();
				});
				const input = box.querySelector("input");
				if (input) {
					input.readOnly = true; // no inline typing, no mobile keyboard flash
					// keyboard path (Tab / the cockpit's Cmd+G handler focuses it)
					input.addEventListener("focus", () => {
						input.blur();
						open();
					});
				}
			});
		};
		wire();
	}
	// //// END NEOFFICE PATCH

	make_nav_bar() {
		// toolbar
		if (frappe.boot && frappe.boot.home_page !== "setup-wizard") {
			frappe.frappe_toolbar = new frappe.ui.toolbar.Toolbar();
		}
	}

	make_sidebar() {
		this.sidebar = new frappe.ui.Sidebar({});
	}

	logout() {
		var me = this;
		me.logged_out = true;
		return frappe.call({
			method: "logout",
			callback: function (r) {
				if (r.exc) {
					return;
				}

				me.redirect_to_login();
			},
		});
	}
	handle_session_expired() {
		frappe.app.redirect_to_login();
	}
	redirect_to_login() {
		window.location.href = `/login?redirect-to=${encodeURIComponent(
			window.location.pathname + window.location.search
		)}`;
	}
	set_favicon() {
		var link = $('link[type="image/x-icon"]').remove().attr("href");
		$('<link rel="shortcut icon" href="' + link + '" type="image/x-icon">').appendTo("head");
		$('<link rel="icon" href="' + link + '" type="image/x-icon">').appendTo("head");
	}
	trigger_primary_action() {
		// to trigger change event on active input before triggering primary action
		$(document.activeElement).blur();
		// wait for possible JS validations triggered after blur (it might change primary button)
		setTimeout(() => {
			if (window.cur_dialog && cur_dialog.display && !cur_dialog.is_minimized) {
				// trigger primary
				cur_dialog.get_primary_btn().trigger("click");
			} else if (cur_frm && cur_frm.page.btn_primary.is(":visible")) {
				cur_frm.page.btn_primary.trigger("click");
			} else if (frappe.container.page.save_action) {
				frappe.container.page.save_action();
			}
		}, 100);
	}

	show_change_log() {
		var me = this;
		let change_log = frappe.boot.change_log;

		// frappe.boot.change_log = [{
		// 	"change_log": [
		// 		[<version>, <change_log in markdown>],
		// 		[<version>, <change_log in markdown>],
		// 	],
		// 	"description": "ERP made simple",
		// 	"title": "ERPNext",
		// 	"version": "12.2.0"
		// }];

		if (
			!Array.isArray(change_log) ||
			!change_log.length ||
			window.Cypress ||
			cint(frappe.boot.sysdefaults.disable_change_log_notification)
		) {
			return;
		}

		// Iterate over changelog
		var change_log_dialog = frappe.msgprint({
			message: frappe.render_template("change_log", { change_log: change_log }),
			title: __("Updated To A New Version 🎉"),
			wide: true,
		});
		change_log_dialog.keep_open = true;
		change_log_dialog.custom_onhide = function () {
			frappe.call({
				method: "frappe.utils.change_log.update_last_known_versions",
			});
			me.show_notes();
		};
	}

	show_update_available() {
		if (!frappe.boot.has_app_updates) return;
		frappe.xcall("frappe.utils.change_log.show_update_popup");
	}

	add_browser_class() {
		$("html").addClass(frappe.utils.get_browser().name.toLowerCase());
	}

	set_fullwidth_if_enabled() {
		frappe.ui.toolbar.set_fullwidth_if_enabled();
	}

	show_notes() {
		var me = this;
		if (frappe.boot.notes.length) {
			frappe.boot.notes.forEach(function (note) {
				if (!note.seen || note.notify_on_every_login) {
					var d = new frappe.ui.Dialog({ content: note.content, title: note.title });
					d.keep_open = true;
					d.msg_area = $('<div class="msgprint">').appendTo(d.body);
					d.msg_area.append(note.content);
					d.onhide = function () {
						note.seen = true;
						// Mark note as read if the Notify On Every Login flag is not set
						if (!note.notify_on_every_login) {
							frappe.call({
								method: "frappe.desk.doctype.note.note.mark_as_seen",
								args: {
									note: note.name,
								},
							});
						} else {
							frappe.call({
								method: "frappe.desk.doctype.note.note.reset_notes",
							});
						}
					};
					d.show();
				}
			});
		}
	}

	setup_build_events() {
		if (frappe.boot.developer_mode) {
			frappe.require("build_events.bundle.js");
		}
	}

	setup_energy_point_listeners() {
		frappe.realtime.on("energy_point_alert", (message) => {
			frappe.show_alert(message);
		});
	}

	//// Neoffice — added (no upstream equivalent). A dialog's backdrop, or a
	//// `#freeze` layer, can outlive what it belongs to and then covers the page
	//// invisibly: every click and keystroke is swallowed and the user can only
	//// reload ("the table comes up with the forbidden cursor, refresh fixes
	//// it"). Individual leak paths were patched one by one (erpnext Quick
	//// Journal Entry, twice) and it kept coming back, so the cure is here
	//// instead: whenever the page settles with nothing displayed, drop whatever
	//// is still covering it. See frappe.dom.sweep_orphan_overlays() for why it
	//// is safe. The delays clear Bootstrap's 300ms hide transition — sweeping
	//// during it would fight the teardown it is meant to finish.
	setup_overlay_watchdog() {
		const sweep = (reason) => {
			setTimeout(() => frappe.dom.sweep_orphan_overlays(reason), 500);
		};

		// SPA navigation: the classic leak, a dialog left behind by the page
		// it was opened from.
		frappe.router.on("change", () => sweep("route change"));

		// Any dialog closing: if Bootstrap did not finish its own teardown,
		// this catches it half a second later.
		$(document).on("hidden.bs.modal", () => sweep("dialog hidden"));

		// Coming back to a tab that was suspended (laptop asleep, network
		// dropped mid-save): timers and transitions that were supposed to clean
		// up never ran while the tab was frozen.
		$(window).on("focus", () => sweep("window focus"));
		document.addEventListener("visibilitychange", () => {
			if (document.visibilityState === "visible") sweep("tab visible");
		});
	}

	setup_copy_doc_listener() {
		$("body").on("paste", (e) => {
			try {
				let pasted_data = frappe.utils.get_clipboard_data(e);
				let doc = JSON.parse(pasted_data);
				if (doc.doctype) {
					e.preventDefault();
					const sleep = frappe.utils.sleep;

					frappe.dom.freeze(__("Creating {0}", [doc.doctype]) + "...");
					// to avoid abrupt UX
					// wait for activity feedback
					sleep(500).then(() => {
						let res = frappe.model.with_doctype(doc.doctype, () => {
							let newdoc = frappe.model.copy_doc(doc);
							newdoc.__newname = doc.name;
							delete doc.name;
							newdoc.idx = null;
							newdoc.__run_link_triggers = false;
							newdoc.on_paste_event = true;
							newdoc = JSON.parse(JSON.stringify(newdoc));
							frappe.set_route("Form", newdoc.doctype, newdoc.name);
							frappe.dom.unfreeze();
						});
						res && res.fail?.(frappe.dom.unfreeze);
					});
				}
			} catch (e) {
				//
			}
		});
	}

	/// Setup event listeners for events across browser tabs / web workers.
	setup_broadcast_listeners() {
		// booted in another tab -> refresh csrf to avoid invalid requests.
		frappe.broadcast.on("boot", ({ csrf_token, user }) => {
			if (user && user != frappe.session.user) {
				frappe.msgprint({
					message: __(
						"You've logged in as another user from another tab. Refresh this page to continue using system."
					),
					title: __("User Changed"),
					primary_action: {
						label: __("Refresh"),
						action: () => {
							window.location.reload();
						},
					},
				});
				return;
			}

			if (csrf_token) {
				// If user re-logged in then their other tabs won't be usable without this update.
				frappe.csrf_token = csrf_token;
			}
		});
	}

	setup_moment() {
		moment.updateLocale("en", {
			week: {
				dow: frappe.datetime.get_first_day_of_the_week_index(),
			},
		});
		moment.locale("en");
		moment.user_utc_offset = moment().utcOffset();
		if (frappe.boot.timezone_info) {
			moment.tz.add(frappe.boot.timezone_info);
		}
	}
};

frappe.get_module = function (m, default_module) {
	var module = frappe.modules[m] || default_module;
	if (!module) {
		return;
	}

	if (module._setup) {
		return module;
	}

	if (!module.label) {
		module.label = m;
	}

	if (!module._label) {
		module._label = __(module.label);
	}

	module._setup = true;

	return module;
};
