import "./assign_to";
import "./attachments";
import "./share";
import "./review";
import "./document_follow";
import "./user_image";
import "./form_sidebar_users";
import { get_user_link, get_user_message } from "../footer/version_timeline_content_builder";

frappe.ui.form.Sidebar = class {
	constructor(opts) {
		$.extend(this, opts);
	}

	make() {
		var sidebar_content = frappe.render_template("form_sidebar", {
			doctype: this.frm.doctype,
			frm: this.frm,
			can_write: frappe.model.can_write(this.frm.doctype, this.frm.docname),
		});

		this.sidebar = $('<div class="form-sidebar overlay-sidebar hidden-xs hidden-sm"></div>')
			.html(sidebar_content)
			.appendTo(this.page.sidebar.empty());

		this.comments = this.sidebar.find(".form-sidebar-stats .comments");
		this.user_actions = this.sidebar.find(".user-actions");
		this.image_section = this.sidebar.find(".sidebar-image-section");
		this.image_wrapper = this.image_section.find(".sidebar-image-wrapper");
		this.make_assignments();
		this.make_attachments();
		this.make_review();
		this.make_shared();

		this.make_tags();
		this.make_like();
		this.make_follow();

		this.bind_events();
		this.setup_keyboard_shortcuts();
		this.show_auto_repeat_status();
		frappe.ui.form.setup_user_image_event(this.frm);
		this.make_sidebar_menu(this.sidebar); //// added

		this.refresh();
	}

	bind_events() {
		var me = this;

		// scroll to comments
		this.comments.on("click", function () {
			frappe.utils.scroll_to(me.frm.footer.wrapper.find(".comment-box"), true);
		});

		this.like_icon.on("click", function () {
			frappe.ui.toggle_like(me.like_wrapper, me.frm.doctype, me.frm.doc.name, function () {
				me.refresh_like();
			});
		});
	}

	setup_keyboard_shortcuts() {
		// add assignment shortcut
		let assignment_link = this.sidebar.find(".add-assignment");
		frappe.ui.keys.get_shortcut_group(this.page).add(assignment_link);
	}

	refresh() {
		if (this.frm.doc.__islocal) {
			this.sidebar.toggle(false);
			this.page.sidebar.addClass("hide-sidebar");
		} else {
			this.page.sidebar.removeClass("hide-sidebar");
			this.sidebar.toggle(true);
			this.frm.assign_to.refresh();
			this.frm.attachments.refresh();
			this.frm.shared.refresh();

			this.frm.tags && this.frm.tags.refresh(this.frm.get_docinfo().tags);

			if (this.frm.doc.route && cint(frappe.boot.website_tracking_enabled)) {
				let route = this.frm.doc.route;
				frappe.utils.get_page_view_count(route).then((res) => {
					this.sidebar
						.find(".pageview-count")
						.html(__("{0} Web page views", [String(res.message).bold()]));
				});
			}

			this.sidebar
				.find(".modified-by")
				.html(
					get_user_message(
						this.frm.doc.modified_by,
						__("You last edited this", null),
						__("{0} last edited this", [get_user_link(this.frm.doc.modified_by)])
					) +
						" · " +
						comment_when(this.frm.doc.modified)
				);
			this.sidebar
				.find(".created-by")
				.html(
					get_user_message(
						this.frm.doc.owner,
						__("You created this", null),
						__("{0} created this", [get_user_link(this.frm.doc.owner)])
					) +
						" · " +
						comment_when(this.frm.doc.creation)
				);

			this.refresh_like();
			this.refresh_follow();
			this.refresh_comments_count();
			frappe.ui.form.set_user_image(this.frm);
		}
	}

	show_auto_repeat_status() {
		if (this.frm.meta.allow_auto_repeat && this.frm.doc.auto_repeat) {
			const me = this;
			frappe.call({
				method: "frappe.client.get_value",
				args: {
					doctype: "Auto Repeat",
					filters: {
						name: this.frm.doc.auto_repeat,
					},
					fieldname: ["frequency"],
				},
				callback: function (res) {
					me.sidebar
						.find(".auto-repeat-status")
						.html(__("Repeats {0}", [__(res.message.frequency)]));
					me.sidebar.find(".auto-repeat-status").on("click", function () {
						frappe.set_route("Form", "Auto Repeat", me.frm.doc.auto_repeat);
					});
				},
			});
		}
	}

	make_tags() {
		if (this.frm.meta.issingle) {
			this.sidebar.find(".form-tags").toggle(false);
			return;
		}

		let tags_parent = this.sidebar.find(".form-tags");

		this.frm.tags = new frappe.ui.TagEditor({
			parent: tags_parent,
			add_button: tags_parent.find(".add-tags-btn"),
			frm: this.frm,
			on_change: function (user_tags) {
				this.frm.tags && this.frm.tags.refresh(user_tags);
			},
		});
	}

	make_attachments() {
		var me = this;
		this.frm.attachments = new frappe.ui.form.Attachments({
			parent: me.sidebar.find(".form-attachments"),
			frm: me.frm,
		});
	}

	make_assignments() {
		this.frm.assign_to = new frappe.ui.form.AssignTo({
			parent: this.sidebar.find(".form-assignments"),
			frm: this.frm,
		});
	}

	make_shared() {
		this.frm.shared = new frappe.ui.form.Share({
			frm: this.frm,
			parent: this.sidebar.find(".form-shared"),
		});
	}

	add_user_action(label, click) {
		return $("<a>")
			.html(label)
			.appendTo(
				$('<li class="user-action-row">').appendTo(this.user_actions.removeClass("hidden"))
			)
			.on("click", click);
	}

	clear_user_actions() {
		this.user_actions.addClass("hidden");
		this.user_actions.find(".user-action-row").remove();
	}

	make_like() {
		this.like_wrapper = this.sidebar.find(".liked-by");
		this.like_icon = this.sidebar.find(".liked-by .like-icon");
		this.like_count = this.sidebar.find(".liked-by .like-count");
		frappe.ui.setup_like_popover(this.sidebar.find(".form-stats-likes"), ".like-icon");
	}

	make_follow() {
		this.follow_button = this.sidebar.find(".form-sidebar-stats .form-follow");

		this.follow_button.on("click", () => {
			let is_followed = this.frm.get_docinfo().is_document_followed;
			frappe
				.call("frappe.desk.form.document_follow.update_follow", {
					doctype: this.frm.doctype,
					doc_name: this.frm.doc.name,
					following: !is_followed,
				})
				.then(() => {
					frappe.model.set_docinfo(
						this.frm.doctype,
						this.frm.doc.name,
						"is_document_followed",
						!is_followed
					);
					this.refresh_follow(!is_followed);
				});
		});
	}

	refresh_follow(follow) {
		if (follow == null) {
			follow = this.frm.get_docinfo().is_document_followed;
		}
		this.follow_button.text(follow ? __("Unfollow") : __("Follow"));
	}

	refresh_like() {
		if (!this.like_icon) {
			return;
		}

		this.like_wrapper.attr("data-liked-by", this.frm.doc._liked_by);
		const liked = frappe.ui.is_liked(this.frm.doc);
		this.like_wrapper
			.toggleClass("not-liked", !liked)
			.toggleClass("liked", liked)
			.attr("data-doctype", this.frm.doctype)
			.attr("data-name", this.frm.doc.name);

		this.like_count && this.like_count.text(JSON.parse(this.frm.doc._liked_by || "[]").length);
	}

	refresh_comments_count() {
		let count = (this.frm.get_docinfo().comments || []).length;
		this.comments.find(".comments-count").html(count);
	}

	refresh_image() {}

	make_review() {
		const review_wrapper = this.sidebar.find(".form-reviews");
		if (frappe.boot.energy_points_enabled && !this.frm.is_new()) {
			this.frm.reviews = new frappe.ui.form.Review({
				parent: review_wrapper,
				frm: this.frm,
			});
		} else {
			review_wrapper.remove();
		}
	}

	reload_docinfo(callback) {
		frappe.call({
			method: "frappe.desk.form.load.get_docinfo",
			args: {
				doctype: this.frm.doctype,
				name: this.frm.docname,
			},
			callback: (r) => {
				// docinfo will be synced
				if (callback) callback(r.docinfo);
				this.frm.timeline && this.frm.timeline.refresh();
				this.frm.assign_to.refresh();
				this.frm.attachments.refresh();
			},
		});
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

				const $sidebarSections = $(sidebar).find('.standard-sidebar-section').not(".hidden");
				const $nonLabelItems = $sidebarSections.children().not(".standard-sidebar-label");

				$nonLabelItems.each(function () {
					const $currentElement = $(this);
					const itemname = $currentElement.data("name");
					const itemparent = $currentElement.data("parent");

					if (itemparent) {
						const $selectoritemname = $sidebarSections.find(`[data-name="${itemname}"].sidebar-item-container`);
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
				
				$nonLabelItems.find(".drop-icon").on("click", (e) => {
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
					} else {
						if (!existingArray.includes(itemname)) {
							existingArray.push(itemname);
							localStorage.setItem("list_sidebar_open", JSON.stringify(existingArray));
						}
					}
				});
			}
		});
	}
};
