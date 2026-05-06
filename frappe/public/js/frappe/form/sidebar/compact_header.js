// Copyright (c) 2026, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt
//
// Form Compact Header — meta cluster injected into .page-actions of any
// DocType form. Phase 2 ships the full cluster:
//   [gallery]  |  [avatars]  |  📎 N · 💬 N · 🏷 N  |  ♥ Suivre · ↗ Share
// The native right sidebar stays visible — Phase 3 introduces the toggle
// that hides it. The cluster is only created by Sidebar.make() so the
// .form-meta-cluster class itself is enough to scope its styles.

frappe.ui.form.FILE_TYPE_MAP = {
	// Images render as real thumbnails
	jpg: { kind: "image" },
	jpeg: { kind: "image" },
	png: { kind: "image" },
	gif: { kind: "image" },
	webp: { kind: "image" },
	svg: { kind: "image" },
	avif: { kind: "image" },
	heic: { kind: "image" },

	// PDF — red tile
	pdf: { kind: "file", cls: "pdf", label: "PDF" },

	// Spreadsheet / data — green tile
	xlsx: { kind: "file", cls: "xls", label: "Excel" },
	xls: { kind: "file", cls: "xls", label: "Excel" },
	csv: { kind: "file", cls: "xls", label: "CSV" },
	ods: { kind: "file", cls: "xls", label: "OpenDoc" },

	// Word — blue tile
	docx: { kind: "file", cls: "doc", label: "Word" },
	doc: { kind: "file", cls: "doc", label: "Word" },
	odt: { kind: "file", cls: "doc", label: "OpenDoc" },
	rtf: { kind: "file", cls: "doc", label: "RTF" },

	// Archives — amber tile
	zip: { kind: "file", cls: "zip", label: "ZIP" },
	rar: { kind: "file", cls: "zip", label: "RAR" },
	"7z": { kind: "file", cls: "zip", label: "7z" },
	tar: { kind: "file", cls: "zip", label: "TAR" },
	gz: { kind: "file", cls: "zip", label: "GZ" },

	// Presentation — orange tile
	pptx: { kind: "file", cls: "ppt", label: "PowerPoint" },
	ppt: { kind: "file", cls: "ppt", label: "PowerPoint" },
	key: { kind: "file", cls: "ppt", label: "Keynote" },

	// Code / data — purple tile
	json: { kind: "file", cls: "code", label: "JSON" },
	xml: { kind: "file", cls: "code", label: "XML" },
	txt: { kind: "file", cls: "code", label: "Text" },
	md: { kind: "file", cls: "code", label: "MD" },
};

// Inline SVG icons — keeps the cluster self-contained without pulling
// in the full Frappe icon registry (some es-line glyphs are missing).
frappe.ui.form.COMPACT_ICONS = {
	paperclip:
		'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="m21 12-9.5 9.5a5 5 0 0 1-7-7L13 5a3.5 3.5 0 0 1 5 5L9 19"/></svg>',
	chat:
		'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M21 11.5a8.5 8.5 0 0 1-8.5 8.5 8.4 8.4 0 0 1-3.8-.9L3 21l1.9-5.7A8.5 8.5 0 1 1 21 11.5z"/></svg>',
	tag:
		'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M20.59 13.41 13.42 20.58a2 2 0 0 1-2.83 0L2 12V2h10l8.59 8.59a2 2 0 0 1 0 2.82z"/><circle cx="7" cy="7" r="1.5"/></svg>',
	heart:
		'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l8.84-8.84a5.5 5.5 0 0 0 0-7.78z"/></svg>',
	heart_filled:
		'<svg width="13" height="13" viewBox="0 0 24 24" fill="currentColor" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><path d="M20.84 4.61a5.5 5.5 0 0 0-7.78 0L12 5.67l-1.06-1.06a5.5 5.5 0 0 0-7.78 7.78l1.06 1.06L12 21.23l8.84-8.84a5.5 5.5 0 0 0 0-7.78z"/></svg>',
	share:
		'<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.6" stroke-linecap="round" stroke-linejoin="round"><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><path d="m8.5 10.5 7-4M8.5 13.5l7 4"/></svg>',
};

frappe.ui.form.CompactHeader = class CompactHeader {
	constructor(opts) {
		Object.assign(this, opts);
		this.max_visible = 4;
		this.max_avatars = 3;
		this.make();
	}

	make() {
		// frm.page exposes `wrapper` (raw DOM) and `page_actions` (jQuery)
		// but `frm.$wrapper` is the most reliable jQuery handle on the page.
		const $page_actions =
			(this.frm.page && this.frm.page.page_actions) ||
			(this.frm.$wrapper && this.frm.$wrapper.find(".page-actions").first()) ||
			$();
		if (!$page_actions.length) return;

		// Idempotent: remove any existing cluster from a previous render
		$page_actions.find(".form-meta-cluster").remove();

		const ic = frappe.ui.form.COMPACT_ICONS;
		this.$wrapper = $(`
			<div class="form-meta-cluster">
				<div class="meta-gallery"></div>
				<div class="meta-divider gallery-divider"></div>
				<div class="meta-avatars" title="${__("Assigned To")}"></div>
				<div class="meta-divider avatars-divider"></div>
				<button class="meta-chip meta-attachments" type="button" title="${__(
					"Attachments"
				)}">${ic.paperclip}<strong class="count">0</strong></button>
				<button class="meta-chip meta-comments" type="button" title="${__(
					"Comments"
				)}">${ic.chat}<strong class="count">0</strong></button>
				<button class="meta-chip meta-tags" type="button" title="${__(
					"Tags"
				)}">${ic.tag}<strong class="count">0</strong></button>
				<div class="meta-divider"></div>
				<button class="meta-chip meta-follow" type="button" title="${__(
					"Follow"
				)}">${ic.heart}<span class="label">${__("Follow")}</span></button>
				<button class="meta-chip meta-share" type="button" title="${__(
					"Share"
				)}">${ic.share}</button>
			</div>
		`);

		// Inject as the first child of .page-actions, before .form-viewers
		$page_actions.prepend(this.$wrapper);

		this.bind_events();
		this.refresh();
	}

	bind_events() {
		const me = this;
		this.$wrapper.find(".meta-attachments").on("click", () => me.show_all_attachments());
		this.$wrapper.find(".meta-comments").on("click", () => me.scroll_to_comments());
		this.$wrapper.find(".meta-tags").on("click", () => me.focus_tag_input());
		this.$wrapper.find(".meta-follow").on("click", () => me.toggle_follow());
		this.$wrapper.find(".meta-share").on("click", () => me.open_share_dialog());
	}

	refresh() {
		if (!this.$wrapper) return;

		// Hide entirely on locally-unsaved docs — same behaviour as native sidebar
		if (this.frm.doc.__islocal) {
			this.$wrapper.hide();
			return;
		}

		this.$wrapper.show();

		const attachments = this.get_attachments();
		this.render_gallery(attachments);
		this.render_avatars();
		this.render_chips(attachments);
		this.render_follow_state();
	}

	// ---- Attachments gallery -----------------------------------------

	get_attachments() {
		if (!this.frm.attachments || !this.frm.attachments.get_attachments) {
			return [];
		}
		return this.frm.attachments.get_attachments().map((att) => {
			const ext = this.get_extension(att.file_name);
			const type_def = frappe.ui.form.FILE_TYPE_MAP[ext] || {
				kind: "file",
				cls: "gen",
				label: ext.toUpperCase() || "File",
			};
			return Object.assign({}, att, { extension: ext, type_def });
		});
	}

	get_extension(file_name) {
		if (!file_name) return "";
		const dot = file_name.lastIndexOf(".");
		return dot > 0 ? file_name.slice(dot + 1).toLowerCase() : "";
	}

	render_gallery(attachments) {
		const $gallery = this.$wrapper.find(".meta-gallery").empty();
		const $divider = this.$wrapper.find(".gallery-divider");
		if (!attachments.length) {
			$gallery.hide();
			$divider.hide();
			return;
		}
		$gallery.show();
		$divider.show();

		const visible = attachments.slice(0, this.max_visible);
		const hidden = Math.max(0, attachments.length - this.max_visible);

		visible.forEach((att) => {
			$gallery.append(this.make_thumb(att));
		});

		if (hidden > 0) {
			$gallery.append(this.make_more_button(hidden, attachments.length));
		}
	}

	make_thumb(att) {
		const file_url = this.get_file_url(att);
		const safe_name = frappe.utils.escape_html(att.file_name || "");
		const $btn = $(`
			<button class="att" type="button" title="${safe_name}"></button>
		`);

		if (att.type_def.kind === "image") {
			$btn.append(`<img class="att-thumb" src="${file_url}" alt=""/>`);
		} else {
			const short = frappe.utils.escape_html(this.short_name(att.file_name));
			const cls = att.type_def.cls || "gen";
			$btn.append(`
				<div class="att-file ${cls}">
					<span class="ext">${frappe.utils.escape_html(
						att.type_def.label || att.extension.toUpperCase() || "·"
					)}</span>
					<span class="name">${short}</span>
				</div>
			`);
		}

		$btn.on("click", () => this.handle_thumb_click(att));
		return $btn;
	}

	make_more_button(hidden_count, total) {
		const $btn = $(`
			<button class="att" type="button"
				title="${__("View all attachments ({0})", [total])}">
				<div class="att-more">+${hidden_count}</div>
			</button>
		`);
		$btn.on("click", () => this.show_all_attachments());
		return $btn;
	}

	handle_thumb_click(att) {
		const file_url = this.get_file_url(att);
		// PDF — use Frappe's native preview if available
		if (att.extension === "pdf" && typeof frappe.preview_pdf === "function") {
			frappe.preview_pdf(file_url);
			return;
		}
		window.open(file_url, "_blank", "noopener");
	}

	show_all_attachments() {
		// Phase 1/2: redirect to File list filtered on this document.
		// Phase 4 will replace this with an in-page modal.
		frappe.open_in_new_tab = true;
		frappe.set_route("List", "File", {
			attached_to_doctype: this.frm.doctype,
			attached_to_name: this.frm.docname,
		});
	}

	get_file_url(att) {
		if (att.file_url) return att.file_url;
		if (att.file_name && att.file_name.startsWith("files/")) {
			return "/" + att.file_name;
		}
		if (att.file_name) return "/files/" + att.file_name;
		return "#";
	}

	short_name(file_name, max = 8) {
		if (!file_name) return "";
		const dot = file_name.lastIndexOf(".");
		const base = dot > 0 ? file_name.slice(0, dot) : file_name;
		return base.length > max ? base.slice(0, max) : base;
	}

	// ---- Avatars -----------------------------------------------------

	get_assigned_users() {
		const docinfo = this.frm.get_docinfo() || {};
		// docinfo.assignments is the canonical source (array of objects with `owner`)
		const assignments = docinfo.assignments || [];
		const users = assignments.map((a) => a.owner).filter(Boolean);
		if (users.length) return users;
		// Fallback: doc._assign is a JSON-stringified array kept on the document
		try {
			const raw = this.frm.doc._assign;
			if (raw) {
				const parsed = typeof raw === "string" ? JSON.parse(raw) : raw;
				return Array.isArray(parsed) ? parsed : [];
			}
		} catch (e) {
			// silent — empty list is the safe fallback
		}
		return [];
	}

	render_avatars() {
		const $av = this.$wrapper.find(".meta-avatars").empty();
		const $divider = this.$wrapper.find(".avatars-divider");
		const users = this.get_assigned_users();
		if (!users.length) {
			$av.hide();
			$divider.hide();
			return;
		}
		$av.show();
		$divider.show();
		const $group = frappe.avatar_group(users, this.max_avatars, {
			align: "left",
			overlap: true,
		});
		$av.append($group);
	}

	// ---- Counts ------------------------------------------------------

	render_chips(attachments) {
		const docinfo = this.frm.get_docinfo() || {};
		const tags = (docinfo.tags || "").split(",").filter((t) => t.trim());
		const comments = Array.isArray(docinfo.comments) ? docinfo.comments : [];

		this.$wrapper.find(".meta-attachments .count").text(attachments.length);
		this.$wrapper.find(".meta-comments .count").text(comments.length);
		this.$wrapper.find(".meta-tags .count").text(tags.length);
	}

	scroll_to_comments() {
		const $comments = this.frm.$wrapper.find(
			".comment-input-wrapper, .form-comments, .new-timeline-content"
		);
		if ($comments.length) {
			$comments[0].scrollIntoView({ behavior: "smooth", block: "center" });
			const $input = $comments.find(".ql-editor").first();
			if ($input.length) setTimeout(() => $input.trigger("focus"), 400);
		}
	}

	focus_tag_input() {
		// Frappe stores the TagEditor instance on the form sidebar
		const tags = this.frm.tags;
		if (!tags) return;
		// Tag editor lives in .form-tags inside the sidebar
		const $tagInput = this.frm.sidebar.sidebar.find(".tag-input");
		if ($tagInput.length) {
			$tagInput.trigger("focus");
		} else {
			// Fall back to clicking the add-tags-btn which Frappe wires up
			this.frm.sidebar.sidebar.find(".add-tags-btn").first().trigger("click");
		}
	}

	// ---- Follow ------------------------------------------------------

	render_follow_state() {
		const $btn = this.$wrapper.find(".meta-follow");
		// On the very first render, seed the local state from docinfo.
		// Subsequent renders trust `this.is_following` so the toggle stays
		// authoritative even when reload_docinfo() refreshes other fields.
		if (typeof this.is_following === "undefined") {
			const docinfo = this.frm.get_docinfo() || {};
			this.is_following = !!docinfo.is_document_followed;
		}
		const ic = frappe.ui.form.COMPACT_ICONS;
		$btn.toggleClass("active", this.is_following);
		$btn.attr("title", this.is_following ? __("Unfollow") : __("Follow"));
		$btn.find("svg").replaceWith(this.is_following ? ic.heart_filled : ic.heart);
		$btn.find(".label").text(this.is_following ? __("Following") : __("Follow"));
	}

	toggle_follow() {
		const want_follow = !this.is_following;
		// Optimistic update — the click feels instant; we revert on server error.
		this.is_following = want_follow;
		this.render_follow_state();
		frappe.call({
			method: "frappe.desk.form.document_follow.update_follow",
			args: {
				doctype: this.frm.doctype,
				doc_name: this.frm.docname,
				following: want_follow ? 1 : 0,
			},
			error: () => {
				this.is_following = !want_follow;
				this.render_follow_state();
			},
		});
	}

	// ---- Share -------------------------------------------------------

	open_share_dialog() {
		if (this.frm.shared && typeof this.frm.shared.show === "function") {
			this.frm.shared.show();
		}
	}
};
