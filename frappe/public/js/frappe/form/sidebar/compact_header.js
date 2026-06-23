////////////////////////////////////////////////////////////////////////
//// Neoffice ▼▼▼ form-header-compact (NEW FILE — entire module)
////
//// Patch: Form Header Compact (Neoffice fork on bvisible/frappe v15)
//// Status: Phase 2 — full meta cluster (avatars, chips, follow, share)
//// Spec : Obsidian → Neoffice/Form-Header-Compact/00-README.md
////
//// Form Compact Header — meta cluster injected into .page-actions of
//// any DocType form. Layout:
////   [gallery] | [avatars] | 📎 N · 💬 N · 🏷 N | ♥ Follow · ↗ Share
//// The native right sidebar stays visible — Phase 3 introduces the
//// toggle that hides it. The .form-meta-cluster element is only ever
//// created by frappe.ui.form.Sidebar.make() (see form_sidebar.js)
//// so the class name itself is enough to scope its styles.
////
//// On a Frappe upstream upgrade: keep this file as-is, only adapt
//// references to frm.page / get_docinfo() / frappe.avatar_group if
//// upstream renames them. Maintenance contract: this module only
//// READS frm state, the only mutating call is update_follow.
////////////////////////////////////////////////////////////////////////

// Copyright (c) 2026, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt

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

// ──────────────────────────────────────────────────────────────────
// Phase 3 — Compact mode is ALWAYS ON (Neoffice canonical form layout).
//
// The masking rules in compact_header.scss apply unconditionally to
// every form page (scoped by data-route + docstatus markers), so we no
// longer need a body-class toggle. The `frappe.compact_form_header`
// object is kept as a stable no-op shim so any external callers that
// referenced the old API don't crash; calls are silently accepted but
// have no visible effect.
// ──────────────────────────────────────────────────────────────────
frappe.compact_form_header = {
	is_enabled() {
		return true;
	},
	set() {
		// No-op — compact mode is always enabled. Kept for backward
		// compatibility with older code paths and developer console
		// usage. To opt out per-instance, customise the SCSS directly.
	},
	toggle() {
		return true;
	},
	apply() {
		// No-op — see above.
	},
};

frappe.ui.form.CompactHeader = class CompactHeader {
	constructor(opts) {
		Object.assign(this, opts);
		this.max_avatars = 3;
		// max_visible is computed dynamically by get_max_visible(); we
		// keep a sensible default for the very first render.
		this.max_visible = this.get_max_visible();
		this.make();
	}

	// Adapt the gallery to the viewport: 4 thumbs on desktop, 3 on
	// tablet (1024–1279px) where the buttons row gets cramped. The
	// SCSS hides the cluster entirely below 992px (mobile + tablet),
	// where the native sidebar overlay takes over.
	get_max_visible() {
		const w = window.innerWidth || document.documentElement.clientWidth || 1440;
		if (w < 1280) return 3;
		return 4;
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
				<div class="meta-avatars" title="${__("Assigned To")}"></div>
				<button class="meta-chip meta-attachments" type="button" title="${__(
					"Attachments"
				)}">${ic.paperclip}<span class="count"></span></button>
				<button class="meta-chip meta-comments" type="button" title="${__(
					"Comments"
				)}">${ic.chat}<span class="count"></span></button>
				<button class="meta-chip meta-tags" type="button" title="${__(
					"Tags"
				)}">${ic.tag}<span class="count"></span></button>
				<button class="meta-chip meta-follow" type="button" title="${__(
					"Follow"
				)}">${ic.heart}</button>
				<button class="meta-chip meta-share" type="button" title="${__(
					"Share"
				)}">${ic.share}</button>
			</div>
		`);

		// Inject as the first child of .page-actions, before .form-viewers
		$page_actions.prepend(this.$wrapper);

		this.bind_events();
		this.bind_resize_handler();
		this.refresh();
	}

	// Re-render the gallery when the viewport crosses the tablet
	// breakpoint so we drop / add a thumbnail on the fly. Debounced
	// to keep resize cheap on dragging windows.
	bind_resize_handler() {
		let timer = null;
		this._resize_handler = () => {
			clearTimeout(timer);
			timer = setTimeout(() => {
				const next = this.get_max_visible();
				if (next !== this.max_visible) {
					this.max_visible = next;
					this.render_gallery(this.get_attachments());
				}
			}, 150);
		};
		window.addEventListener("resize", this._resize_handler);
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

		// Do NOT use jQuery .show() here: when the first render happens at a
		// viewport where the SCSS hides the cluster (< 992px — i.e. loading
		// straight onto a phone or tablet), .show() detects the element as
		// "hidden by stylesheet" and writes an inline `display: block`, which
		// BEATS the responsive `display: none` rule and makes the cluster
		// reappear, overlapping the page-head. Clearing the inline display
		// instead lets the stylesheet own visibility (inline-flex on desktop,
		// hidden < 992). This also undoes the .hide() above once a draft is saved.
		this.$wrapper.css("display", "");

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
		// With attachments: show the thumbnail gallery and hide the plain
		// paperclip chip (the thumbnails already represent them — no point
		// showing both). With none: hide the gallery and keep the chip
		// (greyed, no count) so the user can still open / add attachments.
		const $attach_chip = this.$wrapper.find(".meta-attachments");
		if (!attachments.length) {
			$gallery.hide();
			$attach_chip.removeClass("chip-hidden");
			return;
		}
		$gallery.show();
		$attach_chip.addClass("chip-hidden");

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
			// On hover, surface a larger preview popover so the thumb
			// stays compact in the cluster but the user can read it.
			this.bind_image_hover_preview($btn, att);
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

	bind_image_hover_preview($btn, att) {
		const file_url = this.get_file_url(att);
		let $preview = null;
		let timer = null;

		$btn.on("mouseenter.compact", () => {
			clearTimeout(timer);
			timer = setTimeout(() => {
				if ($preview) return;
				$preview = $(`
					<div class="att-hover-preview">
						<img src="${file_url}" alt=""/>
						<div class="att-hover-name">${frappe.utils.escape_html(att.file_name || "")}</div>
					</div>
				`);
				$("body").append($preview);
				const r = $btn[0].getBoundingClientRect();
				const previewW = $preview.outerWidth();
				// Anchor below the thumb, horizontally centered, but keep
				// the preview inside the viewport on narrow screens.
				let left = r.left + r.width / 2 - previewW / 2;
				left = Math.max(8, Math.min(left, window.innerWidth - previewW - 8));
				$preview.css({ top: r.bottom + 8 + "px", left: left + "px" });
			}, 220); // small delay avoids flicker on quick mouse passes
		});

		$btn.on("mouseleave.compact", () => {
			clearTimeout(timer);
			if ($preview) {
				$preview.remove();
				$preview = null;
			}
		});
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
		// Image — open inline lightbox so the user reads it in place
		// rather than getting yanked to a new browser tab.
		if (att.type_def && att.type_def.kind === "image") {
			this.show_image_lightbox(att);
			return;
		}
		window.open(file_url, "_blank", "noopener");
	}

	show_image_lightbox(att) {
		const file_url = this.get_file_url(att);
		const safe_name = frappe.utils.escape_html(att.file_name || "");
		const $lightbox = $(`
			<div class="att-lightbox-bg" role="dialog" aria-modal="true">
				<button class="att-lightbox-close" type="button" aria-label="${__(
					"Close"
				)}">×</button>
				<figure class="att-lightbox-figure">
					<img src="${file_url}" alt=""/>
					<figcaption>${safe_name}</figcaption>
				</figure>
			</div>
		`);
		const close = () => {
			$lightbox.remove();
			$(document).off("keydown.compact-lightbox");
		};
		$lightbox.on("click", (e) => {
			if (e.target === $lightbox[0] || $(e.target).is(".att-lightbox-close")) {
				close();
			}
		});
		$(document).on("keydown.compact-lightbox", (e) => {
			if (e.key === "Escape") close();
		});
		$("body").append($lightbox);
	}

	show_all_attachments() {
		// Reuse the existing modal if it's still open — Frappe dialogs are
		// expensive to instantiate and the DOM stays cached.
		if (this.attachments_dialog) {
			this.attachments_dialog.show();
			this.refresh_attachments_dialog();
			return;
		}

		const me = this;
		this.attachments_dialog = new frappe.ui.Dialog({
			title: __("Attachments"),
			size: "large",
			fields: [{ fieldtype: "HTML", fieldname: "list" }],
			secondary_action_label: __("Open File List"),
			secondary_action() {
				frappe.set_route("List", "File", {
					attached_to_doctype: me.frm.doctype,
					attached_to_name: me.frm.docname,
				});
			},
		});

		this.refresh_attachments_dialog();
		this.attachments_dialog.show();
	}

	refresh_attachments_dialog() {
		if (!this.attachments_dialog) return;
		const attachments = this.get_attachments();
		const $body = this.attachments_dialog
			.get_field("list")
			.$wrapper.empty()
			.addClass("compact-attachments-modal");
		this.attachments_dialog.set_title(
			__("Attachments ({0})", [attachments.length])
		);

		if (!attachments.length) {
			$body.append(
				`<div class="text-muted text-center" style="padding:40px 0;">
					${__("No attachments yet")}
				</div>`
			);
			return;
		}

		const $list = $('<div class="att-modal-list"></div>').appendTo($body);
		attachments.forEach((att) => $list.append(this.make_modal_row(att)));
	}

	make_modal_row(att) {
		const file_url = this.get_file_url(att);
		const safe_name = frappe.utils.escape_html(att.file_name || "");
		const type_label = att.type_def.label || att.extension.toUpperCase() || __("File");
		const $row = $(`
			<div class="att-modal-row" data-fileid="${frappe.utils.escape_html(att.name)}">
				<div class="att-modal-thumb"></div>
				<div class="att-modal-meta">
					<a class="att-modal-name" href="${file_url}" target="_blank" rel="noopener">${safe_name}</a>
					<div class="att-modal-sub">
						<span class="badge att-badge ${att.type_def.cls || "gen"}">${frappe.utils.escape_html(
			type_label
		)}</span>
						${att.is_private ? `<span class="att-private">🔒 ${__("Private")}</span>` : ""}
					</div>
				</div>
				<div class="att-modal-actions">
					<a class="btn btn-default btn-xs" href="${file_url}" download
						title="${__("Download")}">⤓</a>
					<button class="btn btn-default btn-xs att-modal-delete"
						title="${__("Delete attachment")}">×</button>
				</div>
			</div>
		`);

		// Thumbnail (image) or coloured tile (other types)
		const $thumb = $row.find(".att-modal-thumb");
		if (att.type_def.kind === "image") {
			$thumb.append(`<img src="${file_url}" alt="" class="att-modal-img"/>`);
		} else {
			$thumb.append(`
				<div class="att-file ${att.type_def.cls || "gen"}" style="width:48px;height:48px;border-radius:8px;">
					<span class="ext" style="font-size:11px;">${frappe.utils.escape_html(type_label)}</span>
				</div>
			`);
		}

		// Click on the thumb opens preview (PDF native or new tab)
		$thumb.css("cursor", "pointer").on("click", () => this.handle_thumb_click(att));

		// Delete with confirm — defers to native frm.attachments.remove_attachment
		const can_delete =
			this.frm.attachments &&
			typeof this.frm.attachments.can_delete_attachment === "function"
				? this.frm.attachments.can_delete_attachment()
				: this.frm.has_perm && this.frm.has_perm("write");

		if (!can_delete) {
			$row.find(".att-modal-delete").prop("disabled", true).attr("title", __("No permission"));
		} else {
			$row.find(".att-modal-delete").on("click", () => {
				frappe.confirm(__("Are you sure you want to delete the attachment?"), () => {
					this.frm.attachments.remove_attachment(att.name, () => {
						this.refresh_attachments_dialog();
						this.refresh();
					});
				});
			});
		}

		return $row;
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
		const users = this.get_assigned_users();
		if (!users.length) {
			$av.hide();
			return;
		}
		$av.show();
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

		// Count shown as a small clay badge in the chip's top-right corner,
		// ONLY when >= 1 (never a "0"). Empty chips get `.is-empty` (greyed
		// icon) but stay clickable so the action (tag, comment...) is reachable.
		const set_count = (cls, n) => {
			const $chip = this.$wrapper.find(cls);
			$chip.find(".count").text(n > 0 ? (n > 99 ? "99+" : n) : "");
			$chip.toggleClass("is-empty", n === 0);
		};
		set_count(".meta-attachments", attachments.length);
		set_count(".meta-comments", comments.length);
		set_count(".meta-tags", tags.length);
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
		// The native sidebar (where Frappe renders the TagEditor) is now
		// hidden by compact mode, so we open a dedicated dialog and
		// reparent the TagEditor's awesomplete input into it. That keeps
		// all existing tag CRUD logic intact (add via Enter, remove via
		// the × on each pill) without rewriting the editor.
		this.show_tags_dialog();
	}

	show_tags_dialog() {
		if (this.tags_dialog) {
			this.refresh_tags_dialog();
			this.tags_dialog.show();
			return;
		}

		const me = this;
		this.tags_dialog = new frappe.ui.Dialog({
			title: __("Tags"),
			fields: [{ fieldtype: "HTML", fieldname: "body" }],
		});

		this.refresh_tags_dialog();
		this.tags_dialog.show();

		// Refocus the input after the dialog has finished rendering.
		this.tags_dialog.$wrapper.on("shown.bs.modal", () => {
			const $input = me.tags_dialog
				.get_field("body")
				.$wrapper.find("input.tags-input")
				.first();
			if ($input.length) setTimeout(() => $input.trigger("focus"), 80);
		});
	}

	refresh_tags_dialog() {
		if (!this.tags_dialog) return;
		const $body = this.tags_dialog
			.get_field("body")
			.$wrapper.empty()
			.addClass("compact-tags-modal");

		// Pass an EMPTY .form-tags container: frappe.ui.TagEditor (via
		// frappe.ui.Tags) builds its OWN input (`input.tags-input`) + pills
		// inside it. The previous code pre-built a `<ul.tags-list>` with an
		// `input.tag-input` (singular) — so TWO inputs ended up in the DOM,
		// the user typed into the dead pre-built one and Enter did nothing.
		// frappe.ui.Tags drops its "+" placeholder button into a
		// `.form-sidebar-items` inside .form-tags and appends the input/pills
		// to .form-tags itself, so that wrapper must exist (an empty
		// .form-tags would swallow the placeholder appendTo and the field
		// would render nothing usable).
		const html = `
			<div class="form-tags">
				<div class="form-sidebar-items"></div>
			</div>
			<div class="text-muted small" style="margin-top:8px;">
				${__("Press Enter to add a tag. Click × on a pill to remove it.")}
			</div>
		`;
		$body.html(html);

		// TagEditor builds the real input (input.tags-input) + awesomplete +
		// persistence (`_user_tags`). We then activate() the Tags widget so the
		// input is shown immediately (skip the "+" click) and the user can type
		// straight away — Enter / focusout commit the tag.
		this.dialog_tags = new frappe.ui.TagEditor({
			parent: $body.find(".form-tags"),
			frm: this.frm,
			on_change: () => this.render_chips(this.get_attachments()),
		});
		this.dialog_tags.refresh(this.frm.get_docinfo().tags || "");
		if (this.dialog_tags.tags && this.dialog_tags.tags.activate) {
			this.dialog_tags.tags.activate();
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

//// Neoffice ▲▲▲ form-header-compact (END OF NEOFFICE MODULE)
////////////////////////////////////////////////////////////////////////
