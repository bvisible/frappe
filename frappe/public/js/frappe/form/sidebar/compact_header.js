// Copyright (c) 2026, Frappe Technologies Pvt. Ltd. and Contributors
// MIT License. See license.txt
//
// Form Compact Header — gallery of typed attachment thumbnails injected
// into .page-actions of a form page. Phase 1: gallery only (sidebar
// remains visible). Scoped strictly to .page-form pages — no impact on
// list, workspace, print or report views.

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

frappe.ui.form.CompactHeader = class CompactHeader {
	constructor(opts) {
		Object.assign(this, opts);
		this.max_visible = 4;
		this.make();
	}

	make() {
		const $page_actions = this.frm.page.$wrapper.find(".page-actions").first();
		if (!$page_actions.length) return;

		// Idempotent: remove any existing cluster from a previous render
		$page_actions.find(".form-meta-cluster").remove();

		this.$wrapper = $(`
			<div class="form-meta-cluster">
				<div class="meta-gallery"></div>
			</div>
		`);

		// Inject as the first child of .page-actions, before .form-viewers
		$page_actions.prepend(this.$wrapper);

		this.refresh();
	}

	refresh() {
		if (!this.$wrapper) return;

		// Hide on local (unsaved) docs — same behaviour as native sidebar
		if (this.frm.doc.__islocal) {
			this.$wrapper.hide();
			return;
		}

		const attachments = this.get_attachments();
		if (!attachments.length) {
			this.$wrapper.hide();
			return;
		}

		this.$wrapper.show();
		this.render_gallery(attachments);
	}

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

		$btn.on("click", () => this.handle_click(att));
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

	handle_click(att) {
		const file_url = this.get_file_url(att);

		// PDF — use Frappe's native preview if available
		if (att.extension === "pdf" && typeof frappe.preview_pdf === "function") {
			frappe.preview_pdf(file_url);
			return;
		}

		// Default: open in a new tab
		window.open(file_url, "_blank", "noopener");
	}

	show_all_attachments() {
		// Phase 1: redirect to File list filtered on this document.
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
};
