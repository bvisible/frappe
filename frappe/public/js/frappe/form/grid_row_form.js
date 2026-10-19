export default class GridRowForm {
	constructor(opts) {
		$.extend(this, opts);
		this.wrapper = $('<div class="form-in-grid"></div>').appendTo(this.row.wrapper);
	}
	//// Neoffice — added wrapper, no upstream equivalent. Upstream builds the row form while it is attached
	//// to the page: each visible control (a long child doctype such as a Sales Invoice Item has ~100
	//// controls, ~45 of them shown) interleaves DOM writes and style reads (jQuery show/hide, label and
	//// description writes) and forces a style recalculation of the page every time. Measured on a draft
	//// invoice: 610 ms attached, 310 ms in a display:none container, 60 ms detached; the DOM that comes
	//// out is identical (99 controls, same shown/hidden state, sizes, editors, sections). The form is
	//// therefore built out of the document and put back where it was, whatever happens in between.
	//// TO REVIEW at the merge: if upstream builds the layout detached or lazily, drop this wrapper and
	//// rename render_content() back to render().
	render() {
		const wrapper = this.wrapper[0];
		const parent = wrapper.parentNode;
		const next_sibling = wrapper.nextSibling;
		if (parent) parent.removeChild(wrapper);
		try {
			this.render_content();
		} finally {
			if (parent) parent.insertBefore(wrapper, next_sibling);
		}
		// make_form() writes the row number through row.wrapper, which does not contain the form while it
		// is detached: write it again now that the form is back.
		this.row.set_row_index();
	}
	//// Neoffice — upstream: this method is `render()`, see the wrapper above. Body unchanged.
	render_content() {
		var me = this;
		this.make_form();
		this.form_area.empty();
		frappe.utils.scroll_to(0, false, 0, this.wrapper.find(".grid-form-body"));

		this.layout = new frappe.ui.form.Layout({
			fields: this.row.docfields,
			body: this.form_area,
			no_submit_on_enter: true,
			frm: this.row.frm,
			grid: this.row.grid,
			grid_row: this.row,
			grid_row_form: this,
		});
		this.layout.make();

		this.fields = this.layout.fields;
		this.fields_dict = this.layout.fields_dict;

		this.layout.refresh(this.row.doc);

		// copy get_query to fields
		for (var fieldname in this.row.grid.fieldinfo || {}) {
			var fi = this.row.grid.fieldinfo[fieldname];
			$.extend(me.fields_dict[fieldname], fi);
		}

		this.toggle_add_delete_button_display(this.wrapper);

		this.row.grid.open_grid_row = this;

		this.set_focus();
	}
	make_form() {
		if (!this.form_area) {
			let template = `<div class="grid-form-heading">
				<div class="toolbar grid-header-toolbar">
					<span class="panel-title">
						${__("Editing Row")} #<span class="grid-form-row-index"></span></span>
					<span class="row-actions">
						<button class="btn btn-secondary btn-sm pull-right grid-collapse-row">
							${frappe.utils.icon("down")}
						</button>
						<button class="btn btn-secondary btn-sm pull-right grid-move-row hidden-xs">
							${__("Move")}</button>
						<button class="btn btn-secondary btn-sm pull-right grid-duplicate-row hidden-xs">
							${frappe.utils.icon("duplicate")}
							${__("Duplicate")}
						</button>
						<button class="btn btn-secondary btn-sm pull-right grid-insert-row hidden-xs">
							${__("Insert Above")}</button>
						<button class="btn btn-secondary btn-sm pull-right grid-insert-row-below hidden-xs">
							${__("Insert Below")}</button>
						<button class="btn btn-danger btn-sm pull-right grid-delete-row">
							${frappe.utils.icon("delete")}
						</button>
					</span>
				</div>
			</div>
			<div class="grid-form-body">
				<div class="form-area"></div>
				<div class="grid-footer-toolbar hidden-xs flex justify-between">
					<div class="grid-shortcuts">
						<span> ${frappe.utils.icon("keyboard", "md")} </span>
						<span class="text-medium"> ${__("Shortcuts")}: </span>
						<kbd>${__("Ctrl + Up")}</kbd> . <kbd>${__("Ctrl + Down")}</kbd> . <kbd>${__("ESC")}</kbd>
					</div>
					<span class="row-actions">
						<button class="btn btn-secondary btn-sm pull-right grid-append-row">
							${__("Insert Below")}
						</button>
					</span>
				</div>
			</div>`;

			$(template).appendTo(this.wrapper);
			//// Neoffice — upstream: `this.form_area = this.wrapper.find(".form-area");` (4c842a98fc, 2023-10-30
			//// "First change v15"; the commit gives no rationale): the row's item_name is added as a CSS class on
			//// the expanded row form, presumably to style a given article. It applies to EVERY child table of
			//// every doctype (on rows without an item_name, addClass(undefined) is a no-op), and an item name
			//// with a space becomes several classes. TO REVIEW at the merge; the upstream line is kept in the
			//// trailing comment.
			this.form_area = this.wrapper.find(".form-area").addClass(this.row.doc.item_name); //// this.form_area = this.wrapper.find(".form-area");
			this.row.set_row_index();
			this.set_form_events();
		}
	}
	set_form_events() {
		var me = this;
		this.wrapper.find(".grid-delete-row").on("click", function () {
			me.row.remove();
			return false;
		});
		this.wrapper.find(".grid-insert-row").on("click", function () {
			me.row.insert(true);
			return false;
		});
		this.wrapper.find(".grid-insert-row-below").on("click", function () {
			me.row.insert(true, true);
			return false;
		});
		this.wrapper.find(".grid-duplicate-row").on("click", function () {
			me.row.insert(true, true, true);
			return false;
		});
		this.wrapper.find(".grid-move-row").on("click", function () {
			me.row.move();
			return false;
		});
		this.wrapper.find(".grid-append-row").on("click", function () {
			me.row.toggle_view(false);
			me.row.grid.add_new_row(me.row.doc.idx + 1, null, true);
			return false;
		});
		this.wrapper.find(".grid-form-heading, .grid-footer-toolbar").on("click", function () {
			me.row.toggle_view();
			return false;
		});
	}
	toggle_add_delete_button_display($parent) {
		$parent.find(".row-actions, .grid-append-row").toggle(this.row.grid.is_editable());
	}
	refresh_field(fieldname) {
		const field = this.fields_dict[fieldname];
		if (!field) return;

		field.docname = this.row.doc.name;
		field.refresh();
		this.layout && this.layout.refresh_dependency();
	}
	set_focus() {
		// wait for animation and then focus on the first row
		var me = this;
		setTimeout(function () {
			if ((me.row.frm && me.row.frm.doc.docstatus === 0) || !me.row.frm) {
				var first = me.form_area.find("input:first");
				if (
					first.length &&
					!["Date", "Datetime", "Time"].includes(first.attr("data-fieldtype"))
				) {
					try {
						first.get(0).focus();
					} catch (e) {
						//
					}
				}
			}
		}, 500);
	}
}
