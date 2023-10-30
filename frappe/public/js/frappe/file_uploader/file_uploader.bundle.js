import { createApp } from "vue";
import FileUploaderComponent from "./FileUploader.vue";
import { watch } from "vue";

class FileUploader {
	constructor({
		wrapper,
		method,
		on_success,
		doctype,
		docname,
		fieldname,
		files,
		folder,
		restrictions = {},
		upload_notes,
		allow_multiple,
		as_dataurl,
		disable_file_browser,
		dialog_title,
		attach_doc_image,
		frm,
		make_attachments_public,
	} = {}) {
		frm && frm.attachments.max_reached(true);

		if (!wrapper) {
			this.make_dialog(dialog_title, is_private); //// added is_private
		} else {
			this.wrapper = wrapper.get ? wrapper.get(0) : wrapper;
		}

		if (restrictions && !restrictions.allowed_file_types) {
			// apply global allow list if present
			let allowed_extensions = frappe.sys_defaults?.allowed_file_extensions;
			if (allowed_extensions) {
				restrictions.allowed_file_types = allowed_extensions
					.split("\n")
					.map((ext) => `.${ext}`);
			}
		}

		let app = createApp(FileUploaderComponent, {
			show_upload_button: !Boolean(this.dialog),
			doctype,
			docname,
			fieldname,
			method,
			folder,
			on_success,
			restrictions,
			upload_notes,
			allow_multiple,
			as_dataurl,
			disable_file_browser,
			attach_doc_image,
			make_attachments_public,
		});
		SetVueGlobals(app);
		this.uploader = app.mount(this.wrapper);

		if (!this.dialog) {
			this.uploader.wrapper_ready = true;
		}

		watch(
			() => this.uploader.files,
			(files) => {
				let all_private = files.every((file) => file.private);
				////commented
				/*if (this.dialog) {
					this.dialog.set_secondary_action_label(
						all_private ? __("Set all public") : __("Set all private")
					);
				}*/
				////
			},
			{ deep: true }
		);

		watch(
			() => this.uploader.trigger_upload,
			(trigger_upload) => {
				if (trigger_upload) {
					this.upload_files();
				}
			}
		);

		watch(
			() => this.uploader.close_dialog,
			(close_dialog) => {
				if (close_dialog) {
					this.dialog && this.dialog.hide();
				}
			}
		);

		watch(
			() => this.uploader.hide_dialog_footer,
			(hide_dialog_footer) => {
				if (hide_dialog_footer) {
					this.dialog && this.dialog.footer.addClass("hide");
					this.dialog.$wrapper.data("bs.modal")._config.backdrop = "static";
				} else {
					this.dialog && this.dialog.footer.removeClass("hide");
					this.dialog.$wrapper.data("bs.modal")._config.backdrop = true;
				}
			}
		);

		if (files && files.length) {
			this.uploader.add_files(files);
		}
	}

	upload_files(is_private) { //// added is_private
		this.dialog && this.dialog.get_primary_btn().prop("disabled", true);
		this.dialog && this.dialog.get_secondary_btn().prop("disabled", true);
		return this.uploader.upload_files();
	}

	make_dialog(title, is_private) { //// added is_private
		this.dialog = new frappe.ui.Dialog({
			title: title || __("Upload"),
			primary_action_label: __("Upload"),
			primary_action: () => this.upload_files(is_private), //// added is_private
			////commented
			/*secondary_action_label: __("Set all private"),
			secondary_action: () => {
				this.uploader.toggle_all_private();
			},*/
			////
			on_page_show: () => {
				this.uploader.wrapper_ready = true;
			},
		});

		this.wrapper = this.dialog.body;
		this.dialog.show();
		this.dialog.$wrapper.on("hidden.bs.modal", function () {
			$(this).data("bs.modal", null);
			$(this).remove();
		});

		//// added
		const switch_file_public_html = '<div data-tooltip-bottom="' + __("Change file visibility: Private => only visible by Neoffice user system. Public => visible by anyone (can be indexed by Google)") + '" class="switch_interface" > <span class="view_interface_span_pre"></span> <div class="switches-container"> <input type="radio" id="switchPrivate" name="switch" value="Private" checked="checked" /> <input type="radio" id="switchPublic" name="switch" value="Public" /> <label for="switchPrivate">' + __("Private") + '</label> <label for="switchPublic">' + __("Public") + '</label> <div class="switch-wrapper"> <div class="switch"> <div>' + __("Private") + '</div> <div>' + __("Public") + '</div> </div> </div> </div> </div>';
		this.dialog.footer.prepend(switch_file_public_html);

		$(this.dialog.footer[0].children[0]).find('label').on('click', function () {
			let switch_private = $("#switchPrivate").prop('checked')
			$("#switchPublic").prop('checked', switch_private);
			$("#switchPrivate").prop('checked', !switch_private);
			is_private = !switch_private;
		});

		if(frappe.router.current_route[1] == 'Item'){
			console.log('should public')
			setTimeout(function () {
				$("#switchPublic").prop('checked', true);
				$("#switchPrivate").prop('checked', false);
				is_private = false;
			}, 1000);
		}
		////
	}
}

frappe.provide("frappe.ui");
frappe.ui.FileUploader = FileUploader;
export default FileUploader;
