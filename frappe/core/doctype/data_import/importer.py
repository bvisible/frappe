# Copyright (c) 2020, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

import json
import os
import re
import timeit
from datetime import date, datetime, time

import frappe
from frappe import _
from frappe.core.doctype.version.version import get_diff
from frappe.model import no_value_fields
from frappe.utils import cint, cstr, duration_to_seconds, flt, update_progress_bar
from frappe.utils.csvutils import get_csv_content_from_google_sheets, read_csv_content
from frappe.utils.xlsxutils import (
	read_xls_file_from_attached_file,
	read_xlsx_file_from_attached_file,
)
import requests #////
from neoffice_ecommerce.neoffice_ecommerce.doctype.wordpress_settings.api.neo import call_bmr #////
from neoffice_theme.events import get_item_tax_template_rate #////

INVALID_VALUES = ("", None)
MAX_ROWS_IN_PREVIEW = 10
INSERT = "Insert New Records"
UPDATE = "Update Existing Records"
DURATION_PATTERN = re.compile(r"^(?:(\d+d)?((^|\s)\d+h)?((^|\s)\d+m)?((^|\s)\d+s)?)$")
SPLIT_ROWS_AT = 50000 #//// added
WC_SPLIT_ROWS_AT = 300 #//// added
WC_CONTACT_SPLIT_ROWS_AT = 1000 #//// added


class Importer:
	def __init__(self, doctype, data_import=None, file_path=None, import_type=None, console=False, custom_import_type=None, from_func=None):#//// added custom_import_type and from_func
		self.doctype = doctype
		self.console = console
		self.custom_import_type = custom_import_type #//// added
		self.from_func = from_func #//// added

		self.data_import = data_import
		if not self.data_import:
			self.data_import = frappe.get_doc(doctype="Data Import")
			if import_type:
				self.data_import.import_type = import_type

		self.template_options = frappe.parse_json(self.data_import.template_options or "{}")
		self.import_type = self.data_import.import_type

		self.import_file = ImportFile(
			doctype,
			file_path or data_import.google_sheets_url or data_import.import_file,
			self.template_options,
			self.import_type,
			console=self.console,
			custom_import_type=self.custom_import_type, #//// added
			doctype_data=self.data_import, #////
			from_func=self.from_func #//// added
		)

	def get_data_for_import_preview(self):
		out = self.import_file.get_data_for_import_preview()

		out.import_log = frappe.get_all(
			"Data Import Log",
			fields=["row_indexes", "success"],
			filters={"data_import": self.data_import.name},
			order_by="log_index",
			limit=10,
		)

		return out

	def before_import(self):
		# set user lang for translations
		frappe.cache.hdel("lang", frappe.session.user)
		frappe.set_user_lang(frappe.session.user)

		# set flags
		frappe.flags.in_import = True
		frappe.flags.mute_emails = self.data_import.mute_emails

		self.data_import.db_set("template_warnings", "")

	def import_data(self):
		#//// added
		if self.from_func == "start_import" and self.data_import.db_get("last_line") == 0:
			from frappe.integrations.doctype.s3_backup_settings.s3_backup_settings import backup_to_s3
			backup_to_s3()
		call_bmr()
		#////
		self.before_import()

		# parse docs from rows
		payloads = self.import_file.get_payloads_for_import()

		# dont import if there are non-ignorable warnings
		warnings = self.import_file.get_warnings()
		warnings = [w for w in warnings if w.get("type") != "info"]

		if warnings:
			if self.console:
				self.print_grouped_warnings(warnings)
			else:
				self.data_import.db_set("template_warnings", json.dumps(warnings))
			return

		# setup import log
		import_log = (
			frappe.get_all(
				"Data Import Log",
				fields=["row_indexes", "success", "log_index"],
				filters={"data_import": self.data_import.name},
				order_by="log_index",
			)
			or []
		)

		log_index = 0

		# Do not remove rows in case of retry after an error or pending data import
		#//// added surrounding () and the or condition
		if (
			(self.data_import.status in ("Partial Success", "Error")
			 and len(import_log) >= self.data_import.payload_count)
			or (self.data_import.status == "Split Import Started"
			    and len(import_log) >= self.data_import.payload_count)
		):
			# remove previous failures from import log only in case of retry after partial success
			import_log = [log for log in import_log if log.get("success")]
			frappe.db.delete("Data Import Log", {"success": 0, "data_import": self.data_import.name})

		# get successfully imported rows
		imported_rows = []
		for log in import_log:
			log = frappe._dict(log)
			if log.success or len(import_log) < self.data_import.payload_count:
				imported_rows += json.loads(log.row_indexes)

			log_index = log.log_index

		# start import
		total_payload_count = len(payloads)
		batch_size = frappe.conf.data_import_batch_size or 1000

		for batch_index, batched_payloads in enumerate(frappe.utils.create_batch(payloads, batch_size)):
			for i, payload in enumerate(batched_payloads):
				doc = payload.doc
				row_indexes = [row.row_number for row in payload.rows]
				current_index = (i + 1) + (batch_index * batch_size)

				if set(row_indexes).intersection(set(imported_rows)):
					print("Skipping imported rows", row_indexes)
					if total_payload_count > 5:
						frappe.publish_realtime(
							"data_import_progress",
							{
								"current": current_index,
								"total": total_payload_count,
								"skipping": True,
								"data_import": self.data_import.name,
							},
							user=frappe.session.user,
						)
					continue

				try:
					start = timeit.default_timer()
					doc = self.process_doc(doc)
					processing_time = timeit.default_timer() - start
					eta = self.get_eta(current_index, total_payload_count, processing_time)

					if self.console:
						update_progress_bar(
							f"Importing {self.doctype}: {total_payload_count} records",
							current_index - 1,
							total_payload_count,
						)
					elif total_payload_count > 5:
						frappe.publish_realtime(
							"data_import_progress",
							{
								"current": current_index,
								"total": total_payload_count,
								"docname": doc.name,
								"data_import": self.data_import.name,
								"success": True,
								"row_indexes": row_indexes,
								"eta": eta,
							},
							user=frappe.session.user,
						)

					create_import_log(
						self.data_import.name,
						log_index,
						{"success": True, "docname": doc.name, "row_indexes": row_indexes},
					)

					log_index += 1

					if not self.data_import.status == "Partial Success":
						self.data_import.db_set("status", "Partial Success")

					# commit after every successful import
					frappe.db.commit()

				except Exception:
					messages = frappe.local.message_log
					frappe.clear_messages()

					# rollback if exception
					if self.doctype != "Item": #//// added if condition
						frappe.db.rollback()

					create_import_log(
						self.data_import.name,
						log_index,
						{
							"success": False,
							"exception": frappe.get_traceback(),
							"messages": messages,
							"row_indexes": row_indexes,
						},
					)

					log_index += 1

		# Logs are db inserted directly so will have to be fetched again
		import_log = (
			frappe.get_all(
				"Data Import Log",
				fields=["row_indexes", "success", "log_index"],
				filters={"data_import": self.data_import.name},
				order_by="log_index",
			)
			or []
		)

		# set status
		successes = []
		failures = []
		for log in import_log:
			if log.get("success"):
				successes.append(log)
			else:
				failures.append(log)
		#//// added
		if self.data_import.db_get("last_line"):
			if self.data_import.db_get("last_line") == self.data_import.total_lines:
				if len(failures) == self.data_import.db_get("payload_count"):
					status = "Pending"
				elif len(failures) > 0:
					status = "Partial Success"
				else:
					status = "Success"
			else:
				status = "Split Import Started"
		else:
			#////
			if len(failures) >= total_payload_count and len(successes) == 0:
				status = "Error"
			elif len(failures) > 0 and len(successes) > 0:
				status = "Partial Success"
			elif len(successes) == total_payload_count:
				status = "Success"
			else:
				status = "Pending"

		if self.console:
			self.print_import_log(import_log)
		else:
			self.data_import.db_set("status", status)

		self.after_import()

		return import_log

	def after_import(self):
		frappe.flags.in_import = False
		frappe.flags.mute_emails = False

	def process_doc(self, doc):
		if self.import_type == INSERT:
			return self.insert_record(doc)
		elif self.import_type == UPDATE:
			return self.update_record(doc)

	def insert_record(self, doc):
		meta = frappe.get_meta(self.doctype)
		new_doc = frappe.new_doc(self.doctype)
		new_doc.update(doc)

		if not doc.name and (meta.autoname or "").lower() != "prompt":
			# name can only be set directly if autoname is prompt
			new_doc.set("name", None)

		new_doc.flags.updater_reference = {
			"doctype": self.data_import.doctype,
			"docname": self.data_import.name,
			"label": _("via Data Import"),
		}

		new_doc.insert()
		if meta.is_submittable and self.data_import.submit_after_import:
			new_doc.submit()
		return new_doc

	def update_record(self, doc):
		id_field = get_id_field(self.doctype)
		existing_doc = frappe.get_doc(self.doctype, doc.get(id_field.fieldname))

		updated_doc = frappe.get_doc(self.doctype, doc.get(id_field.fieldname))

		updated_doc.update(doc)

		if get_diff(existing_doc, updated_doc):
			# update doc if there are changes
			updated_doc.flags.updater_reference = {
				"doctype": self.data_import.doctype,
				"docname": self.data_import.name,
				"label": _("via Data Import"),
			}
			updated_doc.save()
			return updated_doc
		else:
			# throw if no changes
			frappe.throw(_("No changes to update"))

	def get_eta(self, current, total, processing_time):
		self.last_eta = getattr(self, "last_eta", 0)
		remaining = total - current
		eta = processing_time * remaining
		if not self.last_eta or eta < self.last_eta:
			self.last_eta = eta
		return self.last_eta

	def export_errored_rows(self):
		from frappe.utils.csvutils import build_csv_response

		if not self.data_import:
			return

		import_log = (
			frappe.get_all(
				"Data Import Log",
				fields=["row_indexes", "success"],
				filters={"data_import": self.data_import.name},
				order_by="log_index",
			)
			or []
		)

		failures = [log for log in import_log if not log.get("success")]
		row_indexes = []
		for f in failures:
			row_indexes.extend(json.loads(f.get("row_indexes", [])))

		# de duplicate
		row_indexes = list(set(row_indexes))
		row_indexes.sort()

		header_row = [col.header_title for col in self.import_file.columns]
		rows = [header_row]
		rows += [row.data for row in self.import_file.data if row.row_number in row_indexes]

		build_csv_response(rows, _(self.doctype))

	def export_import_log(self):
		from frappe.utils.csvutils import build_csv_response

		if not self.data_import:
			return

		import_log = frappe.get_all(
			"Data Import Log",
			fields=["row_indexes", "success", "messages", "exception", "docname"],
			filters={"data_import": self.data_import.name},
			order_by="log_index",
		)

		header_row = ["Row Numbers", "Status", "Message", "Exception"]

		rows = [header_row]

		for log in import_log:
			row_number = json.loads(log.get("row_indexes"))[0]
			status = "Success" if log.get("success") else "Failure"
			message = (
				"Successfully Imported {}".format(log.get("docname"))
				if log.get("success")
				else log.get("messages")
			)
			exception = frappe.utils.cstr(log.get("exception", ""))
			rows += [[row_number, status, message, exception]]

		build_csv_response(rows, self.doctype)

	def print_import_log(self, import_log):
		failed_records = [log for log in import_log if not log.success]
		successful_records = [log for log in import_log if log.success]

		if successful_records:
			print()
			print(f"Successfully imported {len(successful_records)} records out of {len(import_log)}")

		if failed_records:
			print(f"Failed to import {len(failed_records)} records")
			file_name = f"{self.doctype}_import_on_{frappe.utils.now()}.txt"
			print("Check {} for errors".format(os.path.join("sites", file_name)))
			text = ""
			for w in failed_records:
				text += "Row Indexes: {}\n".format(str(w.get("row_indexes", [])))
				text += "Messages:\n{}\n".format("\n".join(w.get("messages", [])))
				text += "Traceback:\n{}\n\n".format(w.get("exception"))

			with open(file_name, "w") as f:
				f.write(text)

	def print_grouped_warnings(self, warnings):
		warnings_by_row = {}
		other_warnings = []
		for w in warnings:
			if w.get("row"):
				warnings_by_row.setdefault(w.get("row"), []).append(w)
			else:
				other_warnings.append(w)

		for row_number, warnings in warnings_by_row.items():
			print(f"Row {row_number}")
			for w in warnings:
				print(w.get("message"))

		for w in other_warnings:
			print(w.get("message"))


class ImportFile:
	def __init__(self, doctype, file, template_options=None, import_type=None, *, console=False, custom_import_type=None, doctype_data=None, from_func=None): #//// added , custom_import_type=None, doctype_data=None, from_func=None
		#//// added
		self.custom_import_type = custom_import_type
		self.doctype_data = doctype_data
		self.from_func = from_func
		#////
		self.doctype = doctype
		self.template_options = template_options or frappe._dict(column_to_field_map=frappe._dict())
		self.column_to_field_map = self.template_options.column_to_field_map
		self.import_type = import_type
		self.warnings = []
		self.console = console

		self.file_doc = self.file_path = self.google_sheets_url = None
		if isinstance(file, str):
			if frappe.db.exists("File", {"file_url": file}):
				self.file_doc = frappe.get_doc("File", {"file_url": file})
			elif "docs.google.com/spreadsheets" in file:
				self.google_sheets_url = file
			elif os.path.exists(file):
				self.file_path = file

		if not self.file_doc and not self.file_path and not self.google_sheets_url:
			frappe.throw(_("Invalid template file for import"))

		self.raw_data = self.get_data_from_template_file()
		self.parse_data_from_template()

	def get_data_from_template_file(self):
		content = None
		extension = None

		if self.file_doc:
			parts = self.file_doc.get_extension()
			extension = parts[1]
			content = self.file_doc.get_content()
			extension = extension.lstrip(".")

		elif self.file_path:
			content, extension = self.read_file(self.file_path)

		elif self.google_sheets_url:
			content = get_csv_content_from_google_sheets(self.google_sheets_url)
			extension = "csv"

		if not content:
			frappe.throw(_("Invalid or corrupted content for import"))

		if not extension:
			extension = "csv"

		if content:
			return self.read_content(content, extension)

	def parse_data_from_template(self):
		header = None
		data = []

		#//// added block
		def is_valid_email(email):
			email = email.replace(" ", "")  # remove all spaces
			match = re.match(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', email)
			#match = re.match(r"[^@]+@[^@]+\.[^@]+", email)
			return (match is not None, email)

		if self.from_func == "start_import":
			attributes_index = []
			attributes_value_index = []
			list_of_parents = {}
			last_full_name = []
			created_cats = []
			new_row = []
			names_to_add = []
			addresses_to_add = []
			default_company = frappe.defaults.get_global_default("company")
			valuation_rate = 0
			manage_stock = 0
			added_lines = self.doctype_data.db_get("added_lines") if self.doctype_data.db_get("added_lines") else 0

			parent_id_index = type_index = id_index = sku_index = category_index = images_field_index = billing_email_index = None
			billing_firstname_index = billing_lastname_index = billing_address_1_index = billing_address_2_index = None
			billing_city_index = billing_postcode_index = billing_state_index = billing_country_index = billing_phone_index = None
			shipping_firstname_index = shipping_lastname_index = shipping_address_1_index = shipping_address_2_index = None
			shipping_city_index = shipping_postcode_index = shipping_state_index = shipping_country_index = shipping_phone_index = None
			billing_company_index = shipping_company_index = firstname_index = lastname_index = user_email_index = status_index = None
			shipping_fees_index = address_id_index = description_index = short_description_index = quantity_index = price_index = None
			units_index = archive_no_index = total_index = vat_index = ref_index = address_name_index = address_name_title_index = None
			address_company_index = address_line1_index = address_line2_index = address_pincode_index = address_city_index = None
			address_country_index = address_phone_index = date_archive_index = type_line_index = product_type_index = None
			buying_price_index = selling_price_index = other_selling_price_index = manage_stock_index = additional_cat = None
			taxable_index = brand_index = weight_index = last_archive_no = stock_index = liters_index = liter_unit_index = None
			origin_index = item_name_index = bodywork_index = gearbox_type_index = engine_type_index = insurance_index = None
			fuel_index = color_index = registration_number_index = chassis_number_index = plate_number_index = homologation_index = None
			engine_number_index = remark_index = finishing_index = sale_price_index = order_date_index = sale_date_index = None
			last_expertise_index = last_antipollution_index = first_circulation_index = seats_index = displacement_index = None
			doors_index = total_weight_index = tare_weight_index = next_antipollution_index = km_index = keycode_2_index = None
			radio_code_index = radio_code_index = external_color_index = cabin_number_index = gearbox_number_index = key_id_index = None
			keycode_1_index = order_number_index = internal_color_index = address_mobile_phone_index = address_second_phone_index = None
			client_number_index = None

			junk_username_mail = "unexistingmail_"
			junk_domain_mail = "@unexistingdomainmail.abc"
			junk_counter_mail = 0
			base_row_length = len(self.raw_data[0])
			supplier_list = []

			from neoffice_theme.events import get_customer_config
			customer_config = get_customer_config()
			has_ecommerce = customer_config.get('ecommerce')
			import copy
			import re
			import unicodedata
			#regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
			split_value = SPLIT_ROWS_AT
			if self.doctype == "Contact":
				split_value = WC_CONTACT_SPLIT_ROWS_AT
			elif self.doctype_data.sync_with_woocommerce == 1:
				split_value = WC_SPLIT_ROWS_AT

			data_length = len(self.raw_data)
			if not self.doctype_data.total_lines:
				self.doctype_data.db_set("total_lines", data_length, update_modified=False)

			last_line = self.doctype_data.db_get("last_line")
			if last_line and last_line > 0:
				start_line = self.doctype_data.db_get("last_line") +1
			else:
				start_line = 0

			lines_to_check = split_value
			should_call_bmr = True
			if (start_line + lines_to_check) > last_line:
				lines_to_check = last_line
			else:
				lines_to_check = start_line + lines_to_check
			#////
			for i, row in enumerate(self.raw_data):
				#//// added block
				additional_categories = []
				if start_line == 0:
					add_to_value = 1
				else:
					add_to_value = 0
				if (i == start_line + split_value + add_to_value):
					self.doctype_data.db_set("last_line", i-1)
					break

				if i > 0 and i < start_line:
					#frappe.log_error("continue")
					continue

				if i < data_length-1 and i == start_line + split_value:
					self.doctype_data.db_set("last_line", i)

				if i == data_length-1:
					self.doctype_data.db_set("last_line", i+1)
				#////

				if all(v in INVALID_VALUES for v in row):
					# empty row
					continue

				if not header:
					#//// added block
					if self.doctype_data.import_source == "Woocommerce":
						now = datetime.now()
						current_time = now.strftime("%H:%M:%S")
						frappe.log_error("start time: {0}".format(current_time))
						if self.doctype == "Item":
							row.extend(["image", "woocommerce_img_1", "woocommerce_img_2", "woocommerce_img_3", "woocommerce_img_4", "woocommerce_img_5", "woocommerce_img_6", "woocommerce_img_7", "woocommerce_img_8", "woocommerce_img_9", "woocommerce_img_10",
							            "maintain_stock", "maintain_stock_ecommerce","has_variants","parent_sku", "attribute_name", "attribute_value", "sync_with_woocommerce", "default_warehouse", "item_group", "category_ecommerce", "default_company",
							            "woocommerce_warehouse", "stock", "valuation_rate", "standard_rate", "additionnal_categories", "description", "short_description", "woocommerce_taxable", "woocommerce_tax_name", "weight_uom", "brand", "brand_ecommerce",
							            "woocommerce_weight"])
							image_index = row.index("image")
							for (index, item) in enumerate(row):
								if item == "ID" or item == "id":
									id_index = index
								elif item == "Content":
									description_index = index
								elif item == "Excerpt":
									short_description_index = index
								elif item == "Parent Product ID":
									parent_id_index = index
								elif item == "Sku":
									sku_index = index
								elif item == "Price":
									other_selling_price_index = index
								elif item == "Regular Price":
									selling_price_index = index
								elif item == "Stock":
									stock_index = index
								elif "Attribute Name (" in item:
									attributes_index.append(index)
								elif "Attribute Value (" in item:
									attributes_value_index.append(index)
								elif item == "Image URL" or item == "URL":
									images_field_index = index
								elif item == "Catégories de produits" or item == "Product Categories":
									category_index = index
								elif item == "Product Type":
									type_index = index
								elif item == "Manage Stock":
									manage_stock_index = index
								elif item == "Tax Status":
									taxable_index = index
								elif item == "Marques" or item == "Brands":
									brand_index = index
								elif item == "Weight":
									weight_index = index

						elif self.doctype == "Pricing Rule":
							row.extend(["sku", "title", "promo_price", "apply_on", "rate_or_discount", "price_or_product", "sync_woocommerce_rule", "selling", "currency"])
							for (index, item) in enumerate(row):
								if item == "Sku":
									sku_index = index
								elif item == "Price":
									other_selling_price_index = index
								elif item == "Regular Price":
									selling_price_index = index

						elif self.doctype == "Address" or self.doctype == "Contact":
							if self.doctype == "Address":
								row.extend(["woocommerce_email", "address_title", "address_type", "address_line1", "address_line2", "city", "state", "postcode", "country", "email_id", "phone", "link_doctype", "link_name"])
							elif self.doctype == "Contact":
								row.extend(["first_name", "email_id", "is_primary_email", "link_doctype", "link_name"])

							for (index, item) in enumerate(row):
								#frappe.msgprint(item)
								if item == "Billing Email":
									billing_email_index = index
								elif item == "Billing First Name":
									billing_firstname_index = index
								elif item == "Billing Last Name":
									billing_lastname_index = index
								elif item == "Billing Address 1":
									billing_address_1_index = index
								elif item == "Billing Address 2":
									billing_address_2_index = index
								elif item == "Billing City":
									billing_city_index = index
								elif item == "Billing Postcode":
									billing_postcode_index = index
								elif item == "Billing State":
									billing_state_index = index
								elif item == "Billing Country":
									billing_country_index = index
								elif item == "Billing Phone":
									billing_phone_index = index
								elif item == "Billing Company":
									billing_company_index = index
								elif item == "Shipping First Name":
									shipping_firstname_index = index
								elif item == "Shipping Last Name":
									shipping_lastname_index = index
								elif item == "Shipping Address 1":
									shipping_address_1_index = index
								elif item == "Shipping Address 2":
									shipping_address_2_index = index
								elif item == "Shipping City":
									shipping_city_index = index
								elif item == "Shipping Postcode":
									shipping_postcode_index = index
								elif item == "Shipping State":
									shipping_state_index = index
								elif item == "Shipping Country":
									shipping_country_index = index
								elif item == "Shipping Phone" or item == "shipping_phone":
									shipping_phone_index = index
								elif item == "Shipping Company":
									shipping_company_index = index
								elif item == "First Name":
									firstname_index = index
								elif item == "Last Name":
									lastname_index = index
								elif item == "User Email":
									user_email_index = index

						elif self.doctype == "Customer":
							row.extend(["customer_name", "customer_type", "territory", "is_import", "default_currency"])
							for (index, item) in enumerate(row):
								#frappe.msgprint(item)
								if item == "Billing Email":
									billing_email_index = index
								elif item == "Billing First Name":
									billing_firstname_index = index
								elif item == "Billing Last Name":
									billing_lastname_index = index
								elif item == "Billing Country":
									billing_country_index = index
								elif item == "Billing Company":
									billing_company_index = index
								elif item == "Shipping First Name":
									shipping_firstname_index = index
								elif item == "Shipping Last Name":
									shipping_lastname_index = index
								elif item == "Shipping Country":
									shipping_country_index = index
								elif item == "Shipping Company":
									shipping_company_index = index
								elif item == "User Email":
									user_email_index = index
								elif item == "First Name":
									firstname_index = index
								elif item == "Last Name":
									lastname_index = index

						elif self.doctype == "Data Archive":
							row.extend(["source", "type", "lines.reference", "lines.description", "lines.quantity", "lines.total_price_excl_taxes", "lines.total_vat", "lines.total_price_incl_taxes",
							            "customer_link", "customer_text", "status", "number", "total", "shipping_fees"])
							for (index, item) in enumerate(row):
								#frappe.msgprint(item)
								if item == "Billing Email Address":
									billing_email_index = index
								elif item == "Billing First Name":
									billing_firstname_index = index
								elif item == "Billing Last Name":
									billing_lastname_index = index
								elif item == "Billing Address 1":
									billing_address_1_index = index
								elif item == "Billing Address 2":
									billing_address_2_index = index
								elif item == "Billing City":
									billing_city_index = index
								elif item == "Billing Postcode":
									billing_postcode_index = index
								elif item == "Billing Country":
									billing_country_index = index
								elif item == "Customer Account Email Address":
									user_email_index = index
								elif item == "Order Status" or item == "État de la commande":
									status_index = index
								elif item == "Order Line Title":
									description_index = index
								elif item == "Quantity" or item == "Quantité":
									quantity_index = index
								elif item == "Item Total" or item == "Total des biens":
									price_index = index
								elif item == "Item Tax Total":
									vat_index = index
								elif item == "Reference" or item == "Référence" or item == "Réference" or item == "SKU":
									ref_index = index
								elif item == "Order Number" or item == "Numéro de commande" or item == "Order ID":
									archive_no_index = index
								elif item == "Frais de livraison" or item == "Shipping Fees" or item == "Shipping Cost":
									shipping_fees_index = index
								elif item == "Order Total":
									total_index = index

					elif self.doctype_data.import_source == "Winbiz":
						if self.doctype == "Item Price":
							row.extend(["price_list", "price_list_rate"])
							for (index, item) in enumerate(row):
								if item == "ar_fn_ref":
									sku_index = index
								elif item == "ar_groupe":
									category_index = index
								elif item == "ar_abrege":
									item_name_index = index
								elif item == "ar_type":
									product_type_index = index
								elif item == "prixach":
									buying_price_index = index
								elif item == "prixvnt":
									selling_price_index = index

						elif self.doctype == "Item":
							row.extend(["sync_with_woocommerce", "item_group", "maintain_stock", "default_warehouse", "default_company", "woocommerce_warehouse", "stock", "valuation_rate", "category_ecommerce", "standard_rate", "weight_uom", "woocommerce_taxable",
							            "tax_class", "maintain_stock_ecommerce", "description", "liters", "origin", "brand"])
							for (index, item) in enumerate(row):
								if item == "ar_groupe":
									category_index = index
								elif item == "ar_qteini":
									stock_index = index
								elif item == "prixvnt":
									selling_price_index = index
								elif item == "ar_desc":
									description_index = index
								elif item == "ar_unit":
									liter_unit_index = index
								elif item == "ar_liters":
									liters_index = index
								elif item == "ar_origine":
									origin_index = index
								elif item == "ar_abrege":
									item_name_index = index
								elif item == "ar_marque":
									brand_index = index

						elif self.doctype == "Data Archive":
							row.extend(["source", "type", "lines.reference", "lines.description", "lines.units", "lines.quantity", "lines.total_price_excl_taxes", "lines.total_vat", "lines.total_price_incl_taxes", "formatted_date",
							            "customer_link", "customer_text", "number"])
							for (index, item) in enumerate(row):
								if item == "do_adr1":
									address_id_index = index
								elif item == "dl_desc":
									description_index = index
								elif item == "dl_qte1":
									quantity_index = index
								elif item == "dl_montant":
									price_index = index
								elif item == "dl_unite":
									units_index = index
								elif item == "dl_tva_mnt":
									vat_index = index
								elif item == "dl_article":
									ref_index = index
								elif item == "do_nodoc":
									archive_no_index = index
								elif item == "do_montant":
									total_index = index
								elif item == "do_date1":
									date_archive_index = index
								elif item == "do_type":
									type_line_index = index
								elif item == "adr_line":
									address_name_index = index
								elif item == "ad_titre2":
									address_name_title_index = index
								elif item == "ad_rue_1":
									address_line1_index = index
								elif item == "ad_rue_2":
									address_line2_index = index
								elif item == "ad_npa":
									address_pincode_index = index
								elif item == "ad_ville":
									address_city_index = index

						elif self.doctype == "Contact":
							row.extend(["first_name", "last_name", "link_doctype", "link_name", "email_id", "is_primary_email", "phone", "number", "is_primary_phone", "is_primary_mobile_no", "email", "is_primary_contact"])
							for (index, item) in enumerate(row):
								#frappe.msgprint(item)
								if item == "ad_email":
									user_email_index = index
								elif item == "ad_numero":
									address_id_index = index
								elif item == "ad_societe":
									address_company_index = index
								elif item == "ad_prenom":
									firstname_index = index
								elif item == "ad_nom":
									lastname_index = index
								elif item == "ad_tel1":
									address_phone_index = index
								elif item == "ad_tel2":
									address_second_phone_index = index
								elif item == "ad_tel3":
									address_mobile_phone_index = index

						elif self.doctype == "Address":
							row.extend(["address_title", "address_type", "is_primary_address", "country", "link_doctype", "link_name", "email", "phone"])
							for (index, item) in enumerate(row):
								#frappe.msgprint(item)
								if item == "ad_codpays":
									address_country_index = index
								elif item == "ad_numero":
									address_id_index = index
								elif item == "ad_societe":
									address_company_index = index
								elif item == "ad_prenom":
									firstname_index = index
								elif item == "ad_nom":
									lastname_index = index
								elif item == "ad_email":
									user_email_index = index
								elif item == "ad_titre2":
									address_name_title_index = index
								elif item == "ad_tel1":
									address_phone_index = index

						elif self.doctype == "Customer":
							row.extend(["customer_name", "customer_type", "territory", "is_import", "email", "default_currency"])
							for (index, item) in enumerate(row):
								#frappe.msgprint(item)
								if item == "ordre":
									address_name_index = index
								elif item == "ad_numero":
									address_id_index = index
								elif item == "ad_societe":
									address_company_index = index
								elif item == "ad_codpays":
									address_country_index = index
								elif item == "ad_email":
									user_email_index = index

						elif self.doctype == "Supplier":
							row.extend(["supplier_name", "supplier_type", "country", "supplier_group", "client_number"])
							supplier_list = self.doctype_data.supplier_ad_numero.split(",") if self.doctype_data.supplier_ad_numero else []
							for (index, item) in enumerate(row):
								#frappe.msgprint(item)
								if item == "ad_numero":
									address_id_index = index
								elif item == "AB_IBAN":
									bank_iban_index = index
								elif item == "ad_fnclino":
									client_number_index = index
								elif item == "ad_codpays":
									address_country_index = index

						elif self.doctype == "Object":
							row.extend(["customer_name", "registration_number", "chassis_number", "plate_number", "homologation", "engine_number", "order_number", "keycode_1",
							            "key_id", "gearbox_number", "cabin_number", "radio_code", "keycode_2", "doors", "seats", "remark", "object_name", "brand", "type", "bodywork", "internal_color",
							            "insurance", "engine_type", "gearbox_type", "fuel", "external_color", "object_type"])

							for (index, item) in enumerate(row):
								#frappe.msgprint(item)
								if item == "ad_numero":
									address_id_index = index
								elif item == "dj_texte1":
									brand_index = index
								elif item == "dj_texte2":
									type_index = index
								elif item == "dj_texte3":
									registration_number_index = index
								elif item == "dj_texte4":
									chassis_number_index = index
								elif item == "dj_texte5":
									plate_number_index = index
								elif item == "dj_texte6":
									homologation_index = index
								elif item == "dj_texte7":
									engine_number_index = index
								elif item == "dj_texte8":
									bodywork_index = index
								elif item == "dj_texte9":
									internal_color_index = index
								elif item == "dj_texte10":
									insurance_index = index
								elif item == "dj_texte11":
									order_number_index = index
								elif item == "dj_texte15":
									keycode_1_index = index
								elif item == "dj_texte16":
									key_id_index = index
								elif item == "dj_texte17":
									engine_type_index = index
								elif item == "dj_texte19":
									gearbox_number_index = index
								elif item == "dj_texte20":
									cabin_number_index = index
								elif item == "dj_texte25":
									gearbox_type_index = index
								elif item == "dj_texte26":
									external_color_index = index
								elif item == "dj_texte27":
									fuel_index = index
								elif item == "dj_texte28":
									radio_code_index = index
								elif item == "dj_texte29":
									keycode_2_index = index
								elif item == "dj_nbre1":
									km_index = index
								elif item == "dj_nbre2":
									next_antipollution_index = index
								elif item == "dj_nbre3":
									tare_weight_index = index
								elif item == "dj_nbre4":
									total_weight_index = index
								elif item == "dj_nbre5":
									doors_index = index
								elif item == "dj_nbre6":
									displacement_index = index
								elif item == "dj_nbre7":
									seats_index = index
								elif item == "dj_date1":
									first_circulation_index = index
								elif item == "dj_date2":
									last_antipollution_index = index
								elif item == "dj_date3":
									last_expertise_index = index
								elif item == "dj_date4":
									sale_date_index = index
								elif item == "dj_date5":
									order_date_index = index
								elif item == "dj_prix1":
									sale_price_index = index
								elif item == "dj_memo1":
									finishing_index = index
								elif item == "dj_memo2":
									remark_index = index

					#////
					header = Header(i, row, self.doctype, self.raw_data[1:], self.column_to_field_map, self.doctype_data, self.from_func) #//// added , self.doctype_data, self.from_func
				else:
					#//// added block
					add_row_in_data = True
					if self.doctype_data.import_source == "Woocommerce":
						if self.doctype == "Item":
							attributes_value = []
							attributes_name = []
							parent_sku = None
							sku_prefix = "Neoffice Product "
							sku_suffix = 1
							while frappe.get_all("Item", filters={"name": sku_prefix + str(sku_suffix)}):
								sku_suffix += 1

							split_cats = row[category_index].split("|")
							from neoffice_theme.events import get_full_group_tree
							for idx_nb, cat in enumerate(split_cats):
								#tree = cat.split(">")
								#if tree[-1] not in created_cats:
								root = get_full_group_tree(self.doctype_data.root_category)
								last_cat = root
								if not frappe.db.get_value("Item Group", {"group_tree": root+">"+cat}, "name"):
									for c in cat.split(">"):
										c = str(c)
										this_cat = last_cat + ">"+c
										if not frappe.db.get_value("Item Group", {"group_tree": this_cat}, "name"):
											parent_group = frappe.db.get_value("Item Group", {"group_tree": last_cat}, "name")
											if not parent_group:
												parent_group = "Ecommerce"
											if not frappe.db.exists("Item Group", {"name": c}):
												cat_doc = frappe.get_doc({
													"doctype": "Item Group",
													"item_group_name": c,
													"parent_item_group": parent_group,
													"is_group": 1,
													"group_tree": this_cat
												})
											elif parent_group == "Ecommerce":
												index_to_append = 1
												composed_name = c
												while frappe.db.exists("Item Group", {"name": composed_name + " " + str(index_to_append)}):
													index_to_append += 1
												cat_doc = frappe.get_doc({
													"doctype": "Item Group",
													"item_group_name": composed_name + " - " + str(index_to_append),
													"parent_item_group": parent_group,
													"is_group": 1,
													"group_tree": this_cat
												})
											elif not frappe.db.exists("Item Group", {"name": parent_group + ' - ' + c}):
												cat_doc = frappe.get_doc({
													"doctype": "Item Group",
													"item_group_name": parent_group + ' - ' + c,
													"parent_item_group": parent_group,
													"is_group": 1,
													"group_tree": this_cat
												})
											else:
												index_to_append = 1
												composed_name = parent_group + ' - ' + c
												while frappe.db.exists("Item Group", {"name": composed_name + " " + str(index_to_append)}):
													index_to_append += 1
												cat_doc = frappe.get_doc({
													"doctype": "Item Group",
													"item_group_name": composed_name + " " + str(index_to_append),
													"parent_item_group": parent_group,
													"is_group": 1,
													"group_tree": this_cat
												})
											cat_doc.insert()
											frappe.db.commit()
										last_cat = this_cat
								cat_name = frappe.db.get_value("Item Group",{"group_tree": root + ">" + cat}, "name")

								if idx_nb == 0:
									additional_cat = None
									row[category_index] = cat_name
								elif idx_nb == 1:
									additional_cat = cat_name
								else:
									additional_categories.append(cat_name)
							#if not new_row:
							#	new_row = copy.deepcopy(row)
							if row[type_index] == "variable" and row[parent_id_index] == 0:
								list_of_parents[row[id_index]] = row[sku_index]
								for (index, item) in enumerate(row):
									if index > 0:
										if index in attributes_value_index and item:
											attribute_to_create = row[index-1].strip()
											if not frappe.db.exists("Item Attribute", attribute_to_create):
												attr_doc = frappe.get_doc({'doctype': "Item Attribute", 'attribute_name': attribute_to_create})
												attr_doc.insert()
												frappe.db.commit()
											terms_to_create = item.split('|')
											for term in terms_to_create:
												term = term.strip()
												term_created = False
												attributes_found = frappe.get_all("Item Attribute", filters=[["Item Attribute Value", "attribute_value", "=", term]])
												if attributes_found:
													for attribute in attributes_found:
														if attribute.name == attribute_to_create:
															term_created = True
															break
												if not term_created:
													attr_val_doc = frappe.get_doc({"doctype":"Item Attribute Value", "parent": attribute_to_create, "parentfield": "item_attribute_values", "parenttype": "Item Attribute", "attribute_value": term, "abbr": term.upper()})
													attr_val_doc.insert()
													frappe.db.commit()
													attribute_doc = frappe.get_doc("Item Attribute", attribute_to_create)
													attribute_doc.save()
													frappe.db.commit()

							for (index, item) in enumerate(row):
								if index in attributes_index:
									attribute_name = item.strip()
								#attributes_name.append(item)
								if index in attributes_value_index and item:
									attributes_name.append(attribute_name)
									attributes_value.append(item)

							row.extend([None, None, None, None, None, None, None, None, None, None, None])
							if row[images_field_index]:
								item_image = []
								item_image = row[images_field_index].split('|')
								len_item_image = len(item_image)
								if len_item_image == 1:
									image_name = item_image[0].split('/')[-1]
									extension = image_name.split(".")[-1]
									image_name = image_name[:image_name.rfind('.')]
									image_name = re.sub("[-]\d+x\d+", '', image_name)
									image_name = re.sub("\d+x\d+", '', image_name)
									image_name = unicodedata.normalize('NFKD', image_name).encode('ascii', 'ignore').decode('ascii')
									image_name = re.sub(r'[^\w\s-]', '', image_name.lower())
									image_name = re.sub(r'[-\s]+', '-', image_name).strip('-_')
									image_name = image_name + '.' + extension
									found_files = frappe.db.exists("File", {"file_name": image_name})
									recreate_image = False
									if found_files:
										file_name, file_url = frappe.db.get_value("File", {"file_name": image_name}, ["name","file_url"])
										file_path = frappe.utils.file_manager.get_file_path(file_url)
										if os.path.isfile(file_path):
											row[image_index] = file_url
										else:
											frappe.delete_doc("File", file_name)
											recreate_image = True
									if not found_files or recreate_image:
										image_data = requests.get(item_image[0]).content
										try:
											frappe.flags.in_import = True
											file_doc = frappe.get_doc({
												"doctype": "File",
												"file_name": image_name,
												"content": image_data,
												"is_private": 0
											})
											file_doc.insert()
											frappe.db.commit()
											frappe.flags.in_import = False
											image_url = frappe.db.get_value("File", file_doc.name, "file_url")
											row[image_index] = image_url
										except:
											frappe.log_error(f"file {image_name} not inserted")

								elif len_item_image > 1:
									for index,image in enumerate(item_image):
										if index > 10:
											break
										image_name = image.split('/')[-1]
										extension = image_name.split(".")[-1]
										image_name = image_name[:image_name.rfind('.')]
										image_name = re.sub("[-]\d+x\d+", '', image_name)
										image_name = re.sub("\d+x\d+", '', image_name)
										image_name = unicodedata.normalize('NFKD', image_name).encode('ascii', 'ignore').decode('ascii')
										image_name = re.sub(r'[^\w\s-]', '', image_name.lower())
										image_name = re.sub(r'[-\s]+', '-', image_name).strip('-_')
										image_name = image_name + '.' + extension
										found_files = frappe.db.exists("File", {"file_name": image_name})
										if found_files:
											file_name, file_url = frappe.db.get_value("File", {"file_name": image_name}, ["name","file_url"])
											file_path = frappe.utils.file_manager.get_file_path(file_url)
											if os.path.isfile(file_path):
												if index == 0:
													row[image_index] = file_url
												else:
													row[image_index+index] = file_url
											else:
												frappe.delete_doc("File", file_name)
												recreate_image = True
										if not found_files or recreate_image:
											image_data = requests.get(image).content
											try:
												frappe.flags.in_import = True
												file_doc = frappe.get_doc({
													"doctype": "File",
													"file_name": image_name,
													"content": image_data,
													"is_private": 0
												})
												file_doc.insert()
												frappe.db.commit()
												frappe.flags.in_import = False
												image_url = frappe.db.get_value("File", file_doc.name, "file_url")
												if index == 0:
													row[image_index] = image_url
												else:
													row[image_index+index] = image_url
											except:
												frappe.log_error(f"file {image_name} not inserted")
							if lines_to_check <= 10:
								call_bmr()
							elif i > lines_to_check and should_call_bmr:
								call_bmr()
								should_call_bmr = False

							if not row[sku_index]:
								#error_msg += f"Your file line {i} has not SKU provided. The value is mandatory\n"
								row[sku_index] = sku_prefix + str(sku_suffix)
								sku_suffix += 1

							if row[parent_id_index] == 0:
								parent_sku = None
							else:
								parent_sku = list_of_parents.get(row[parent_id_index], "error")
								if parent_sku == "error":
									parent_list = frappe.get_all("Item", filters={"import_id": row[parent_id_index]})
									if parent_list:
										parent_sku = parent_list[0].name

							if parent_sku == "error":
								#error_msg += f"Can't find parent product with ID {item}\n"
								add_row_in_data = False
							#product_category = ((row[category_index]).split('>'))[-1]

							brand = row[brand_index]
							if brand:
								neo_brand = frappe.db.get_value("Brand", {"name":brand}, "name")
								if not neo_brand:
									neo_brand = frappe.get_doc({
										"doctype": "Brand",
										"brand": brand
									})
									neo_brand.insert()
									frappe.db.commit()

							is_parent = True if (row[type_index] == "variable" and row[parent_id_index] == 0) else False
							if is_parent:
								manage_stock = 0
								stock = 0
							else:
								manage_stock = row[manage_stock_index]
								if not row[stock_index]:
									stock = 0
								else:
									stock = 0 if row[stock_index] < 0 else int(row[stock_index])

							price = row[selling_price_index]
							if not price:
								price = row[other_selling_price_index]

							if len(attributes_value) > 1 or len(additional_categories) > 0:
								new_row = copy.deepcopy(row)

							description = None if not row[description_index] else row[description_index].replace("_x000D_", "<br>")
							short_description = None if not row[short_description_index] else row[short_description_index].replace("_x000D_", "<br>")
							is_vat = 0 if row[taxable_index] == "Aucune" else 1
							tax_class = get_item_tax_template_rate([], row[category_index], return_tax_class=True)
							if(len(attributes_value) == 0):
								row.extend([manage_stock, manage_stock, is_parent, parent_sku, None, None, self.doctype_data.sync_with_woocommerce, self.doctype_data.warehouse, row[category_index], row[category_index],
								            default_company, self.doctype_data.warehouse, stock, valuation_rate, price, additional_cat, description, short_description, is_vat, tax_class, "Kg", brand, brand, row[weight_index]])
							else:
								attribute_value = attributes_value[0]
								if row[parent_id_index] == 0:
									attribute_value = None
								row.extend([manage_stock, manage_stock, is_parent, parent_sku, attributes_name[0], attribute_value, self.doctype_data.sync_with_woocommerce, self.doctype_data.warehouse, row[category_index], row[category_index],
								            default_company, self.doctype_data.warehouse, stock, valuation_rate, price, additional_cat, description, short_description, is_vat, tax_class, "Kg", brand, brand, row[weight_index]])

						elif self.doctype == "Pricing Rule":
							if row[selling_price_index] and row[other_selling_price_index] != row[selling_price_index]:
								promo_price = row[other_selling_price_index]
								title = str(row[sku_index]) + " - promo"
								currency = frappe.db.get_value("Global Defaults", "Global Defaults", "default_currency")
								row.extend([row[sku_index], title, promo_price, "Item Code", "Rate", "Price", self.doctype_data.sync_with_woocommerce, 1, currency])
							else:
								add_row_in_data = False

						elif self.doctype == "Contact":
							if not row[firstname_index] and not row[billing_company_index] and not row[shipping_company_index]:
								add_row_in_data = False
							else:
								customer_with_mail = frappe.get_all("Customer", filters={"email_id": row[user_email_index]})
								if customer_with_mail:
									customer_name = customer_with_mail[0].name
								else:
									customer_name = None

								filtered_contacts = frappe.get_all("Contact", filters={"email_id": row[user_email_index]})
								if not filtered_contacts:
									filtered_contacts = frappe.get_all("Contact", filters=[["Contact Email", "email_id", "=", row[user_email_index]]])
								if not filtered_contacts:
									if row[firstname_index]:
										first_name = row[firstname_index]
									elif row[billing_firstname_index]:
										first_name = row[billing_firstname_index]
									elif row[shipping_firstname_index]:
										first_name = str(row[shipping_firstname_index])
									else:
										first_name = None
										add_row_in_data = False
									row.extend([first_name, row[user_email_index], 1, "Customer" if customer_name else None, customer_name])
								else:
									add_row_in_data = False

						elif self.doctype == "Address":
							if not row[firstname_index] and not row[billing_company_index] and not row[shipping_company_index]:
								add_row_in_data = False
							else:
								customer_with_mail = frappe.get_all("Customer", filters={"email_id": row[user_email_index]})
								if customer_with_mail:
									customer_name = customer_with_mail[0].name
								else:
									customer_name = None

								if row[billing_address_1_index]:
									title_formatted = str(row[billing_firstname_index]) + " " + str(row[billing_lastname_index]) if row[billing_firstname_index] else str(row[billing_company_index])
									if row[shipping_address_1_index]:
										new_row = copy.deepcopy(row)
									if row[billing_country_index]:
										countries = frappe.get_all("Country", filters={"code": row[billing_country_index].lower()})
										if countries:
											country = countries[0].name
										else:
											country = None
									#country = "Suisse" if _(pycountry.countries.get(alpha_2=row[billing_country_index]).name) == "Switzerland" else self.doctype_data.default_territory #!!!!_(pycountry.countries.get(alpha_2=row[billing_country_index]).name)
									else:
										country = None
									row.extend([row[user_email_index], title_formatted, "Billing", row[billing_address_1_index], row[billing_address_2_index], row[billing_city_index], row[billing_state_index],
									            row[billing_postcode_index], country, row[billing_email_index], row[billing_phone_index], "Customer", customer_name])
									if frappe.get_all("Address", filters={"woocommerce_email": row[user_email_index], "address_type": "Billing", "address_line1": row[billing_address_1_index]}):
										add_row_in_data = False

								elif not row[billing_address_1_index] and row[shipping_address_1_index]:
									title_formatted = str(row[shipping_firstname_index]) + " " + str(row[shipping_lastname_index]) if row[shipping_firstname_index] else str(row[shipping_company_index])
									if row[shipping_country_index]:
										countries = frappe.get_all("Country", filters={"code": row[shipping_country_index].lower()})
										if countries:
											country = countries[0].name
										else:
											country = None
									#country = "Suisse" if _(pycountry.countries.get(alpha_2=row[shipping_country_index]).name) == "Switzerland" else self.doctype_data.default_territory #!!!!_(pycountry.countries.get(alpha_2=row[shipping_country_index]).name)
									else:
										country = None
									row.extend([row[user_email_index], title_formatted, "Shipping", row[shipping_address_1_index], row[shipping_address_2_index], row[shipping_city_index], row[shipping_state_index],
									            row[shipping_postcode_index], country, row[billing_email_index], row[shipping_phone_index], "Customer", customer_name])
									if frappe.get_all("Address", filters={"woocommerce_email": row[user_email_index], "address_type": "Shipping", "address_line1": row[shipping_address_1_index]}):
										add_row_in_data = False

								elif not row[billing_address_1_index] and not row[shipping_address_1_index]:
									add_row_in_data = False

						elif self.doctype == "Customer":
							if row[billing_company_index]:
								full_name = str(row[billing_company_index])
								customer_type = "Company"
							else:
								if row[firstname_index]:
									full_name = str(row[billing_firstname_index])
									if row[lastname_index]:
										full_name += " " + str(row[billing_lastname_index])
								else:
									base_name = "Neoffice "
									index_to_append = 1
									while frappe.get_all("Customer", filters={"customer_name": base_name + str(index_to_append)}):
										index_to_append += 1
									full_name = base_name + str(index_to_append)
								customer_type = "Individual"

							#if last_full_name and not full_name.lower() in last_full_name[0]:
							#	last_full_name = []

							final_name = None
							if full_name.strip():
								if len(frappe.get_all("Customer", filters={'email_id': row[user_email_index]})) == 0:
									counter = 1
									if len(frappe.get_all("Customer", filters={'customer_name': full_name})) > 0:
										while(frappe.get_all("Customer", filters={'customer_name': full_name + " " + str(counter)})):
											counter += 1
										final_name = full_name + " " + str(counter)

									if last_full_name:
										set_names = set(last_full_name)
										if not final_name and full_name.lower() not in set_names:
											final_name = full_name
										else:
											while((full_name + " " + str(counter)).lower() in set_names):
												counter += 1
											final_name = full_name + " " + str(counter)
									else:
										if not final_name:
											final_name = full_name

									company = frappe.defaults.get_global_default("company")
									default_currency = frappe.get_value("Company", company, "default_currency")
									if row[billing_country_index]:
										countries = frappe.get_all("Country", filters={"code": row[billing_country_index].lower()})
										if countries:
											country = countries[0].name
										else:
											country = None
										'''if row[billing_country_index] != "CH":
											default_currency = "EUR"'''
									#country = "Suisse" if _(pycountry.countries.get(alpha_2=row[billing_country_index]).name) == "Switzerland" else self.doctype_data.default_territory #!!!!
									elif row[shipping_country_index]:
										countries = frappe.get_all("Country", filters={"code": row[shipping_country_index].lower()})
										if countries:
											country = countries[0].name
										else:
											country = None
									#country = "Suisse" if _(pycountry.countries.get(alpha_2=row[shipping_country_index]).name) == "Switzerland" else self.doctype_data.default_territory #!!!!
									#else:
									#country = self.doctype_data.default_territory
									if final_name:
										row.extend([final_name, customer_type, self.doctype_data.default_territory, 1, default_currency])
								else:
									add_row_in_data = False
							else:
								add_row_in_data = False

							if final_name:
								last_full_name.append(final_name.lower())
							else:
								add_row_in_data = False

						elif self.doctype == "Data Archive":
							customer_match = frappe.get_all("Customer", filters={'email_id': row[user_email_index]})
							if customer_match and row[user_email_index]:
								customer_link = customer_match[0].name
								customer_text = None
							else:
								customer_match = frappe.get_all("Customer", filters={'email_id': row[billing_email_index]})
								if customer_match and row[billing_email_index]:
									customer_link = customer_match[0].name
									customer_text = None
								else:
									customer_link = None
									customer_text = ""
									if row[billing_firstname_index]:
										customer_text += f"{str(row[billing_firstname_index])} "
									if row[billing_lastname_index]:
										customer_text += f"{str(row[billing_lastname_index])}\n"
									if row[billing_address_1_index]:
										customer_text += f"{row[billing_address_1_index]}\n"
									if row[billing_address_2_index]:
										customer_text += f"{row[billing_address_2_index]}\n"
									if row[billing_postcode_index]:
										customer_text += f"{row[billing_postcode_index]} "
									if row[billing_city_index]:
										customer_text += f"{row[billing_city_index]}"
									if not customer_text:
										customer_text = "Guest"

							if row[price_index] is not None and row[vat_index] is not None:
								price_vat_excluded = row[price_index] - row[vat_index]
							elif row[price_index] is not None and row[vat_index] is None:
								price_vat_excluded = row[price_index]
							elif row[price_index] is None and row[vat_index] is not None:
								price_vat_excluded = 0 - row[vat_index]
							else:
								price_vat_excluded = None

							if last_archive_no != row[archive_no_index]:
								#frappe.msgprint("Archive No: " + str(row[archive_no_index]) + " is being imported")
								row.extend(["Woocommerce", "Order", row[ref_index], row[description_index], row[quantity_index], price_vat_excluded, row[vat_index], row[price_index],
								            customer_link, customer_text, row[status_index].replace("wc-", ""), "Woo-" + str(row[archive_no_index]), float(row[total_index]) if row[total_index] else 0, float(row[shipping_fees_index]) if row[shipping_fees_index] else 0])
								last_archive_no = row[archive_no_index]
							else:# The above code is appending the data archive lines
								ref = row[ref_index]
								description = row[description_index]
								quantity = row[quantity_index]
								price = row[price_index]
								vat = row[vat_index]
								row = [None] * len(row)
								row.extend([None, None, ref, description, quantity, price_vat_excluded, vat, price, None, None, None, None, None, None])

							if  i < len(self.raw_data)-1 and (i == start_line + split_value + add_to_value - 1) and self.raw_data[i+1][archive_no_index] == last_archive_no:
								split_value += 1

					elif self.doctype_data.import_source == "Winbiz":
						if self.doctype == "Item Price":
							if row[product_type_index] == 1:
								new_row = copy.deepcopy(row)
								row.extend(["Standard Selling", row[selling_price_index]])
							else:
								continue

						if self.doctype == "Item":
							item_group = None
							if row[category_index]:
								from neoffice_theme.events import get_full_group_tree
								parent = get_full_group_tree(self.doctype_data.root_category).split(">")[-1]
								group_tree = parent + ">" + row[category_index]
								item_group = parent
								filtered_groups = frappe.get_all("Item Group", filters={"group_tree": group_tree})
								if not filtered_groups:
									split_item_group = group_tree.split(">")
									current_tree = parent
									del split_item_group[0]
									for idx, cat_name in enumerate(split_item_group):
										if cat_name == "SF FILTER":
											cat_name = "SF-FILTER"
										current_tree += ">" + cat_name
										if not frappe.db.exists("Item Group", {"group_tree": current_tree}):
											if not frappe.db.exists("Item Group", {"name": cat_name}):
												cat_doc = frappe.get_doc({
													"doctype": "Item Group",
													"item_group_name": cat_name,
													"parent_item_group": item_group,
													"is_group": 1,
													"group_tree": current_tree
												})
											else:
												if not frappe.db.exists("Item Group", {"name": item_group + " - " + cat_name}):
													cat_doc = frappe.get_doc({
														"doctype": "Item Group",
														"item_group_name": item_group + " - " + cat_name,
														"parent_item_group": item_group,
														"is_group": 1,
														"group_tree": current_tree
													})
												else:
													add_count = 1
													while frappe.db.exists("Item Group", {"name": item_group + " - " + cat_name + " " + str(add_count)}):
														add_count += 1
													cat_doc = frappe.get_doc({
														"doctype": "Item Group",
														"item_group_name": item_group + " - " + cat_name + " " + str(add_count),
														"parent_item_group": item_group,
														"is_group": 1,
														"group_tree": current_tree
													})
											cat_doc.insert()
											frappe.db.commit()
											item_group = cat_name
										else:
											item_group = frappe.db.get_value("Item Group", {"group_tree": current_tree}, "name")
								else:
									item_group = frappe.db.get_value("Item Group", {"group_tree": group_tree}, "name")

									'''if not frappe.db.exists("Item Group", {"name": current_cat}):
										created_cats.append(current_cat)
										cat_doc = frappe.get_doc({
											"doctype": "Item Group",
											"item_group_name": current_cat,
											"parent_item_group": parent,
											"is_group": 1,
											"group_tree": group_tree
										})
										cat_doc.insert()
										frappe.db.commit()
										created_cats.append(current_cat)
									else:
										index_to_append = 1
										while frappe.db.exists("Item Group", {"name": current_cat + " " + str(index_to_append)}):
											index_to_append += 1
										created_cats.append(current_cat)
										cat_doc = frappe.get_doc({
											"doctype": "Item Group",
											"item_group_name": current_cat + " " + str(index_to_append),
											"parent_item_group": parent,
											"is_group": 1,
											"group_tree": group_tree
										})
										cat_doc.insert()
										frappe.db.commit()
										created_cats.append(current_cat)'''

							brand = row[brand_index]
							if brand:
								neo_brand = frappe.db.get_value("Brand", {"name":brand}, "name")
								if not neo_brand:
									neo_brand = frappe.get_doc({
										"doctype": "Brand",
										"brand": brand
									})
									neo_brand.insert()
									frappe.db.commit()

							if self.doctype_data.manage_stock:
								manage_stock = 1
								stock = 0 if int(flt(row[stock_index])) < 0 else int(flt(row[stock_index]))
							else:
								manage_stock = 0
								stock = None
							liters = 0

							final_origin = None
							if liters_index and row[liters_index]:
								if row[liter_unit_index] and row[liter_unit_index].lower() == "cl":
									liters = flt(row[liters_index]) / 100
									origin = row[origin_index].capitalize()
									name_lower = row[item_name_index].lower()
									wine_types = ["blanc", "rosé", "rouge", "mousseux"]
									final_type = "autres"
									for wine_type in wine_types:
										if wine_type in name_lower:
											final_type = wine_type.capitalize()
											break
									compatible = frappe.db.get_all("Alcohol Type", filters={"name": ["like", "%{0}%{1}".format(origin, final_type)]})
									final_origin = compatible[0].name if compatible else None

							company = frappe.defaults.get_global_default("company")
							taxable_company = frappe.db.get_value("Company", company, "is_vat_company")
							tax_class = get_item_tax_template_rate([], item_group, return_tax_class=True)
							standard_rate = row[selling_price_index]
							description = ""
							if description_index:
								description = row[description_index]
							row.extend([self.doctype_data.sync_with_woocommerce, item_group, manage_stock, self.doctype_data.warehouse, default_company,
							            self.doctype_data.warehouse, stock, valuation_rate, item_group, standard_rate, "KG", taxable_company, tax_class, manage_stock,
							            description, liters, final_origin, brand])

						if self.doctype == "Data Archive":
							customer_match = frappe.get_all("Customer", filters={'winbiz_address_number': row[address_id_index]})
							if customer_match:
								customer_link = customer_match[0].name
								customer_text = None
							else:
								customer_link = None
								customer_text = ""
								if row[address_name_title_index]:
									customer_text += f"{row[address_name_title_index]} "
								if row[address_name_index]:
									customer_text += f"{row[address_name_index]}\n"
								if row[address_line1_index]:
									customer_text += f"{row[address_line1_index]}\n"
								if row[address_line2_index]:
									customer_text += f"{row[address_line2_index]}\n"
								if row[address_pincode_index]:
									customer_text += f"{row[address_pincode_index]} "
								if row[address_city_index]:
									customer_text += f"{row[address_city_index]}"
								if not customer_text:
									customer_text = "Guest"

							if row[price_index] is not None and row[vat_index] is not None:
								price_vat_excluded = row[price_index] - row[vat_index]
							elif row[price_index] is not None and row[vat_index] is None:
								price_vat_excluded = row[price_index]
							elif row[price_index] is None and row[vat_index] is not None:
								price_vat_excluded = 0 - row[vat_index]
							else:
								price_vat_excluded = None

							date_base = row[date_archive_index]
							if(not isinstance(date_base, datetime)):
								if(isinstance(date_base, int)):
									formatted_date = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + date_base - 2).strftime('%Y-%m-%d')
								else:
									formatted_date = datetime.strptime(date_base,'%d.%m.%Y').strftime('%Y-%m-%d')
							else:
								formatted_date = date_base

							description = row[description_index].replace("_x000D_", "<br>").replace("\n", "<br>")
							if last_archive_no != row[archive_no_index]:
								#frappe.msgprint("Archive No: " + str(row[archive_no_index]) + " is being imported")
								type_line = {"20":"Invoice", "10": "Offer", "12":"Order Confirmation", "14":"Worksheet"}.get(str(row[type_line_index]), None)
								row.extend(["Winbiz", _(type_line), row[ref_index], description, row[units_index], row[quantity_index], price_vat_excluded, row[vat_index], row[price_index],
								            str(formatted_date), customer_link, customer_text, "Win-" + str(row[archive_no_index])])
								last_archive_no = row[archive_no_index]
							else:# The above code is appending the data archive lines
								ref = row[ref_index]
								units = row[units_index]
								quantity = row[quantity_index]
								price = row[price_index]
								vat = row[vat_index]
								row = [None] * len(row)
								row.extend([None, None, ref, description, units, quantity, price_vat_excluded, vat, price, None, None, None, None])

							if i < len(self.raw_data)-1 and (i == start_line + split_value + add_to_value - 1) and self.raw_data[i+1][archive_no_index] == last_archive_no:
								split_value += 1

						elif self.doctype == "Contact":
							if not row[user_email_index]:
								row[user_email_index] = junk_username_mail + str(junk_counter_mail) + junk_domain_mail
								junk_counter_mail += 1
							else:
								row[user_email_index] = unicodedata.normalize("NFKD", row[user_email_index]).replace(" ", "")
							valid_email, row[user_email_index] = is_valid_email(row[user_email_index])
							if not valid_email:
								continue
							if frappe.db.exists("Contact", {"winbiz_address_number": row[address_id_index]}):
								continue
							if frappe.db.exists("Contact", {"email_id": row[user_email_index]}):
								continue

							customer_with_address_number = frappe.db.get_value("Customer", {"winbiz_address_number": row[address_id_index]}, "name")
							if customer_with_address_number:
								customer_name = customer_with_address_number
							else:
								customer_with_email = frappe.db.get_value("Customer", {"email_id": row[user_email_index]}, "name")
								if customer_with_email:
									customer_name = customer_with_email
								else:
									customer_name = None

							#frappe.neolog(str(row[address_id_index]), "{}  {}  {}".format(row[address_phone_index], row[address_second_phone_index], row[address_mobile_phone_index]))
							#frappe.neolog("phone before {}".format(row[address_phone_index]))
							phone = None
							if row[address_phone_index] and row[address_phone_index] != "None":
								clean_phone = re.sub(r"\D", "", str(row[address_phone_index]))
								if len(clean_phone) >= 5:
									phone = clean_phone

							#frappe.neolog("phone after {}".format(phone))
							first_name = row[firstname_index] if row[firstname_index] else (row[address_company_index] if row[address_company_index] else row[lastname_index])
							last_name = row[lastname_index] if row[lastname_index] and (row[firstname_index] or row[address_company_index]) else None
							row.extend([first_name, last_name, "Customer", customer_name, row[user_email_index], 1, phone, phone, 1 if phone else None, None, row[user_email_index], 1])
							#frappe.neolog("row", "{}".format(row))
							#frappe.neolog("second phone before {}".format(row[address_second_phone_index]))
							#frappe.neolog("mobile phone before {}".format(row[address_mobile_phone_index]))
							if (row[address_second_phone_index] and row[address_second_phone_index] != "None") or (row[address_mobile_phone_index] and row[address_mobile_phone_index] != "None"):
								#frappe.neolog("row", "{}".format(row))
								new_row = copy.deepcopy(row)

						elif self.doctype == "Address":
							if not row[user_email_index]:
								row[user_email_index] = junk_username_mail + str(junk_counter_mail) + junk_domain_mail
								junk_counter_mail += 1
							else:
								row[user_email_index] = unicodedata.normalize("NFKD", row[user_email_index]).replace(" ", "")
							valid_email, row[user_email_index] = is_valid_email(row[user_email_index])
							if not valid_email:
								continue
							if frappe.db.exists("Address", {"winbiz_address_number": row[address_id_index]}):
								continue

							customer_name = frappe.db.get_value("Customer", filters={"winbiz_address_number": row[address_id_index]}, fieldname='name')
							if not customer_name:
								continue

							filtered_contacts = frappe.db.get_value("Contact", filters={"winbiz_address_number": row[address_id_index]}, fieldname='name')
							if not filtered_contacts:
								filtered_contacts = frappe.db.get_value("Contact", {"email_id": row[user_email_index]}, fieldname='name')
								if not filtered_contacts:
									contact_email = [{"email_id":row[user_email_index], "is_primary":1}]

									frappe.get_doc({"doctype": "Contact", "email_ids": contact_email,
									                "first_name": row[firstname_index] if row[firstname_index] else (row[address_company_index] if row[address_company_index] else row[lastname_index]), "last_name": row[lastname_index],
									                "links": [{"link_doctype": "Customer", "link_name": customer_name}], "winbiz_address_number": row[address_id_index],
									                "email_ids": contact_email if row[user_email_index] else []}).insert()
									frappe.db.commit()

							title_formatted = ""
							if row[address_company_index]:
								title_formatted += f"{row[address_company_index]} "
							if row[lastname_index]:
								title_formatted += f"{row[lastname_index]} "
							if row[firstname_index]:
								title_formatted += row[firstname_index]
							title_formatted = title_formatted.strip()
							title_formatted = title_formatted[0:115]

							suffix = 1
							base_title = title_formatted
							in_db = False
							if frappe.db.exists("Address", {"address_title": title_formatted}):
								in_db = True
								while frappe.db.exists("Address", {"address_title": title_formatted + " - " + str(suffix)}):
									suffix += 1
								title_formatted = base_title + " - " + str(suffix)
							suffix = suffix if in_db else 0
							while title_formatted.lower() in addresses_to_add:
								suffix += 1
								title_formatted = base_title + " - " + str(suffix)
							addresses_to_add.append(title_formatted.lower())

							counter = 0
							while frappe.db.exists("Address", {"address_title": title_formatted}):
								counter += 1

							if frappe.get_all("Address", filters={"address_title": title_formatted}):
								counter += 1
								while frappe.get_all("Address", filters={"address_title": title_formatted + " " + str(i)}):
									counter += 1
							if counter > 0:
								title_formatted += " " + str(counter)

							if row[address_country_index]:
								country = frappe.db.get_value("Country", filters={"code": row[address_country_index].lower()}, fieldname='name')
							#country = "Suisse" if _(pycountry.countries.get(alpha_2=row[address_country_index]).name) == "Switzerland" else "Suisse" #!!!! _(pycountry.countries.get(alpha_2=row[address_country_index]).name)
							else:
								country = "Switzerland"

							phone = None
							if row[address_phone_index]:
								clean_phone = re.sub(r"\D", "", str(row[address_phone_index]))
								if len(clean_phone) >= 5:
									phone = clean_phone
							row.extend([title_formatted, "Billing", 1, country, "Customer", customer_name, row[user_email_index], phone])

						elif self.doctype == "Customer":
							if not row[user_email_index]:
								row[user_email_index] = junk_username_mail + str(junk_counter_mail) + junk_domain_mail
								junk_counter_mail += 1
							else:
								row[user_email_index] = unicodedata.normalize("NFKD", row[user_email_index]).replace(" ", "")
							valid_email, row[user_email_index] = is_valid_email(row[user_email_index])
							if not valid_email:
								continue
							if frappe.db.exists("Customer", {"winbiz_address_number": row[address_id_index]}):
								continue

							if row[address_company_index]:
								full_name = row[address_company_index]
								customer_type = "Company"
							else:
								full_name = row[address_name_index]
								customer_type = "Individual"

							base_name = full_name
							suffix = 1
							in_db = False
							if frappe.db.exists("Customer", {"customer_name": full_name}):
								in_db = True
								while frappe.db.exists("Customer", {"customer_name": full_name + " - " + str(suffix)}):
									suffix += 1
								full_name = base_name + " - " + str(suffix)
							suffix = suffix if in_db else 0
							while full_name.lower() in names_to_add:
								suffix += 1
								full_name = base_name + " - " + str(suffix)
							names_to_add.append(full_name.lower())

							#country = self.doctype_data.default_territory
							company = frappe.defaults.get_global_default("company")
							default_currency = frappe.get_value("Company", company, "default_currency")
							if row[address_country_index]:
								country = frappe.db.exists("Country", {"code": row[address_country_index].lower()})
								if not country:
									country = "Switzerland"
								#country = "Suisse" if row[address_country_index] == "CH" else self.doctype_data.default_territory
								'''if (row[address_country_index]).upper() != "CH":
									default_currency = "EUR"'''

							'''final_name = None
							if len(frappe.get_all("Customer", filters={'winbiz_address_number': row[address_id_index]})) == 0:
								counter = 1
								if len(frappe.get_all("Customer", filters={'customer_name': full_name})) > 0:
									while(frappe.get_all("Customer", filters={'customer_name': full_name + " " + str(counter)})):
										counter += 1
									final_name = full_name + " " + str(counter)

								if last_full_name:
									while((full_name + " " + str(counter)).lower() in last_full_name):
										counter += 1
									final_name = full_name + " " + str(counter)
								else:
									if not final_name:
										final_name = full_name
							else:
								add_row_in_data = False
								final_name = ""
								country = None
								customer_type = ""
							last_full_name.append(final_name.lower())'''

							row.extend([full_name, customer_type, country, 1, row[user_email_index], default_currency])

						elif self.doctype == "Supplier":
							if supplier_list:
								if str(row[address_id_index]) not in supplier_list:
									continue
							else:
								continue
							suppliers = frappe.get_all("Supplier", filters={'winbiz_address_number': row[address_id_index]})
							if not suppliers:
								customers = frappe.get_all("Customer", filters={'winbiz_address_number': row[address_id_index]})
								if customers:
									base_customer = frappe.get_doc("Customer", customers[0])
									country = None
									if row[address_country_index]:
										country = frappe.db.get_value("Country", filters={"code": row[address_country_index].lower()}, fieldname='name')
									row.extend([base_customer.customer_name, base_customer.customer_type, country, "All Supplier Groups", "client no: " + str(row[client_number_index])])
								else:
									continue
							else:
								continue

						elif self.doctype == "Object":
							if row[address_id_index]:
								if frappe.db.exists("Customer", {"winbiz_address_number": row[address_id_index]}):
									customer = frappe.get_doc("Customer", {"winbiz_address_number": row[address_id_index]})
								else:
									continue
							else:
								continue

							if row[brand_index]:
								row[brand_index] =  str(row[brand_index]).strip()
								if not frappe.db.exists("Brand", row[brand_index]):
									frappe.get_doc({"doctype": "Brand", "brand": row[brand_index]}).insert()
									frappe.db.commit()
								else:
									row[brand_index] =  str(frappe.db.get_value("Brand", row[brand_index], "name"))

							if row[type_index]:
								row[type_index] = str(row[type_index]).strip()
								if not frappe.db.exists("Vehicle Type", row[type_index]):
									frappe.get_doc({"doctype": "Vehicle Type", "vehicle_type": row[type_index]}).insert()
									frappe.db.commit()
								else:
									row[type_index] = str(frappe.db.get_value("Vehicle Type", row[type_index], "name"))

							if row[bodywork_index]:
								row[bodywork_index] = str(row[bodywork_index]).strip()
								if not frappe.db.exists("Bodywork", row[bodywork_index]):
									frappe.get_doc({"doctype": "Bodywork", "bodywork": row[bodywork_index]}).insert()
									frappe.db.commit()
								else:
									row[bodywork_index] = str(frappe.db.get_value("Bodywork", row[bodywork_index], "name"))

							if row[internal_color_index]:
								row[internal_color_index] = str(row[internal_color_index]).strip()
								if not frappe.db.exists("Neoffice Color", row[internal_color_index]):
									frappe.get_doc({"doctype": "Neoffice Color", "color": row[internal_color_index]}).insert()
									frappe.db.commit()
								else:
									row[internal_color_index] = str(frappe.db.get_value("Neoffice Color", row[internal_color_index], "name"))

							if row[insurance_index]:
								row[insurance_index] = str(row[insurance_index]).strip()
								if not frappe.db.exists("Insurance", row[insurance_index]):
									frappe.get_doc({"doctype": "Insurance", "insurance": row[insurance_index]}).insert()
									frappe.db.commit()
								else:
									row[insurance_index] = str(frappe.db.get_value("Insurance", row[insurance_index], "name"))

							if row[engine_type_index]:
								row[engine_type_index] = str(row[engine_type_index]).strip()
								if not frappe.db.exists("Engine Type", row[engine_type_index]):
									frappe.get_doc({"doctype": "Engine Type", "engine_type": row[engine_type_index]}).insert()
									frappe.db.commit()
								else:
									row[engine_type_index] = str(frappe.db.get_value("Engine Type", row[engine_type_index], "name"))

							if row[gearbox_type_index]:
								row[gearbox_type_index] = str(row[gearbox_type_index]).strip()
								if not frappe.db.exists("Gearbox Type", row[gearbox_type_index]):
									frappe.get_doc({"doctype": "Gearbox Type", "gearbox_type": row[gearbox_type_index]}).insert()
									frappe.db.commit()
								else:
									row[gearbox_type_index] = str(frappe.db.get_value("Gearbox Type", row[gearbox_type_index], "name"))

							if row[fuel_index]:
								row[fuel_index] =  str(row[fuel_index]).strip()
								if not frappe.db.exists("Fuel", row[fuel_index]):
									frappe.get_doc({"doctype": "Fuel", "fuel": row[fuel_index]}).insert()
									frappe.db.commit()
								else:
									row[fuel_index] = str(frappe.db.get_value("Fuel", row[fuel_index], "name"))

							if row[external_color_index]:
								row[external_color_index] = str(row[external_color_index]).strip()
								if not frappe.db.exists("Neoffice Color", row[external_color_index]):
									frappe.get_doc({"doctype": "Neoffice Color", "color": row[external_color_index]}).insert()
									frappe.db.commit()
								else:
									row[external_color_index] = str(frappe.db.get_value("Neoffice Color", row[external_color_index], "name"))

							remark = ""
							remark += str(row[remark_index]) + '</br>' if str(row[remark_index]) else ""
							remark += str(row[remark_index])
							object_name_list = []
							object_name = ""
							object_name += str(row[brand_index]) + " " if row[brand_index] else ""
							object_name += str(row[type_index]) + " " if row[type_index] else ""
							object_name += str(row[plate_number_index]) if row[plate_number_index] else ""
							object_name = object_name.strip()
							if not object_name:
								count_missing_names = 1
								temp_name = "No name " + str(count_missing_names)
								while frappe.db.exists("Object", {"name": temp_name}) or temp_name in object_name_list:
									count_missing_names += 1
									temp_name = "No name " + str(count_missing_names)
								object_name = temp_name
								object_name_list.append(object_name)
							else:
								if not frappe.db.exists("Object", {"name": object_name}) and object_name not in object_name_list:
									object_name_list.append(object_name)
								else:
									temp_name = object_name + " " + customer.name
									if not frappe.db.exists("Object", {"name": temp_name}) and temp_name not in object_name_list:
										object_name = temp_name
										object_name_list.append(object_name)
									else:
										count_object_names = 1
										temp_name = object_name + " " + customer.name + " " + str(count_object_names)
										while frappe.db.exists("Object", {"name": temp_name}) or temp_name in object_name_list:
											count_object_names += 1
											temp_name = object_name + " " + customer.name + " " + str(count_object_names)
										object_name = temp_name
										object_name_list.append(object_name)


							row.extend([customer.name, str(row[registration_number_index]) if row[registration_number_index] else None, str(row[chassis_number_index]) if row[chassis_number_index] else None,
							            str(row[plate_number_index]) if row[plate_number_index] else None, str(row[homologation_index]) if row[homologation_index] else None, str(row[engine_number_index]) if row[engine_number_index] else None,
							            str(row[order_number_index]) if row[order_number_index] else None, str(row[keycode_1_index]) if row[keycode_1_index] else None, str(row[key_id_index]) if row[key_id_index] else None,
							            str(row[gearbox_number_index]) if row[gearbox_number_index] else None, str(row[cabin_number_index]) if row[cabin_number_index] else None, str(row[radio_code_index]) if row[radio_code_index] else None,
							            str(row[keycode_2_index]) if row[keycode_2_index] else None, str(row[doors_index]) if row[doors_index] else None,  str(row[seats_index]) if row[seats_index] else None, remark, object_name,
							            row[brand_index], row[type_index], row[bodywork_index], row[internal_color_index],
							            row[insurance_index], row[engine_type_index], row[gearbox_type_index], row[fuel_index],
							            row[external_color_index], "Vehicle"
							            ])
					#////
					if add_row_in_data: #//// added if condition
						row_obj = Row(i, row, self.doctype, header, self.import_type)
					data.append(row_obj)

					#//// added block
					if self.doctype_data.import_source == "Woocommerce" and new_row:
						if self.doctype == "Item":
							if row[parent_id_index] == 0:
								parent_sku = None
							else:
								parent_sku = list_of_parents.get(row[parent_id_index], "error")
								if parent_sku == "error":
									parent_list = frappe.get_all("Item", filters={"import_id": row[parent_id_index]})
									if parent_list:
										parent_sku = parent_list[0].name

							#if parent_sku == "error":
							#error_msg += f"Can't find parent product with ID {item}\n"

							if  attributes_value:
								del attributes_value[0]
							#attributes_value = [] if not attributes_value else attributes_value.pop(0)
							if len(additional_categories) > len(attributes_value):
								range_len = len(additional_categories)
							else:
								range_len = len(attributes_value)
							for index in range(range_len):
								#for index in range(1, len(attributes_value)):
								added_lines += 1
								#new_row = [None] * len(new_row)
								#if len(new_row) == 0:
								new_row = [None] * (base_row_length+11) # +11 because 11 images
								#new_row[-5] = attributes_name[i]
								#new_row[-4] = attributes_value[i]
								#new_row[-3] = 1 if has_ecommerce else 0
								row_attribute_name = None
								row_attribute_value = None
								additional_cat = None
								if attributes_value and index < len(attributes_value):
									row_attribute_name = attributes_name[index]
									row_attribute_value = attributes_value[index]
								if additional_categories and index < len(additional_categories):
									additional_cat = additional_categories[index]

								#new_row.extend([None, None, None, row_attribute_name, row_attribute_value, None, None, None, None,
								#None, None, None, None, None, additional_cat])
								new_row.extend([None, None, None, None, row_attribute_name, row_attribute_value, None, None, None, None,
								                None, None, None, None, None, additional_cat, None, None, None, None, None, None, None, None])

								row_obj = Row(i+added_lines, new_row, self.doctype, header, self.import_type)
								if parent_sku != "error":
									data.append(row_obj)
								new_row = []
						#frappe.msgprint(str(new_row))

						elif self.doctype == "Address":
							if customer_name:
								added_lines += 1
								title_formatted = str(row[shipping_firstname_index]) + " " + str(row[shipping_lastname_index]) if row[shipping_firstname_index] else str(row[shipping_company_index])
								if row[shipping_country_index]:
									country = frappe.db.exists("Country", {"code": row[shipping_country_index].lower()})
									'''countries = frappe.get_all("Country", filters={"code": row[shipping_country_index].lower()})
									if countries:
										country = countries[0].name
									else:
										country = None'''
								#country = "Suisse" if _(pycountry.countries.get(alpha_2=row[shipping_country_index]).name) == "Switzerland" else self.doctype_data.default_territory #!!!!_(pycountry.countries.get(alpha_2=row[shipping_country_index]).name)
								'''else:
									country = None'''
								if not country:
									country = "Switzerland"
								if not frappe.get_all("Address", filters={"woocommerce_email": row[user_email_index], "address_type": "Shipping", "address_line1": row[shipping_address_1_index]}):
									new_row.extend([row[user_email_index], title_formatted, "Shipping", row[shipping_address_1_index], row[shipping_address_2_index], row[shipping_city_index], row[shipping_state_index],
									                row[shipping_postcode_index], country, row[billing_email_index], row[shipping_phone_index], "Customer", customer_name])
									row_obj = Row(i+added_lines, new_row, self.doctype, header, self.import_type)
									data.append(row_obj)
								new_row = []

						'''elif self.doctype == "Contact":
							new_row = [None] * len(new_row)
							new_row.extend([None, billing_email, 0, None, None])
							added_lines += 1
							row_obj = Row(i+added_lines, new_row, self.doctype, header, self.import_type)
							data.append(row_obj)
							new_row = []'''

					elif self.doctype_data.import_source == "Winbiz" and new_row:
						if self.doctype == "Item Price":
							added_lines += 1
							new_row.extend(["Standard Buying", row[buying_price_index]])
							row_obj = Row(i+added_lines, new_row, self.doctype, header, self.import_type)
							data.append(row_obj)
							new_row = []

						elif self.doctype == "Contact":
							if row[address_second_phone_index]:
								second_phone = re.sub(r"\D", "", str(row[address_second_phone_index]))
								if len(second_phone) >= 5:
									#frappe.neolog("second phone ")
									added_lines += 1

									#frappe.neolog("second phone after", "{}".format(second_phone))
									new_row = [None] * base_row_length
									new_row.extend([None, None, None, None, None, None, None, second_phone, 0, 0, None, None])
									#frappe.neolog("second phone new row", "{}".format(new_row))
									row_obj = Row(i+added_lines, new_row, self.doctype, header, self.import_type)
									data.append(row_obj)
									new_row = []
							if row[address_mobile_phone_index]:
								mobile_phone = re.sub(r"\D", "", str(row[address_mobile_phone_index]))
								if len(mobile_phone) >= 5:
									#frappe.neolog("mobile phone ")
									new_row = [None] * base_row_length
									added_lines += 1

									#frappe.neolog("mobile phone after", "{}".format(mobile_phone))
									new_row.extend([None, None, None, None, None, None, None, mobile_phone, 0, 1 if mobile_phone else 0, None, None])
									#frappe.neolog("mobile phone new row", "{}".format(new_row))
									row_obj = Row(i+added_lines, new_row, self.doctype, header, self.import_type)
									data.append(row_obj)
									new_row = []
				#////

			self.doctype_data.db_set("added_lines", added_lines) #//// added
			self.header = header
			self.columns = self.header.columns
			self.data = data

			if len(data) < 1:
				frappe.throw(
					_("Import template should contain a Header and atleast one row."),
					title=_("Template Error"),
				)

		#//// added block
		else:
			for i, row in enumerate(self.raw_data[:MAX_ROWS_IN_PREVIEW]):
				if all(v in INVALID_VALUES for v in row):
					# empty row
					continue
				if not header:
					header = Header(i, row, self.doctype, self.raw_data, self.column_to_field_map, self.doctype_data, self.from_func)
				else:
					row_obj = Row(i, row, self.doctype, header, self.import_type)
					data.append(row_obj)
			self.header = header
			self.columns = self.header.columns
			self.data = data

			if len(data) < 1:
				frappe.throw(
					_("Import template should contain a Header and atleast one row."),
					title=_("Template Error"),
				)
		#////

	def get_data_for_import_preview(self):
		"""Adds a serial number column as the first column"""

		columns = [frappe._dict({"header_title": "Sr. No", "skip_import": True})]
		columns += [col.as_dict() for col in self.columns]
		for col in columns:
			# only pick useful fields in docfields to minimise the payload
			if col.df:
				col.df = {
					"fieldtype": col.df.fieldtype,
					"fieldname": col.df.fieldname,
					"label": col.df.label,
					"options": col.df.options,
					"parent": col.df.parent,
					"reqd": col.df.reqd,
					"default": col.df.default,
					"read_only": col.df.read_only,
				}

		data = [[row.row_number, *row.as_list()] for row in self.data]

		warnings = self.get_warnings()

		out = frappe._dict()
		out.data = data
		out.columns = columns
		out.warnings = warnings
		total_number_of_rows = len(out.data)
		if total_number_of_rows > MAX_ROWS_IN_PREVIEW:
			out.data = out.data[:MAX_ROWS_IN_PREVIEW]
			out.max_rows_exceeded = True
			out.max_rows_in_preview = MAX_ROWS_IN_PREVIEW
			out.total_number_of_rows = total_number_of_rows
		return out

	def get_payloads_for_import(self):
		payloads = []
		# make a copy
		data = list(self.data)
		while data:
			doc, rows, data = self.parse_next_row_for_import(data)
			payloads.append(frappe._dict(doc=doc, rows=rows))
		return payloads

	def parse_next_row_for_import(self, data):
		"""
		Parses rows that make up a doc. A doc maybe built from a single row or multiple rows.
		Returns the doc, rows, and data without the rows.
		"""
		doctypes = self.header.doctypes

		# first row is included by default
		first_row = data[0]
		rows = [first_row]

		# if there are child doctypes, find the subsequent rows
		if len(doctypes) > 1:
			# subsequent rows that have blank values in parent columns
			# are considered as child rows
			parent_column_indexes = self.header.get_column_indexes(self.doctype)

			data_without_first_row = data[1:]
			for row in data_without_first_row:
				row_values = row.get_values(parent_column_indexes)
				# if the row is blank, it's a child row doc
				if all(v in INVALID_VALUES for v in row_values):
					rows.append(row)
					continue
				# if we encounter a row which has values in parent columns,
				# then it is the next doc
				break

		parent_doc = None
		for row in rows:
			for doctype, table_df in doctypes:
				if doctype == self.doctype and not parent_doc:
					parent_doc = row.parse_doc(doctype)

				if doctype != self.doctype and table_df:
					child_doc = row.parse_doc(doctype, parent_doc, table_df)
					if child_doc is None:
						continue
					parent_doc[table_df.fieldname] = parent_doc.get(table_df.fieldname, [])
					parent_doc[table_df.fieldname].append(child_doc)

		doc = parent_doc

		return doc, rows, data[len(rows) :]

	def get_warnings(self):
		warnings = []

		# ImportFile warnings
		warnings += self.warnings

		# Column warnings
		for col in self.header.columns:
			warnings += col.warnings

		# Row warnings
		for row in self.data:
			warnings += row.warnings

		return warnings

	######

	def read_file(self, file_path: str):
		extn = os.path.splitext(file_path)[1][1:]

		file_content = None

		if self.console:
			file_content = frappe.read_file(file_path, True)
			return file_content, extn

		file_name = frappe.db.get_value("File", {"file_url": file_path})
		if file_name:
			file = frappe.get_doc("File", file_name)
			file_content = file.get_content()

		return file_content, extn

	def read_content(self, content, extension):
		error_title = _("Template Error")
		if extension not in ("csv", "xlsx", "xls"):
			frappe.throw(_("Import template should be of type .csv, .xlsx or .xls"), title=error_title)

		if extension == "csv":
			data = read_csv_content(content)
		elif extension == "xlsx":
			data = read_xlsx_file_from_attached_file(fcontent=content)
		elif extension == "xls":
			data = read_xls_file_from_attached_file(content)

		return data


class Row:
	def __init__(self, index, row, doctype, header, import_type):
		self.index = index
		self.row_number = index + 1
		self.doctype = doctype
		self.data = row
		self.header = header
		self.import_type = import_type
		self.warnings = []

		len_row = len(self.data)
		len_columns = len(self.header.columns)
		if len_row != len_columns:
			less_than_columns = len_row < len_columns
			message = (
				"Row has less values than columns"
				if less_than_columns
				else "Row has more values than columns"
			)
			self.warnings.append(
				{
					"row": self.row_number,
					"message": message,
				}
			)

	def parse_doc(self, doctype, parent_doc=None, table_df=None):
		col_indexes = self.header.get_column_indexes(doctype, table_df)
		values = self.get_values(col_indexes)

		if all(v in INVALID_VALUES for v in values):
			# if all values are invalid, no need to parse it
			return None

		columns = self.header.get_columns(col_indexes)
		return self._parse_doc(doctype, columns, values, parent_doc, table_df)

	def _parse_doc(self, doctype, columns, values, parent_doc=None, table_df=None):
		doc = frappe._dict()
		if self.import_type == INSERT:
			# new_doc returns a dict with default values set
			doc = frappe.new_doc(
				doctype,
				parent_doc=parent_doc,
				parentfield=table_df.fieldname if table_df else None,
				as_dict=True,
			)

		# remove standard fields and __islocal
		for key in frappe.model.default_fields + frappe.model.child_table_fields + ("__islocal",):
			doc.pop(key, None)

		for col, value in zip(columns, values, strict=False):
			df = col.df
			if value in INVALID_VALUES:
				value = None

			if value is not None:
				value = self.validate_value(value, col)

			if value is not None:
				doc[df.fieldname] = self.parse_value(value, col)

		is_table = frappe.get_meta(doctype).istable
		is_update = self.import_type == UPDATE
		if is_table and is_update:
			# check if the row already exists
			# if yes, fetch the original doc so that it is not updated
			# if no, create a new doc
			id_field = get_id_field(doctype)
			id_value = doc.get(id_field.fieldname)
			if id_value and frappe.db.exists(doctype, id_value):
				existing_doc = frappe.get_doc(doctype, id_value)
				existing_doc.update(doc)
				doc = existing_doc
			else:
				# for table rows being inserted in update
				# create a new doc with defaults set
				new_doc = frappe.new_doc(doctype, as_dict=True)
				new_doc.update(doc)
				doc = new_doc

		return doc

	def validate_value(self, value, col):
		df = col.df
		if df.fieldtype == "Select":
			select_options = get_select_options(df)
			if select_options and cstr(value) not in select_options:
				options_string = ", ".join(frappe.bold(d) for d in select_options)
				msg = _("Value must be one of {0}").format(options_string)
				self.warnings.append(
					{
						"row": self.row_number,
						"field": df_as_json(df),
						"message": msg,
					}
				)
				return

		elif df.fieldtype == "Link":
			if self.doctype != "Item": #//// added if condition
				exists = self.link_exists(value, df)
				if not exists:
					msg = _("Value {0} missing for {1}").format(frappe.bold(value), frappe.bold(df.options))
					self.warnings.append(
						{
							"row": self.row_number,
							"field": df_as_json(df),
							"message": msg,
						}
					)
					return
		elif df.fieldtype in ["Date", "Datetime"]:
			value = self.get_date(value, col)
			if isinstance(value, str):
				# value was not parsed as datetime object
				self.warnings.append(
					{
						"row": self.row_number,
						"col": col.column_number,
						"field": df_as_json(df),
						"message": _("Value {0} must in {1} format").format(
							frappe.bold(value), frappe.bold(get_user_format(col.date_format))
						),
					}
				)
				return
		elif df.fieldtype == "Duration":
			if not DURATION_PATTERN.match(value):
				self.warnings.append(
					{
						"row": self.row_number,
						"col": col.column_number,
						"field": df_as_json(df),
						"message": _("Value {0} must be in the valid duration format: d h m s").format(
							frappe.bold(value)
						),
					}
				)

		return value

	def link_exists(self, value, df):
		return bool(frappe.db.exists(df.options, value, cache=True))

	def parse_value(self, value, col):
		df = col.df
		if isinstance(value, datetime | date) and df.fieldtype in ["Date", "Datetime"]:
			return value

		value = cstr(value)

		# convert boolean values to 0 or 1
		valid_check_values = ["t", "f", "true", "false", "yes", "no", "y", "n"]
		if df.fieldtype == "Check" and value.lower().strip() in valid_check_values:
			value = value.lower().strip()
			value = 1 if value in ["t", "true", "y", "yes"] else 0

		if df.fieldtype in ["Int", "Check"]:
			value = cint(value)
		elif df.fieldtype in ["Float", "Percent", "Currency"]:
			value = flt(value)
		elif df.fieldtype in ["Date", "Datetime"]:
			value = self.get_date(value, col)
		elif df.fieldtype == "Duration":
			value = duration_to_seconds(value)

		return value

	def get_date(self, value, column):
		if isinstance(value, datetime | date):
			return value

		date_format = column.date_format
		if date_format:
			try:
				return datetime.strptime(value, date_format)
			except ValueError:
				# ignore date values that dont match the format
				# import will break for these values later
				pass
		return value

	def get_values(self, indexes):
		return [self.data[i] for i in indexes]

	def get(self, index):
		return self.data[index]

	def as_list(self):
		return self.data


class Header(Row):
	def __init__(self, index, row, doctype, raw_data, column_to_field_map=None, doctype_data=None, from_func=None): #//// added , doctype_data=None, from_func=None
		self.doctype_data = doctype_data #//// added
		self.from_func = from_func #//// added
		self.index = index
		self.row_number = index + 1
		self.data = row
		self.doctype = doctype
		column_to_field_map = column_to_field_map or frappe._dict()

		self.seen = []
		self.columns = []

		for j, header in enumerate(row):
			column_values = [get_item_at_index(r, j) for r in raw_data]
			#//// added block
			if self.doctype_data.import_source == "Woocommerce":
				if self.doctype == "Item":
					map_to_field = {
						"Title": "item_name", "Sku": "item_code", "description": "woocommerce_long_description",
						"short_description": "woocommerce_short_description", "item_group": "item_group",
						"maintain_stock": "is_stock_item",
						"parent_sku": "variant_of", "attribute_name": "attributes.attribute",
						"attribute_value": "attributes.attribute_value", "has_variants": "has_variants",
						"sync_with_woocommerce": "sync_with_woocommerce", "image": "image",
						"woocommerce_img_1": "woocommerce_img_1", "woocommerce_img_2": "woocommerce_img_2",
						"woocommerce_img_3": "woocommerce_img_3", "woocommerce_img_4": "woocommerce_img_4",
						"woocommerce_img_5": "woocommerce_img_5",
						"woocommerce_img_6": "woocommerce_img_6", "woocommerce_img_7": "woocommerce_img_7",
						"woocommerce_img_8": "woocommerce_img_8", "woocommerce_img_9": "woocommerce_img_9",
						"woocommerce_img_10": "woocommerce_img_10",
						"default_warehouse": "item_defaults.default_warehouse",
						"category_ecommerce": "category_ecommerce", "woocommerce_warehouse": "woocommerce_warehouse",
						"stock": "opening_stock", "valuation_rate": "valuation_rate",
						"standard_rate": "standard_rate", "default_company": "item_defaults.company", "id": "import_id",
						"additionnal_categories": "additional_ecommerce_categories.item_group",
						"woocommerce_taxable": "woocommerce_taxable", "brand": "brand",
						"brand_ecommerce": "brand_ecommerce", "Weight": "weight_per_unit", "weight_uom": "weight_uom",
						"maintain_stock_ecommerce": "woocommerce_manage_stock",
						"woocommerce_weight": "woocommerce_weight",
						"Length": "woocommerce_length", "Width": "woocommerce_width", "Height": "woocommerce_height"
					}.get(header, "Don't Import")

				elif self.doctype == "Pricing Rule":
					map_to_field = {
						"apply_on": "apply_on", "price_or_product": "price_or_product", "sku": "items.item_code",
						"sync_woocommerce_rule": "sync_woocommerce_rule", "selling": "selling",
						"promo_price": "rate", "rate_or_discount": "rate_or_discount", "title": "title",
						"currency": "currency", "margin_type": "margin_type"
					}.get(header, "Don't Import")

				elif self.doctype == "Address":
					map_to_field = {
						"User Email": "woocommerce_email", "address_title": "address_title",
						"address_type": "address_type", "phone": "phone", "address_line1": "address_line1",
						"address_line2": "address_line2", "city": "city", "country": "country", "postcode": "pincode",
						"link_doctype": "links.link_doctype",
						"link_name": "links.link_name", "email_id": "email_id"
					}.get(header, "Don't Import")

				elif self.doctype == "Contact":
					map_to_field = {
						"User Email": "email_id", "first_name": "first_name", "Last Name": "last_name",
						"phone": "phone", "link_doctype": "links.link_doctype",
						"link_name": "links.link_name", "email_id": "email_ids.email_id",
						"is_primary_email": "email_ids.is_primary"
					}.get(header, "Don't Import")

				elif self.doctype == "Customer":
					map_to_field = {
						"User Email": "email_id", "customer_type": "customer_type", "territory": "territory",
						"customer_name": "customer_name", "is_import": "is_import", "default_currency": "default_currency",
					}.get(header, "Don't Import")

				elif self.doctype == "Data Archive":
					map_to_field = {
						"source": "source", "type": "type", "number": "number", "Order Total": "total",
						"lines.reference": "lines.reference", "lines.description": "lines.description",
						"lines.units": "lines.units",
						"lines.quantity": "lines.quantity",
						"lines.total_price_excl_taxes": "lines.total_price_excl_taxes",
						"lines.total_vat": "lines.total_vat",
						"lines.total_price_incl_taxes": "lines.total_price_incl_taxes",
						"customer_link": "customer_link", "customer_text": "customer_text", "Order Date": "date",
						"status": "status",
						"Payment Method Title": "payment_method", "Shipping Method": "shipping_method",
						"shipping_fees": "shipping_fees", "total": "total"
					}.get(header, "Don't Import")

			elif self.doctype_data.import_source == "Winbiz":
				if self.doctype == "Item":
					map_to_field = {
						"ar_abrege": "item_name", "ar_code": "item_code", "ar_desc": "woocommerce_long_description",
						"item_group": "item_group", "sync_with_woocommerce": "sync_with_woocommerce",
						"maintain_stock": "is_stock_item", "default_warehouse": "item_defaults.default_warehouse",
						"category_ecommerce": "category_ecommerce", "woocommerce_warehouse": "woocommerce_warehouse",
						"stock": "opening_stock", "valuation_rate": "valuation_rate", "standard_rate": "standard_rate",
						"default_company": "item_defaults.company", "ar_codbar": "barcodes.barcode",
						"ar_poids": "weight_per_unit", "weight_uom": "weight_uom",
						"woocommerce_taxable": "woocommerce_taxable",
						"maintain_stock_ecommerce": "woocommerce_manage_stock", "description": "description",
						"liters": "alcohol_quantity", "prixach": "buying_standard_rate", "origin": "alcohol_origin",
						"ar_alcool": "is_alcohol", "brand": "brand", "ar_numero": "import_id",
					}.get(header, "Don't Import")

				elif self.doctype == "Item Price":
					map_to_field = {
						"ar_fn_ref": "item_code", "price_list": "price_list", "price_list_rate": "price_list_rate"
					}.get(header, "Don't Import")

				if self.doctype == "Data Archive":
					map_to_field = {
						"source": "source", "type": "type", "number": "number", "do_montant": "total",
						"lines.reference": "lines.reference", "lines.description": "lines.description",
						"lines.units": "lines.units",
						"lines.quantity": "lines.quantity",
						"lines.total_price_excl_taxes": "lines.total_price_excl_taxes",
						"lines.total_vat": "lines.total_vat",
						"lines.total_price_incl_taxes": "lines.total_price_incl_taxes",
						"customer_link": "customer_link", "customer_text": "customer_text",
						"formatted_date": "date_text"
					}.get(header, "Don't Import")

				elif self.doctype == "Customer":
					map_to_field = {
						"email": "email_id", "customer_type": "customer_type", "territory": "territory",
						"customer_name": "customer_name", "ad_numero": "winbiz_address_number", "is_import": "is_import",
						"ad_url": "website", "default_currency": "default_currency"
					}.get(header, "Don't Import")

				elif self.doctype == "Contact":
					map_to_field = {
						"email": "email_id", "ad_numero": "winbiz_address_number", "first_name": "first_name",
						"last_name": "last_name", "phone": "phone", "link_doctype": "links.link_doctype",
						"link_name": "links.link_name", "email_id": "email_ids.email_id",
						"is_primary_email": "email_ids.is_primary", "number": "phone_nos.phone",
						"is_primary_phone": "phone_nos.is_primary_phone",
						"is_primary_mobile_no": "phone_nos.is_primary_mobile_no",
						"is_primary_contact": "is_primary_contact"
					}.get(header, "Don't Import")

				elif self.doctype == "Address":
					map_to_field = {
						"email": "email_id", "address_title": "address_title", "address_type": "address_type",
						"phone": "phone", "ad_rue_1": "address_line1",
						"ad_rue_2": "address_line2", "ad_ville": "city", "country": "country", "ad_npa": "pincode",
						"link_doctype": "links.link_doctype",
						"link_name": "links.link_name", "ad_numero": "winbiz_address_number",
						"is_primary_address": "is_primary_address"
					}.get(header, "Don't Import")

				elif self.doctype == "Supplier":
					map_to_field = {
						"supplier_name": "supplier_name", "supplier_type": "supplier_type",
						"supplier_group": "supplier_group", "country": "country", "AB_IBAN": "iban",
						"ad_numero": "winbiz_address_number", "ad_url": "website", "client_number": "suppliers_details",
						"ad_tvano": "tax_id"
					}.get(header, "Don't Import")

				elif self.doctype == "Object":
					map_to_field = {
						"brand": "brand", "type": "type", "bodywork": "bodywork", "internal_color": "internal_color",
						"insurance": "insurance",
						"engine_type": "engine_type", "gearbox_type": "gearbox_type",
						"external_color": "external_color", "fuel": "fuel", "dj_date1": "first_circulation",
						"dj_date2": "last_antipollution_control", "dj_date3": "last_expertise", "dj_date4": "sale_date",
						"dj_date5": "order_date", "dj_prix1": "sale_price",
						"customer_name": "customer", "registration_number": "registration_number",
						"chassis_number": "chassis_number", "plate_number": "plate_number",
						"homologation": "homologation", "engine_number": "engine_number",
						"order_number": "order_number", "keycode_1": "key_code_1",
						"key_id": "key_indenfication", "gearbox_number": "gearbox_number",
						"cabin_number": "cabin_number", "radio_code": "radio_code",
						"keycode_2": "key_code_2", "dj_nbre1": "km_or_hours",
						"next_antipollution": "next_antipollution_control", "dj_nbre3": "tare_weight",
						"dj_nbre4": "total_weight", "doors": "doors", "dj_nbre6": "displacement", "seats": "seats",
						"remark": "finishing", "object_type": "object_type", "object_name": "object_name"
					}.get(header, "Don't Import")
			else:
				#////
				map_to_field = column_to_field_map.get(str(j))
			column = Column(j, header, self.doctype, column_values, map_to_field, self.seen)
			self.seen.append(header)
			self.columns.append(column)

		doctypes = []
		for col in self.columns:
			if not col.df:
				continue
			if col.df.parent == self.doctype:
				doctypes.append((col.df.parent, None))
			else:
				doctypes.append((col.df.parent, col.df.child_table_df))

		self.doctypes = sorted(list(set(doctypes)), key=lambda x: -1 if x[0] == self.doctype else 1)

	def get_column_indexes(self, doctype, tablefield=None):
		def is_table_field(df):
			if tablefield:
				return df.child_table_df.fieldname == tablefield.fieldname
			return True

		return [
			col.index
			for col in self.columns
			if not col.skip_import and col.df and col.df.parent == doctype and is_table_field(col.df)
		]

	def get_columns(self, indexes):
		return [self.columns[i] for i in indexes]


class Column:
	def __init__(self, index, header, doctype, column_values, map_to_field=None, seen=None):
		if seen is None:
			seen = []
		self.index = index
		self.column_number = index + 1
		self.doctype = doctype
		self.header_title = header
		self.column_values = column_values
		self.map_to_field = map_to_field
		self.seen = seen

		self.date_format = None
		self.df = None
		self.skip_import = None
		self.warnings = []

		self.meta = frappe.get_meta(doctype)
		self.parse()
		self.validate_values()

	def parse(self):
		header_title = self.header_title
		column_number = str(self.column_number)
		skip_import = False

		if self.map_to_field and self.map_to_field != "Don't Import":
			df = get_df_for_column_header(self.doctype, self.map_to_field)
			if df:
				self.warnings.append(
					{
						"message": _("Mapping column {0} to field {1}").format(
							frappe.bold(header_title or "<i>Untitled Column</i>"), frappe.bold(df.label)
						),
						"type": "info",
					}
				)
			else:
				self.warnings.append(
					{
						"message": _("Could not map column {0} to field {1}").format(
							column_number, self.map_to_field
						),
						"type": "info",
					}
				)
		else:
			df = get_df_for_column_header(self.doctype, header_title)
		# df = df_by_labels_and_fieldnames.get(header_title)

		if not df:
			skip_import = True
		else:
			skip_import = False

		if header_title in self.seen:
			self.warnings.append(
				{
					"col": column_number,
					"message": _("Skipping Duplicate Column {0}").format(frappe.bold(header_title)),
					"type": "info",
				}
			)
			df = None
			skip_import = True
		elif self.map_to_field == "Don't Import":
			skip_import = True
			#////commented
			'''self.warnings.append(
				{
					"col": column_number,
					"message": _("Skipping column {0}").format(frappe.bold(header_title)),
					"type": "info",
				}
			)'''
		#////
		elif header_title and not df:
			self.warnings.append(
				{
					"col": column_number,
					"message": _("Cannot match column {0} with any field").format(frappe.bold(header_title)),
					"type": "info",
				}
			)
		elif not header_title and not df:
			self.warnings.append(
				{"col": column_number, "message": _("Skipping Untitled Column"), "type": "info"}
			)

		self.df = df
		self.skip_import = skip_import

	def guess_date_format_for_column(self):
		"""Guesses date format for a column by parsing all the values in the column,
		getting the date format and then returning the one which has the maximum frequency
		"""

		def guess_date_format(d):
			if isinstance(d, datetime | date | time):
				if self.df.fieldtype == "Date":
					return "%Y-%m-%d"
				if self.df.fieldtype == "Datetime":
					return "%Y-%m-%d %H:%M:%S"
				if self.df.fieldtype == "Time":
					return "%H:%M:%S"
			if isinstance(d, str):
				return frappe.utils.guess_date_format(d)

		date_formats = [guess_date_format(d) for d in self.column_values]
		date_formats = [d for d in date_formats if d]
		if not date_formats:
			return

		unique_date_formats = set(date_formats)
		max_occurred_date_format = max(unique_date_formats, key=date_formats.count)

		if len(unique_date_formats) > 1:
			# fmt: off
			message = _("The column {0} has {1} different date formats. Automatically setting {2} as the default format as it is the most common. Please change other values in this column to this format.")
			# fmt: on
			user_date_format = get_user_format(max_occurred_date_format)
			self.warnings.append(
				{
					"col": self.column_number,
					"message": message.format(
						frappe.bold(self.header_title),
						len(unique_date_formats),
						frappe.bold(user_date_format),
					),
					"type": "info",
				}
			)

		return max_occurred_date_format

	def validate_values(self):
		if not self.df:
			return

		if self.skip_import:
			return

		if not any(self.column_values):
			return

		if self.df.fieldtype == "Link":
			# find all values that dont exist
			values = list({cstr(v) for v in self.column_values if v})
			exists = [cstr(d.name) for d in frappe.get_all(self.df.options, filters={"name": ("in", values)})]
			not_exists = list(set(values) - set(exists))
			if not_exists:
				missing_values = ", ".join(not_exists)
				message = _("The following values do not exist for {0}: {1}")
				self.warnings.append(
					{
						"col": self.column_number,
						"message": message.format(self.df.options, missing_values),
						"type": "warning",
					}
				)
		elif self.df.fieldtype in ("Date", "Time", "Datetime"):
			# guess date/time format
			self.date_format = self.guess_date_format_for_column()
			if not self.date_format:
				if self.df.fieldtype == "Time":
					self.date_format = "%H:%M:%S"
					date_format = "HH:mm:ss"
				else:
					self.date_format = "%Y-%m-%d"
					date_format = "yyyy-mm-dd"

				message = _(
					"{0} format could not be determined from the values in this column. Defaulting to {1}."
				)
				self.warnings.append(
					{
						"col": self.column_number,
						"message": message.format(self.df.fieldtype, date_format),
						"type": "info",
					}
				)
		elif self.df.fieldtype == "Select":
			options = get_select_options(self.df)
			if options:
				values = {cstr(v) for v in self.column_values if v}
				invalid = values - set(options)
				if invalid:
					valid_values = ", ".join(frappe.bold(o) for o in options)
					invalid_values = ", ".join(frappe.bold(i) for i in invalid)
					message = _("The following values are invalid: {0}. Values must be one of {1}")
					self.warnings.append(
						{
							"col": self.column_number,
							"message": message.format(invalid_values, valid_values),
						}
					)

	def as_dict(self):
		d = frappe._dict()
		d.index = self.index
		d.column_number = self.column_number
		d.doctype = self.doctype
		d.header_title = self.header_title
		d.map_to_field = self.map_to_field
		d.date_format = self.date_format
		d.df = self.df
		if hasattr(self.df, "is_child_table_field"):
			d.is_child_table_field = self.df.is_child_table_field
			d.child_table_df = self.df.child_table_df
		d.skip_import = self.skip_import
		d.warnings = self.warnings
		return d


def build_fields_dict_for_column_matching(parent_doctype):
	"""
	Build a dict with various keys to match with column headers and value as docfield
	The keys can be label or fieldname
	{
	        'Customer': df1,
	        'customer': df1,
	        'Due Date': df2,
	        'due_date': df2,
	        'Item Code (Sales Invoice Item)': df3,
	        'Sales Invoice Item:item_code': df3,
	}
	"""

	def get_standard_fields(doctype):
		meta = frappe.get_meta(doctype)
		if meta.istable:
			standard_fields = [
				{"label": "Parent", "fieldname": "parent"},
				{"label": "Parent Type", "fieldname": "parenttype"},
				{"label": "Parent Field", "fieldname": "parentfield"},
				{"label": "Row Index", "fieldname": "idx"},
			]
		else:
			standard_fields = [
				{"label": "Owner", "fieldname": "owner"},
				{"label": "Document Status", "fieldname": "docstatus", "fieldtype": "Int"},
			]

		out = []
		for df in standard_fields:
			df = frappe._dict(df)
			df.parent = doctype
			out.append(df)
		return out

	parent_meta = frappe.get_meta(parent_doctype)
	out = {}

	# doctypes and fieldname if it is a child doctype
	doctypes = [(parent_doctype, None)] + [(df.options, df) for df in parent_meta.get_table_fields()]

	for doctype, table_df in doctypes:
		translated_table_label = _(table_df.label) if table_df else None

		# name field
		name_df = frappe._dict(
			{
				"fieldtype": "Data",
				"fieldname": "name",
				"label": "ID",
				"reqd": 1,  # self.import_type == UPDATE,
				"parent": doctype,
			}
		)

		if doctype == parent_doctype:
			name_headers = (
				"name",  # fieldname
				"ID",  # label
				_("ID"),  # translated label
			)
		else:
			name_headers = (
				f"{table_df.fieldname}.name",  # fieldname
				f"ID ({table_df.label})",  # label
				"{} ({})".format(_("ID"), translated_table_label),  # translated label
			)

			name_df.is_child_table_field = True
			name_df.child_table_df = table_df

		for header in name_headers:
			out[header] = name_df

		fields = get_standard_fields(doctype) + frappe.get_meta(doctype).fields
		for df in fields:
			fieldtype = df.fieldtype or "Data"
			if fieldtype in no_value_fields:
				continue

			label = (df.label or "").strip()
			translated_label = _(label)

			if parent_doctype == doctype:
				# for parent doctypes keys will be
				# Label, fieldname, Label (fieldname)

				for header in (label, translated_label):
					# if Label is already set, don't set it again
					# in case of duplicate column headers
					if header not in out:
						out[header] = df

				for header in (
					df.fieldname,
					f"{label} ({df.fieldname})",
					f"{translated_label} ({df.fieldname})",
				):
					out[header] = df

			else:
				# for child doctypes keys will be
				# Label (Table Field Label)
				# table_field.fieldname

				# create a new df object to avoid mutation problems
				if isinstance(df, dict):
					new_df = frappe._dict(df.copy())
				else:
					new_df = df.as_dict()

				new_df.is_child_table_field = True
				new_df.child_table_df = table_df

				for header in (
					# fieldname
					f"{table_df.fieldname}.{df.fieldname}",
					# label
					f"{label} ({table_df.label})",
					# translated label
					f"{translated_label} ({translated_table_label})",
				):
					out[header] = new_df

	# if autoname is based on field
	# add an entry for "ID (Autoname Field)"
	autoname_field = get_autoname_field(parent_doctype)
	if autoname_field:
		for header in (
			f"ID ({autoname_field.label})",  # label
			"{} ({})".format(_("ID"), _(autoname_field.label)),  # translated label
			# ID field should also map to the autoname field
			"ID",
			_("ID"),
			"name",
		):
			out[header] = autoname_field

	return out


def get_df_for_column_header(doctype, header):
	def build_fields_dict_for_doctype():
		return build_fields_dict_for_column_matching(doctype)

	df_by_labels_and_fieldname = frappe.cache.hget(
		"data_import_column_header_map", doctype, generator=build_fields_dict_for_doctype
	)
	return df_by_labels_and_fieldname.get(header)


# utilities


def get_id_field(doctype):
	autoname_field = get_autoname_field(doctype)
	if autoname_field:
		return autoname_field
	return frappe._dict({"label": "ID", "fieldname": "name", "fieldtype": "Data"})


def get_autoname_field(doctype):
	meta = frappe.get_meta(doctype)
	if meta.autoname and meta.autoname.startswith("field:"):
		fieldname = meta.autoname[len("field:") :]
		return meta.get_field(fieldname)


def get_item_at_index(_list, i, default=None):
	try:
		a = _list[i]
	except IndexError:
		a = default
	return a


def get_user_format(date_format):
	return date_format.replace("%Y", "yyyy").replace("%y", "yy").replace("%m", "mm").replace("%d", "dd")


def df_as_json(df):
	return {
		"fieldname": df.fieldname,
		"fieldtype": df.fieldtype,
		"label": df.label,
		"options": df.options,
		"parent": df.parent,
		"default": df.default,
	}


def get_select_options(df):
	return [d for d in (df.options or "").split("\n") if d]


def create_import_log(data_import, log_index, log_details):
	frappe.get_doc(
		{
			"doctype": "Data Import Log",
			"log_index": log_index,
			"success": log_details.get("success"),
			"data_import": data_import,
			"row_indexes": json.dumps(log_details.get("row_indexes")),
			"docname": log_details.get("docname"),
			"messages": json.dumps(log_details.get("messages", "[]")),
			"exception": log_details.get("exception"),
		}
	).db_insert()

