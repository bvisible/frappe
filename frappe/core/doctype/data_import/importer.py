# Copyright (c) 2020, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

import io
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
from copy import deepcopy
import requests

INVALID_VALUES = ("", None)
MAX_ROWS_IN_PREVIEW = 10
INSERT = "Insert New Records"
UPDATE = "Update Existing Records"
DURATION_PATTERN = re.compile(r"^(?:(\d+d)?((^|\s)\d+h)?((^|\s)\d+m)?((^|\s)\d+s)?)$")
SPLIT_ROWS_AT = 100


class Importer:
	def __init__(self, doctype, data_import=None, file_path=None, import_type=None, console=False, custom_import_type=None, from_func=None): #////
		self.doctype = doctype
		self.console = console
		self.custom_import_type = custom_import_type #////
		self.from_func = from_func #////

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
			self.custom_import_type, #////
			self.data_import, #////
			self.from_func #////
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
		frappe.cache().hdel("lang", frappe.session.user)
		frappe.set_user_lang(frappe.session.user)

		# set flags
		frappe.flags.in_import = True
		frappe.flags.mute_emails = self.data_import.mute_emails

		self.data_import.db_set("template_warnings", "")

	def import_data(self):
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
		#////
		if (
			(self.data_import.status == "Partial Success"
			and len(import_log) >= self.data_import.payload_count)
			or (self.data_import.status == "Splited Import Started"
			and len(import_log) >= self.data_import.payload_count)
		):
		#////
			# remove previous failures from import log only in case of retry after partial success
			import_log = [log for log in import_log if log.get("success")]

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
						)
					continue

				try:
					start = timeit.default_timer()
					doc = self.process_doc(doc)
					processing_time = timeit.default_timer() - start
					eta = self.get_eta(current_index, total_payload_count, processing_time)

					if self.console:
						update_progress_bar(
							f"Importing {total_payload_count} records",
							current_index,
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
					if self.doctype != "Item": #////
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
		failures = [log for log in import_log if not log.get("success")]
		#////
		if self.data_import.db_get("last_line"):
			if self.data_import.db_get("last_line") == self.data_import.total_lines-1:
				if len(failures) == self.data_import.db_get("payload_count"):
					status = "Pending"
				elif len(failures) > 0:
					status = "Partial Success"
				else:
					status = "Success"
			else:
				status = "Splited Import Started"
		else:
			if len(failures) == total_payload_count:
				status = "Pending"
			elif len(failures) > 0:
				status = "Partial Success"
			else:
				status = "Success"
		#////

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
	def __init__(self, doctype, file, template_options=None, import_type=None, custom_import_type=None, doctype_data=None, from_func=None): #////
		self.custom_import_type = custom_import_type #////
		self.doctype_data = doctype_data #////
		self.from_func = from_func #////
		self.doctype = doctype
		self.template_options = template_options or frappe._dict(column_to_field_map=frappe._dict())
		self.column_to_field_map = self.template_options.column_to_field_map
		self.import_type = import_type
		self.warnings = []

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

		#////

		attributes_index  = []
		parent_id_index = None
		added_lines = self.doctype_data.db_get("added_lines") if self.doctype_data.db_get("added_lines") else 0
		type_index = None
		id_index = None
		sku_index = None
		category_index = None
		error_msg = ""
		attributes_value_index = []
		created_attributes = {}
		list_of_parents = {}
		images_field_index = None
		billing_email_index = None
		billing_firstname_index = None
		billing_lastname_index = None
		billing_address_1_index = None
		billing_address_2_index = None
		billing_city_index = None
		billing_postcode_index = None
		billing_state_index = None
		billing_country_index = None
		billing_phone_index = None
		shipping_firstname_index = None
		shipping_lastname_index = None
		shipping_address_1_index = None
		shipping_address_2_index = None
		shipping_city_index = None
		shipping_postcode_index = None
		shipping_state_index = None
		shipping_country_index = None
		shipping_phone_index = None
		billing_company_index = None
		shipping_company_index = None
		firstname_index = None
		lastname_index = None
		user_email_index = None
		status_index = None

		address_id_index = None
		description_index = None
		short_description_index = None
		quantity_index = None
		price_index = None
		units_index = None
		archive_no_index = None
		total_index = None
		vat_index = None
		ref_index = None
		address_name_index = None
		address_name_title_index = None
		address_company_index = None
		address_line1_index = None
		address_line2_index = None
		address_pincode_index = None
		address_city_index = None
		address_country_index = None
		address_phone_index = None
		date_archive_index = None
		type_line_index = None
		bank_iban_index = None
		name_index = None
		product_type_index = None
		buying_price_index = None
		selling_price_index = None
		other_selling_price_index = None
		manage_stock_index = None
		additional_cat = None
		taxable_index = None
		tax_rate_index = None
		brand_index = None


		last_archive_no = None
		last_full_name = []
		created_cats = []
		new_row = []
		additional_categories = []

		manage_stock = 0
		stock_index = None
		default_company = frappe.defaults.get_global_default("company")
		valuation_rate = 0

		base_row_length = len(self.raw_data[0])

		from neoffice_theme.events import get_customer_config
		customer_config = get_customer_config()
		has_ecommerce = customer_config.get('ecommerce')
		import copy
		import re
		import unicodedata
		regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
		split_value = SPLIT_ROWS_AT
		if self.doctype_data.sync_with_woocommerce == 1:
			split_value = WC_SPLIT_ROWS_AT

		data_length = len(self.raw_data)
		if not self.doctype_data.total_lines:
			self.doctype_data.db_set("total_lines", data_length, update_modified=False)
		
		last_line = self.doctype_data.db_get("last_line")
		if last_line and last_line > 0:
			start_line = self.doctype_data.db_get("last_line") +1
		else:
			start_line = 0
		#////

		for i, row in enumerate(self.raw_data):
			#////
			if (i == start_line + split_value+1) and self.from_func == "start_import":
				self.doctype_data.db_set("last_line", i-1)
				break

			if i > 0 and i < start_line:
				if self.from_func == "start_import":
					#frappe.log_error("continue")
					continue

			if ((i < data_length-1 and i == start_line + split_value) or (i == data_length-1)) and self.from_func == "start_import":
				self.doctype_data.db_set("last_line", i)
			#////

			if all(v in INVALID_VALUES for v in row):
				# empty row
				continue

			if not header:
				#////
				if self.doctype_data.import_source == "Woocommerce" and self.from_func == "start_import":
					if self.doctype == "Item":
						row.extend(["image", "woocommerce_img_1", "woocommerce_img_2", "woocommerce_img_3", "woocommerce_img_4", "woocommerce_img_5", "maintain_stock","has_variants","parent_sku", "attribute_name", "attribute_value",
						"sync_with_woocommerce", "default_warehouse", "item_group", "category_ecommerce", "default_company", "woocommerce_warehouse", "stock", "valuation_rate", "standard_rate", "additionnal_categories", "description",
						"woocommerce_taxable", "woocommerce_tax_rate", "weight_uom", "brand", "brand_ecommerce"])
						image_index = row.index("image")
						for (index, item) in enumerate(row):
							if item == "ID":
								id_index = index
							elif item == "Content":
								description_index = index
							#elif item == "Excerpt":
							#	short_description_index = index
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
							elif item == "Image URL":
								images_field_index = index
							elif item == "Catégories de produits":
								category_index = index
							elif item == "Product Type":
								type_index = index
							elif item == "Manage Stock":
								manage_stock_index = index
							elif item == "Tax Status":
								taxable_index = index
							elif item == "Tax Class":
								tax_rate_index = index
							elif item == "Marques":
								brand_index = index
							#frappe.msgprint(str(attributes_name))
							#frappe.msgprint(str(attributes_index))

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
							elif item == "Billing Address Line 1":
								billing_address_1_index = index
							elif item == "Billing Address Line 2":
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
							elif item == "Shipping Address Line 1":
								shipping_address_1_index = index
							elif item == "Shipping Address Line 2":
								shipping_address_2_index = index
							elif item == "Shipping City":
								shipping_city_index = index
							elif item == "Shipping Postcode":
								shipping_postcode_index = index
							elif item == "Shipping State":
								shipping_state_index = index
							elif item == "Shipping Country":
								shipping_country_index = index
							elif item == "Shipping Phone":
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
						row.extend(["customer_name", "customer_type", "territory", "is_import"])
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
						"customer_link", "customer_text", "status", "number"])
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
							elif item == "Order Status":
								status_index = index
							elif item == "Order Line Title":
								description_index = index
							elif item == "Quantity":
								quantity_index = index
							elif item == "Item Total":
								price_index = index
							elif item == "Item Tax Total":
								vat_index = index
							elif item == "Reference":
								ref_index = index
							elif item == "Order Number":
								archive_no_index = index

				elif self.doctype_data.import_source == "Winbiz" and self.from_func == "start_import":
					if self.doctype == "Item Price":
						row.extend(["price_list", "price_list_rate"])
						for (index, item) in enumerate(row):
							if item == "ar_fn_ref":
								sku_index = index
							elif item == "ar_groupe":
								category_index = index
							elif item == "ar_abrege":
								name_index = index
							elif item == "ar_fn_ref":
								sku_index = index
							elif item == "ar_type":
								product_type_index = index
							elif item == "prixach":
								buying_price_index = index
							elif item == "prixvnt":
								selling_price_index = index

					if self.doctype == "Item":
						row.extend(["sync_with_woocommerce", "item_group", "maintain_stock", "default_warehouse", "default_company", "woocommerce_warehouse", "stock", "valuation_rate", "category_ecommerce", "standard_rate"])
						for (index, item) in enumerate(row):
							if item == "ar_groupe":
								category_index = index
							elif item == "ar_qteini":
								stock_index = index
							elif item == "prixvnt":
								selling_price_index = index

					if self.doctype == "Data Archive":
						row.extend(["source", "type", "lines.reference", "lines.description", "lines.units", "lines.quantity", "lines.total_price_excl_taxes", "lines.total_vat", "lines.total_price_incl_taxes", "date",
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
						row.extend(["link_doctype", "link_name", "email_id", "is_primary_email", "phone", "number", "is_primary_phone"])
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

					elif self.doctype == "Address":
						row.extend(["address_title", "address_type", "country", "link_doctype", "link_name"])
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

					elif self.doctype == "Customer":
						row.extend(["customer_name", "customer_type", "territory", "is_import"])
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
						row.extend(["supplier_name", "supplier_type", "country", "supplier_group"])
						for (index, item) in enumerate(row):
							#frappe.msgprint(item)
							if item == "AB_ADRESSE":
								address_id_index = index
							elif item == "AB_IBAN":
								bank_iban_index = index
				#////

				header = Header(i, row, self.doctype, self.raw_data, self.column_to_field_map, self.doctype_data, self.from_func) #////
			else:
				#////
				add_row_in_data = True
				if self.doctype_data.import_source == "Woocommerce" and self.from_func == "start_import":
					if self.doctype == "Item":
						attributes_value = []
						attributes_name  = []
						parent_sku = None
						sku_prefix = "Neoffice Product "
						sku_suffix = 1
						while frappe.get_all("Item", filters={"name": sku_prefix + str(sku_suffix)}):
							sku_suffix += 1

						split_cats = row[category_index].split("|")
						for idx_nb, cat in enumerate(split_cats):
							#tree = cat.split(">")
							#if tree[-1] not in created_cats:
							root = self.doctype_data.root_category
							last_cat = root
							if not frappe.db.get_value("Item Group", {"group_tree": root+">"+cat}, "name"):
								for c in cat.split(">"):
									this_cat = last_cat + ">"+c
									if not frappe.db.get_value("Item Group", {"group_tree": this_cat}, "name"):
										parent_group = frappe.db.get_value("Item Group", {"group_tree": last_cat}, "name")
										if not frappe.db.exists("Item Group", {"name": c}):
											cat_doc = frappe.get_doc({
												"doctype": "Item Group",
												"item_group_name": c,
												"parent_item_group": parent_group,
												"is_group": 1,
												"group_tree": this_cat
											})
										elif parent_group == "Ecommerce" :
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
							'''for index in range(len(tree)):
								if tree[index] not in created_cats:
									if(index == 0):
										parent = self.doctype_data.root_category
										parent_tree = parent
									else:
										parent_tree = self.doctype_data.root_category + ">"
										parent_tree += ">".join([e for j, e in enumerate(tree) if j in range(0,index)])
										parent_filtered = frappe.get_all("Item Group", filters={"group_tree": parent_tree})
										if parent_filtered:
											parent = parent_filtered[0].name
										else:
											frappe.throw("Parent Category not found: " + tree[index])

									group_tree = parent_tree + ">" + tree[index]
									filtered_groups = frappe.get_all("Item Group", filters={"group_tree": group_tree})
									if not filtered_groups:
										if not frappe.db.exists("Item Group", {"name": tree[index]}):
											#created_cats.append(tree[index])
											cat_doc = frappe.get_doc({
												"doctype": "Item Group",
												"item_group_name": tree[index],
												"parent_item_group": parent,
												"is_group": 1,
												"group_tree": group_tree
											})
											cat_doc.insert()
											frappe.db.commit()
											#created_cats.append(tree[index])
										elif not frappe.db.exists("Item Group", {"name": parent + ' - ' + tree[index]}):
											#created_cats.append(tree[index])
											cat_doc = frappe.get_doc({
												"doctype": "Item Group",
												"item_group_name": parent + ' - ' + tree[index],
												"parent_item_group": parent,
												"is_group": 1,
												"group_tree": group_tree
											})
											cat_doc.insert()
											frappe.db.commit()
											#created_cats.append(tree[index])
										else:
											index_to_append = 1
											composed_name = parent + ' - ' + tree[index]
											while frappe.db.exists("Item Group", {"name": composed_name + " " + str(index_to_append)}):
												index_to_append += 1
											#created_cats.append(tree[index])
											cat_doc = frappe.get_doc({
												"doctype": "Item Group",
												"item_group_name": composed_name + " " + str(index_to_append),
												"parent_item_group": parent,
												"is_group": 1,
												"group_tree": group_tree
											})
											cat_doc.insert()
											frappe.db.commit()
											#created_cats.append(tree[index])'''
							#cat_name = frappe.get_all("Item Group", filters={"group_tree": self.doctype_data.root_category + ">" + ">".join(tree)})[0].name
							if idx_nb == 0:
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
										attribute_to_create = row[index-1]
										terms_to_create = item.split('|')
										if not attribute_to_create in created_attributes:
											created_attributes[attribute_to_create] = []
										#frappe.log_error("{0}".format(frappe.get_all("Item Attribute", filters=[{"name": attribute_to_create}])), f"all item attribute for {attribute_to_create}")
										if len(frappe.get_all("Item Attribute", filters=[{"name": attribute_to_create}])) == 0 and self.from_func == "start_import":
											item_attribute_values = []
											for term in terms_to_create:
												item_attribute_values.append({"attribute_value": term.strip(), "abbr": term.strip().upper()})
												created_attributes[attribute_to_create] += [term.strip()]
											attr_doc = frappe.get_doc({'doctype': "Item Attribute", 'attribute_name': attribute_to_create, 'item_attribute_values': item_attribute_values})
											attr_doc.insert()
											frappe.db.commit()
										else:
											for term in terms_to_create:
												if term.strip() not in created_attributes[attribute_to_create]:
													#frappe.log_error("{0}".format(term), "term")
													#frappe.log_error("{0}".format(frappe.get_all("Item Attribute", filters=[{"name": attribute_to_create}, ["Item Attribute Value", "attribute_value", "=", term.strip()]])), f"all item attribute value for {attribute_to_create} - {term.strip()}")
													if len(frappe.get_all("Item Attribute Value", filters=[{"attribute_value": term.strip(), "parent": attribute_to_create}])) == 0 and self.from_func == "start_import":
														attr_val_doc = frappe.get_doc({"doctype":"Item Attribute Value", "parent": attribute_to_create, "parentfield": "item_attribute_values", "parenttype": "Item Attribute", "attribute_value": term.strip(), "abbr": term.strip().upper()})
														attr_val_doc.insert()
														created_attributes[attribute_to_create] += [term.strip()]
											frappe.db.commit()
						for (index, item) in enumerate(row):
							if index in attributes_index:
								attribute_name = item
								#attributes_name.append(item)
							if index in attributes_value_index and item:
								attributes_name.append(attribute_name)
								attributes_value.append(item)

						row.extend([None, None, None, None, None, None])
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
								found_files = frappe.get_all("File", filters={"file_name": image_name})
								if not found_files:
									image_data = requests.get(item_image[0]).content
									try:
										file_doc = frappe.get_doc({
											"doctype": "File",
											"file_name": image_name,
											"content": image_data,
											"is_private": 0
										})
										file_doc.insert()
										frappe.db.commit()
										image_url = frappe.db.get_value("File", file_doc.name, "file_url")
										row[image_index] = image_url
									except:
										frappe.log_error(f"file {image_name} not inserted")
								else:
									link_file = frappe.get_doc("File", found_files[0].name)
									row[image_index] = link_file.file_url

							elif len_item_image > 1:
								for index,image in enumerate(item_image):
									if index > 5:
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
									found_files = frappe.get_all("File", filters={"file_name": image_name})
									if not found_files:
										image_data = requests.get(image).content
										try:
											file_doc = frappe.get_doc({
												"doctype": "File",
												"file_name": image_name,
												"content": image_data,
												"is_private": 0
											})
											file_doc.insert()
											frappe.db.commit()
											image_url = frappe.db.get_value("File", file_doc.name, "file_url")
											if index == 0:
												row[image_index] = image_url
											else:
												row[image_index+index] = image_url
										except:
											frappe.log_error(f"file {image_name} not inserted")
									else:
										link_file = frappe.get_doc("File", found_files[0].name)
										if index == 0:
											row[image_index] = link_file.file_url
										else:
											row[image_index+index] = link_file.file_url

							command = "/usr/bin/php /home/neoffice/frappe-bench/sites/web/wp-content/plugins/bulk-media-register-add-on-wpcron/lib/bmrcroncli.php"
							subprocess.run(command, capture_output=False, shell=True)
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
							error_msg += f"Can't find parent product with ID {item}\n"
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

						description = None if not row[description_index] else row[description_index].replace("_x000D_", "\n")
						is_vat = 0 if row[taxable_index] == "Aucune" else 1
						tax_class = {"parent": "parent", "Taux réduit": "Taux réduit", "Taux zéro": "Taux zéro"}.get(row[tax_rate_index], "Standard")
						if(len(attributes_value) == 0):
							row.extend([manage_stock, is_parent, parent_sku, None, None, self.doctype_data.sync_with_woocommerce, self.doctype_data.warehouse, row[category_index], row[category_index],
							default_company, self.doctype_data.warehouse, stock, valuation_rate, price, additional_cat, description, is_vat, tax_class, "Kg", brand, brand])
						else:
							attribute_value = attributes_value[0]
							if row[parent_id_index] == 0:
								attribute_value = None
							row.extend([manage_stock, is_parent, parent_sku, attributes_name[0], attribute_value, self.doctype_data.sync_with_woocommerce, self.doctype_data.warehouse, row[category_index], row[category_index],
							default_company, self.doctype_data.warehouse, stock, valuation_rate, price, additional_cat, description, is_vat, tax_class, "Kg", brand, brand]) 

						if index % 100 == 0 or index == data_length - 1:
							pass
							command = "/usr/bin/php /home/neoffice/frappe-bench/sites/web/wp-content/plugins/bulk-media-register-add-on-wpcron/lib/bmrcroncli.php"
							subprocess.run(command, capture_output=False, shell=True)

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

								if row[billing_country_index]:
									countries = frappe.get_all("Country", filters={"code": row[billing_country_index].lower()})
									if countries:
										country = countries[0].name
									else:
										country = None
									#country = "Suisse" if _(pycountry.countries.get(alpha_2=row[billing_country_index]).name) == "Switzerland" else self.doctype_data.default_territory #!!!!
								elif row[shipping_country_index]:
									countries = frappe.get_all("Country", filters={"code": row[shipping_country_index].lower()})
									if countries:
										country = countries[0].name
									else:
										country = None
									#country = "Suisse" if _(pycountry.countries.get(alpha_2=row[shipping_country_index]).name) == "Switzerland" else self.doctype_data.default_territory #!!!!
								else:
									country = self.doctype_data.default_territory
								if final_name:
									row.extend([final_name, customer_type, country, 1])
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
							customer_link, customer_text, row[status_index].replace("wc-", ""), "Woo-" + row[archive_no_index]])
							last_archive_no = row[archive_no_index]
						else:# The above code is appending the data archive lines
							ref = row[ref_index]
							description = row[description_index]
							quantity = row[quantity_index]
							price = row[price_index]
							vat = row[vat_index]
							row = [None] * len(row)
							row.extend([None, None, ref, description, quantity, price_vat_excluded, vat, price, None, None, None, None])

				elif self.doctype_data.import_source == "Winbiz" and self.from_func == "start_import":
					if self.doctype == "Item Price":
						if row[product_type_index] == 1:
							new_row = copy.deepcopy(row)
							row.extend(["Standard Selling", row[selling_price_index]])
						else:
							continue

					if self.doctype == "Item":
						if row[category_index] and row[category_index] not in created_cats:
							'''parent = self.doctype_data.root_category
							if not frappe.db.exists("Item Group", {"name": row[category_index]}):
								cat_doc = frappe.get_doc({
									"doctype": "Item Group",
									"item_group_name": row[category_index],
									"parent_item_group": parent,
									"is_group": 1
								})
								cat_doc.insert()
								frappe.db.commit()
								created_cats.append(row[category_index])'''

							current_cat = row[category_index]
							parent = self.doctype_data.root_category
							parent_tree = parent

							group_tree = parent_tree + ">" + current_cat
							filtered_groups = frappe.get_all("Item Group", filters={"group_tree": group_tree})
							if not filtered_groups:
								if not frappe.db.exists("Item Group", {"name": current_cat}):
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
									created_cats.append(current_cat)
						if row[category_index]:
							cat_name = frappe.get_all("Item Group", filters={"group_tree": self.doctype_data.root_category + ">" + row[category_index]})[0].name
						else:
							cat_name = self.doctype_data.root_category
						item_group = cat_name
						if self.doctype_data.manage_stock:
							manage_stock = 1
							stock = 0 if row[stock_index] < 0 else int(row[stock_index])
						else:
							manage_stock = 0
							stock = None
						standard_rate = row[selling_price_index]
						row.extend([self.doctype_data.sync_with_woocommerce, item_group, manage_stock, self.doctype_data.warehouse, default_company,
						self.doctype_data.warehouse, stock, valuation_rate, item_group, standard_rate])


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

						if last_archive_no != row[archive_no_index]:
							#frappe.msgprint("Archive No: " + str(row[archive_no_index]) + " is being imported")
							type_line = {"20":"Invoice", "10": "Offer", "12":"Order Confirmation", "14":"Worksheet"}.get(str(row[type_line_index]), None)
							row.extend(["Winbiz", _(type_line), row[ref_index], row[description_index], row[units_index], row[quantity_index], price_vat_excluded, row[vat_index], row[price_index],
							row[date_archive_index], customer_link, customer_text, "Win-" + str(row[archive_no_index])])
							last_archive_no = row[archive_no_index]
						else:# The above code is appending the data archive lines
							ref = row[ref_index]
							description = row[description_index]
							units = row[units_index]
							quantity = row[quantity_index]
							price = row[price_index]
							vat = row[vat_index]
							row = [None] * len(row)
							row.extend([None, None, ref, description, units, quantity, price_vat_excluded, vat, price, None, None, None, None])

					elif self.doctype == "Contact":
						if not row[user_email_index] or row[user_email_index].strip() == "":
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

						if not frappe.db.get_value("Contact", {"winbiz_address_number": row[address_id_index]}, "name") and not frappe.db.get_value("Contact", {"email_id": row[user_email_index]}, "name"):
							first_name = row[firstname_index] if row[firstname_index] else (row[address_company_index] if row[address_company_index] else row[lastname_index])
							last_name = row[lastname_index] if row[lastname_index] and (row[firstname_index] or row[address_company_index]) else None
							email_ids = [{"email_id":row[user_email_index], "is_primary":1}]
							contact_number = str(row[address_phone_index]) if row[address_phone_index] else None
							links = [{"link_doctype": "Customer", "link_name": customer_name}],
							row.extend([first_name, last_name, email_ids, contact_number, links])
							row.extend(["Customer", customer_name, row[user_email_index], 1, str(row[address_phone_index]), contact_number, 1])
						else:
							continue

					elif self.doctype == "Address":
						if not row[user_email_index] or row[user_email_index].strip() == "":
							continue

						'''customer_with_address_number = frappe.db.get_all("Customer", filters={"winbiz_address_number": row[address_id_index]})
						if customer_with_address_number:
							customer_name = customer_with_address_number[0].name
						else:
							customer_name = None

						filtered_contacts = frappe.get_all("Contact", filters={"winbiz_address_number": row[address_id_index]})
						if not filtered_contacts:
							if row[user_email_index] and re.fullmatch(regex,row[user_email_index]):
								contact_email = [{"email_id":row[user_email_index], "is_primary":1}]
							else:
								contact_email = archive_no_index
							if self.from_func == "start_import":
								frappe.get_doc({"doctype": "Contact", "email_ids": contact_email,
								"first_name": row[firstname_index] if row[firstname_index] else (row[address_company_index] if row[address_company_index] else row[lastname_index]), "last_name": row[lastname_index],
								"links": [{"link_doctype": "Customer", "link_name": customer_name}], "winbiz_address_number": row[address_id_index],
								"email_ids": contact_email if row[user_email_index] else []}).insert()
								frappe.db.commit()
						else:
							frappe.msgprint("Contact already exists")'''

						if not frappe.get_all("Address", filters={"winbiz_address_number": row[address_id_index]}):
							title_formatted = ""
							if row[address_company_index]:
								title_formatted += f"{row[address_company_index]} "
							if row[lastname_index]:
								title_formatted += f"{row[lastname_index]} "
							if row[firstname_index]:
								title_formatted += row[firstname_index]

							counter = 0
							if frappe.get_all("Address", filters={"address_title": title_formatted}):
								counter += 1
								while frappe.get_all("Address", filters={"address_title": title_formatted + " " + str(i)}):
									counter += 1
							if counter > 0:
								title_formatted += " " + str(counter)

							if row[address_country_index]:
								countries = frappe.get_all("Country", filters={"code": row[address_country_index].lower()})
								if countries:
									country = countries[0].name
								else:
									country = None
								#country = "Suisse" if _(pycountry.countries.get(alpha_2=row[address_country_index]).name) == "Switzerland" else "Suisse" #!!!! _(pycountry.countries.get(alpha_2=row[address_country_index]).name)
							else:
								country = "Suisse" #!!!!_("Switzerland")

							row.extend([title_formatted, "Billing", country, "Customer", customer_name])
						else:
							add_row_in_data = False

					elif self.doctype == "Customer":
						if not row[user_email_index] or row[user_email_index].strip() == "":
							continue

						if row[address_company_index]:
							full_name = row[address_company_index]
							customer_type = "Company"
						else:
							full_name = row[address_name_index]
							customer_type = "Individual"

						if last_full_name and not full_name.lower() in last_full_name[0]:
							last_full_name = []

						if row[address_country_index]:
							countries = frappe.get_all("Country", filters={"code": row[address_country_index].lower()})
							if countries:
								country = "Suisse" if row[address_country_index] == "CH" else self.doctype_data.default_territory
							else:
								country = self.doctype_data.default_territory
							#country =  "Suisse"#!!!!_("Switzerland") if _(pycountry.countries.get(alpha_2=row[address_country_index]).name) == "Switzerland" else self.doctype_data.default_territory
						else:
							country = self.doctype_data.default_territory

						final_name = None
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
						last_full_name.append(final_name.lower())

						row.extend([final_name, customer_type, country, 1])

					elif self.doctype == "Supplier":
						suppliers = frappe.get_all("Supplier", filters={'winbiz_address_number': row[address_id_index]})
						if not suppliers:
							customers = frappe.get_all("Customer", filters={'winbiz_address_number': row[address_id_index]})
							if customers:
								base_customer = frappe.get_doc("Customer",customers[0])
								row.extend([base_customer.customer_name, base_customer.customer_type, None, "All Supplier Groups"])
							else:
								add_row_in_data = False
						else:
							add_row_in_data = False
				#////
				row_obj = Row(i+added_lines, row, self.doctype, header, self.import_type) #////
				if add_row_in_data: #////
					data.append(row_obj)
		#////
				if self.doctype_data.import_source == "Woocommerce" and new_row and self.from_func == "start_import":
					if self.doctype == "Item":
						if row[parent_id_index] == 0:
							parent_sku = None
						else:
							parent_sku = list_of_parents.get(row[parent_id_index], "error")
							if parent_sku == "error":
								parent_list = frappe.get_all("Item", filters={"import_id": row[parent_id_index]})
								if parent_list:
									parent_sku = parent_list[0].name

						if parent_sku == "error":
							error_msg += f"Can't find parent product with ID {item}\n"

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
							new_row = [None] * (base_row_length+6) # +6 because 6 images
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
							new_row.extend([None, None, None, row_attribute_name, row_attribute_value, None, None, None, None,
							None, None, None, None, None, additional_cat, None, None, None, None, None, None])

							row_obj = Row(i+added_lines, new_row, self.doctype, header, self.import_type)
							data.append(row_obj)
							new_row = []
							#frappe.msgprint(str(new_row))

					elif self.doctype == "Address":
						if customer_name:
							added_lines += 1
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

				if self.doctype_data.import_source == "Winbiz" and new_row and self.from_func == "start_import":
					if self.doctype == "Item Price":
						added_lines += 1
						new_row.extend(["Standard Buying", row[buying_price_index]])
						row_obj = Row(i+added_lines, new_row, self.doctype, header, self.import_type)
						data.append(row_obj)
						new_row = []

		if error_msg:
			frappe.throw(error_msg)
		if self.from_func == "start_import":
			self.doctype_data.db_set("added_lines", added_lines)
		#////
		self.header = header
		self.columns = self.header.columns
		self.data = data

		if len(data) < 1:
			frappe.throw(
				_("Import template should contain a Header and atleast one row."),
				title=_("Template Error"),
			)

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

		data = [[row.row_number] + row.as_list() for row in self.data]

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
			parent_row_values = first_row.get_values(parent_column_indexes)

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

	def read_file(self, file_path):
		extn = os.path.splitext(file_path)[1][1:]

		file_content = None
		with open(file_path, mode="rb") as f:
			file_content = f.read()

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
	link_values_exist_map = {}

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
				"Row has less values than columns" if less_than_columns else "Row has more values than columns"
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
		doc = self._parse_doc(doctype, columns, values, parent_doc, table_df)
		return doc

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

		for col, value in zip(columns, values):
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
			if select_options and value not in select_options:
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
		key = df.options + "::" + cstr(value)
		if Row.link_values_exist_map.get(key) is None:
			Row.link_values_exist_map[key] = frappe.db.exists(df.options, value)
		return Row.link_values_exist_map.get(key)

	def parse_value(self, value, col):
		df = col.df
		if isinstance(value, (datetime, date)) and df.fieldtype in ["Date", "Datetime"]:
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
		if isinstance(value, (datetime, date)):
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
	def __init__(self, index, row, doctype, raw_data, column_to_field_map=None, doctype_data=None, from_func=None): #////
		self.doctype_data = doctype_data #////
		self.from_func = from_func #////
		self.index = index
		self.row_number = index + 1
		self.data = row
		self.doctype = doctype
		column_to_field_map = column_to_field_map or frappe._dict()

		self.seen = []
		self.columns = []

		for j, header in enumerate(row):
			column_values = [get_item_at_index(r, j) for r in raw_data]
			#////
			if self.doctype_data.import_source == "Woocommerce":
				if self.doctype == "Item":
					map_to_field = {"Title": "item_name", "Sku": "item_code", "description": "woocommerce_long_description", "Excerpt":"woocommerce_short_description", "item_group": "item_group", "maintain_stock": "is_stock_item",
					"parent_sku":"variant_of", "attribute_name": "attributes.attribute", "attribute_value": "attributes.attribute_value", "has_variants": "has_variants", "sync_with_woocommerce" : "sync_with_woocommerce", "image": "image",
					"woocommerce_img_1":"woocommerce_img_1", "woocommerce_img_2":"woocommerce_img_2", "woocommerce_img_3":"woocommerce_img_3", "woocommerce_img_4":"woocommerce_img_4",
					"woocommerce_img_5":"woocommerce_img_5", "default_warehouse": "item_defaults.default_warehouse", "category_ecommerce": "category_ecommerce", "woocommerce_warehouse": "woocommerce_warehouse",
					"stock": "opening_stock", "valuation_rate": "valuation_rate", "standard_rate": "standard_rate", "default_company": "item_defaults.company", "ID": "import_id",
					"additionnal_categories": "additional_ecommerce_categories.item_group", "woocommerce_taxable": "woocommerce_taxable", "woocommerce_tax_rate": "woocommerce_tax_rate", "brand":"brand", "brand_ecommerce":"brand_ecommerce",
					"Weight": "weight_per_unit", "weight_uom": "weight_uom"}.get(header, "Don't Import")

				elif self.doctype == "Item Price":
					"price_list", "price_list_rate"

				elif self.doctype == "Address":
					map_to_field = {"User Email": "woocommerce_email", "address_title": "address_title", "address_type": "address_type", "phone": "phone", "address_line1": "address_line1",
					"address_line2":"address_line2", "city": "city", "country": "country", "postcode": "pincode", "link_doctype": "links.link_doctype",
					"link_name":"links.link_name", "email_id": "email_id"}.get(header, "Don't Import")

				elif self.doctype == "Contact":
					map_to_field = {"User Email": "email_id", "first_name": "first_name", "Last Name": "last_name", "phone": "phone", "link_doctype": "links.link_doctype",
					"link_name":"links.link_name", "email_id": "email_ids.email_id", "is_primary_email":"email_ids.is_primary"}.get(header, "Don't Import")

				elif self.doctype == "Customer":
					map_to_field = {"User Email": "email_id", "customer_type": "customer_type", "territory": "territory", "customer_name": "customer_name", "is_import": "is_import"}.get(header, "Don't Import")

				elif self.doctype == "Data Archive":
					map_to_field = {"source": "source", "type": "type", "number": "number", "Total Order": "total", "lines.reference": "lines.reference", "lines.description": "lines.description", "lines.units": "lines.units",
					"lines.quantity": "lines.quantity", "lines.total_price_excl_taxes": "lines.total_price_excl_taxes", "lines.total_vat": "lines.total_vat",
					"lines.total_price_incl_taxes": "lines.total_price_incl_taxes", "customer_link": "customer_link", "customer_text": "customer_text", "Order Date": "date", "status": "status",
					"Payment Method Title":"payment_method", "Shipping Method": "shipping_method", "Shipping Fees": "shipping _fees", "Currency": "currency"}.get(header, "Don't Import")

			elif self.doctype_data.import_source == "Winbiz":
				if self.doctype == "Item":
					map_to_field = {"ar_abrege": "item_name", "ar_fn_ref": "item_code", "ar_desc": "woocommerce_long_description", "item_group": "item_group", "sync_with_woocommerce" : "sync_with_woocommerce",
					"maintain_stock": "is_stock_item", "default_warehouse": "item_defaults.default_warehouse", "category_ecommerce": "category_ecommerce", "woocommerce_warehouse": "woocommerce_warehouse", "stock": "opening_stock", "valuation_rate": "valuation_rate", "standard_rate": "standard_rate", "default_company": "item_defaults.company"}.get(header, "Don't Import")

				elif self.doctype == "Item Price":
					map_to_field = {"ar_fn_ref": "item_code", "price_list": "price_list", "price_list_rate": "price_list_rate"}.get(header, "Don't Import")

				if self.doctype == "Data Archive":
					map_to_field = {"source": "source", "type": "type", "number": "number", "do_montant": "total", "lines.reference": "lines.reference", "lines.description": "lines.description", "lines.units": "lines.units",
					"lines.quantity": "lines.quantity", "lines.total_price_excl_taxes": "lines.total_price_excl_taxes", "lines.total_vat": "lines.total_vat",
					"lines.total_price_incl_taxes": "lines.total_price_incl_taxes", "customer_link": "customer_link", "customer_text": "customer_text", "date": "date"}.get(header, "Don't Import")

				elif self.doctype == "Customer":
					map_to_field = {"ad_email": "email_id", "customer_type": "customer_type", "territory": "territory", "customer_name": "customer_name", "ad_numero": "winbiz_address_number", "is_import": "is_import"}.get(header, "Don't Import")

				elif self.doctype == "Contact":
					map_to_field = {"ad_email": "email_id", "first_name": "first_name", "last_name": "last_name", "phone": "phone", "link_doctype": "links.link_doctype",
					"link_name":"links.link_name", "email_id": "email_ids.email_id", "is_primary_email":"email_ids.is_primary", "number": "phone_nos.phone", "is_primary_phone": "phone_nos.is_primary_phone"}.get(header, "Don't Import")

				elif self.doctype == "Address":
					map_to_field = {"ad_email": "email_id", "address_title": "address_title", "address_type": "address_type", "ad_tel1": "phone", "ad_rue_1": "address_line1",
					"ad_rue_2":"address_line2", "ad_ville": "city", "country": "country", "ad_npa": "pincode", "link_doctype": "links.link_doctype",
					"link_name":"links.link_name", "ad_numero": "winbiz_address_number"}.get(header, "Don't Import")

				elif self.doctype == "Supplier":
					map_to_field = {"supplier_name": "supplier_name", "supplier_type": "supplier_type", "supplier_group": "supplier_group", "country": "country", "AB_IBAN": "iban"}.get(header, "Don't Import")
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
	seen = []
	fields_column_map = {}

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
			self.warnings.append(
				{
					"col": column_number,
					"message": _("Skipping column {0}").format(frappe.bold(header_title)),
					"type": "info",
				}
			)
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
			if isinstance(d, (datetime, date, time)):
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

		if self.df.fieldtype == "Link":
			# find all values that dont exist
			values = list({cstr(v) for v in self.column_values[1:] if v})
			exists = [
				cstr(d.name) for d in frappe.get_all(self.df.options, filters={"name": ("in", values)})
			]
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
				values = {cstr(v) for v in self.column_values[1:] if v}
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
			parent = df.parent or parent_doctype

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

	df_by_labels_and_fieldname = frappe.cache().hget(
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
	return (
		date_format.replace("%Y", "yyyy").replace("%y", "yy").replace("%m", "mm").replace("%d", "dd")
	)


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
