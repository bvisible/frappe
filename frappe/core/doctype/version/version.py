# Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE

import difflib
import json

import frappe
from frappe.model import datetime_fields, no_value_fields, table_fields
from frappe.model.document import Document
from frappe.utils import cstr

FIELDTYPES_TO_IGNORE = frozenset(fieldtype for fieldtype in no_value_fields if fieldtype not in table_fields)


class Version(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		data: DF.Code | None
		docname: DF.Data
		ref_doctype: DF.Link

	# end: auto-generated types
	def update_version_info(self, old: Document | None, new: Document) -> bool:
		"""Update changed info and return true if change contains useful data."""
		if not old:
			# Check if doc has some information about creation source like data import
			return self.for_insert(new)
		else:
			return self.set_diff(old, new)

	@staticmethod
	def set_impersonator(data):
		if not frappe.session:
			return
		if impersonator := frappe.session.data.get("impersonated_by"):
			data["impersonated_by"] = impersonator

		if audit_user := frappe.session.data.get("audit_user"):
			data["audit_user"] = audit_user

	def set_diff(self, old: Document, new: Document) -> bool:
		"""Set the data property with the diff of the docs if present"""
		diff = get_diff(old, new)
		if diff:
			self.set_impersonator(diff)
			self.ref_doctype = new.doctype
			self.docname = new.name
			self.data = frappe.as_json(diff, indent=None, separators=(",", ":"))
			return True
		else:
			return False

	def for_insert(self, doc: Document) -> bool:
		updater_reference = doc.flags.updater_reference
		if not updater_reference:
			return False

		data = {
			"creation": doc.creation,
			"updater_reference": updater_reference,
			"created_by": doc.owner,
		}
		self.set_impersonator(data)
		self.ref_doctype = doc.doctype
		self.docname = doc.name
		self.data = frappe.as_json(data, indent=None, separators=(",", ":"))
		return True

	def get_data(self):
		return json.loads(self.data)

	def onload(self):
		"""Generate HTML diffs for multiline changes on document load."""
		if not self.data:
			return

		# //// Neoffice — the diffs are drawn only from the fields the reader may read of the recorded document
		# //// (neoffice-maintenance#2296; see readable_version_json at the end of this file)
		data = json.loads(readable_version_json(self.data, self.ref_doctype) or "{}")
		changed = data.get("changed", [])
		if not changed:
			return

		html_diffs = {}
		for item in changed:
			if len(item) >= 3:
				fieldname, old_str, new_str = item[0], _as_string(item[1]), _as_string(item[2])
				if not _should_generate_html_diff(old_str, new_str):
					continue
				html_diff = _generate_html_diff(old_str, new_str)
				if html_diff:
					html_diffs[fieldname] = html_diff

		if html_diffs:
			self.set_onload("html_diffs", html_diffs)

	# //// Neoffice — the Version form and every read of a Version as a document (getdoc, frappe.client.get,
	# //// /api/resource, /api/v2) give its data only with the fields the reader may read of the document it
	# //// records (neoffice-maintenance#2296; see readable_version_json at the end of this file)
	def apply_fieldlevel_read_permissions(self):
		super().apply_fieldlevel_read_permissions()
		if self.data:
			self.data = readable_version_json(self.data, self.ref_doctype)


def get_diff(old, new, for_child=False, compare_cancelled=False):
	"""Get diff between 2 document objects

	If there is a change, then returns a dict like:

	        {
	                "changed"    : [[fieldname1, old, new], [fieldname2, old, new]],
	                "added"      : [[table_fieldname1, {dict}], ],
	                "removed"    : [[table_fieldname1, {dict}], ],
	                "row_changed": [[table_fieldname1, row_name1, row_index,
	                        [[child_fieldname1, old, new],
	                        [child_fieldname2, old, new]], ]
	                ],

	        }"""
	if not new:
		return None

	blacklisted_fields = ["Markdown Editor", "Text Editor", "Code", "HTML Editor"]

	# capture data import if set
	data_import = new.flags.via_data_import
	updater_reference = new.flags.updater_reference

	out = frappe._dict(
		changed=[],
		added=[],
		removed=[],
		row_changed=[],
		data_import=data_import,
		updater_reference=updater_reference,
	)

	if not for_child:
		amended_from = new.get("amended_from")
		old_row_name_field = "_amended_from" if (amended_from and amended_from == old.name) else "name"

	for df in new.meta.fields:
		if df.fieldtype in FIELDTYPES_TO_IGNORE or getattr(df, "is_virtual", False):
			continue

		old_value, new_value = old.get(df.fieldname), new.get(df.fieldname)
		if df.fieldtype in ("Link", "Dynamic Link"):
			old_value, new_value = cstr(old_value), cstr(new_value)

		if df.fieldtype in datetime_fields:
			if old_value is None and new_value == "":
				new_value = None

		if not for_child and df.fieldtype in table_fields:
			old_rows_by_name = {}
			for d in old_value:
				old_rows_by_name[d.name] = d

			found_rows = set()

			# check rows for additions, changes
			for i, d in enumerate(new_value):
				old_row_name = getattr(d, old_row_name_field, None)
				if compare_cancelled:
					if amended_from:
						if len(old_value) > i:
							old_row_name = old_value[i].name

				if old_row_name and old_row_name in old_rows_by_name:
					found_rows.add(old_row_name)

					diff = get_diff(old_rows_by_name[old_row_name], d, for_child=True)
					if diff and diff.changed:
						out.row_changed.append((df.fieldname, i, d.name, diff.changed))
				else:
					out.added.append([df.fieldname, d.as_dict()])

			# check for deletions
			for d in old_value:
				if d.name not in found_rows:
					out.removed.append([df.fieldname, d.as_dict()])

		elif old_value != new_value:
			if df.fieldtype not in blacklisted_fields:
				old_value = old.get_formatted(df.fieldname) if old_value else old_value
				new_value = new.get_formatted(df.fieldname) if new_value else new_value

			if old_value != new_value:
				out.changed.append((df.fieldname, old_value, new_value))

	# name & docstatus
	if not for_child:
		for key in ("name", "docstatus"):
			old_value = getattr(old, key)
			new_value = getattr(new, key)

			if old_value != new_value:
				out.changed.append([key, old_value, new_value])

	if any((out.changed, out.added, out.removed, out.row_changed)):
		return out

	else:
		return None


def on_doctype_update():
	frappe.db.add_index("Version", ["ref_doctype", "docname"])


def _generate_html_diff(old_str: str, new_str: str) -> str | None:
	"""Generate HTML diff for the given old and new strings."""
	old_lines = old_str.splitlines(keepends=True)
	new_lines = new_str.splitlines(keepends=True)

	differ = difflib.HtmlDiff(wrapcolumn=80)
	html_diff = differ.make_table(
		old_lines,
		new_lines,
		fromdesc=frappe._("Original"),
		todesc=frappe._("New"),
		context=True,
		numlines=3,
	)
	return html_diff


def _should_generate_html_diff(old_str: str, new_str: str) -> bool:
	"""Determine if HTML diff should be generated for the given values."""
	return (
		old_str and new_str and ("\n" in old_str or "\n" in new_str or len(old_str) > 80 or len(new_str) > 80)
	)


def _as_string(value: str | None) -> str:
	"""Convert the given value to a string."""
	return cstr(value) if value is not None else ""


# //// Neoffice — added block (neoffice-maintenance#2296, security audit question 15). A Version's data holds the old
# //// and the new value of every field a save changed, whatever the reader may read of the document. Upstream
# //// version-15 hands it whole to the history of every form (`get_versions`, in the docinfo), to the Version form
# //// and to any list of Version: a technician who may not read the amount of a project phase (permlevel 1) read
# //// the old and the new amount in its history. What a reader is given of a Version now keeps only the fields of
# //// the permlevels they may read of the recorded doctype (desk's rule, `readable_permlevels`: a share counts as
# //// level 0, rows follow the parent's permission rows, a table field gates its whole table), for every doctype;
# //// who changed it, when and through which import stays. Nothing stored changes: Administrator still reads all.
# //// Upstream develop filters `get_versions` the same way (eb3c578130, 82e99a101e): at the v16 merge take its
# //// `get_versions` and drop our call there; keep the Version form and list filters below until upstream has them.
import re

VERSION_KEYS_OF_EVERY_DOCUMENT = ("name", "docstatus")
# a list of Version selects the doctype each row records under this name, read and dropped by filter_version_rows
_RECORDED_DOCTYPE_COLUMN = "neoffice_version_ref_doctype"
_PLAIN_VERSION_COLUMN = re.compile(
	r"^\s*(?:`?tabVersion`?\s*\.\s*)?`?(?P<column>[A-Za-z_][A-Za-z0-9_]*)`?"
	r"(?:\s+as\s+[`'\"]?(?P<alias>[A-Za-z0-9_]+)[`'\"]?)?\s*$",
	re.IGNORECASE,
)


def readable_version_fields(ref_doctype: str | None) -> tuple[set[str], dict[str, set[str]]] | None:
	"""The fields of `ref_doctype`, and per table field the fields of its rows, that the session user may read;
	None when nothing is hidden from them (Administrator, or a doctype without permission rows)."""
	if frappe.session.user == "Administrator":
		return None
	from frappe.desk.form.activity import readable_permlevels

	try:
		meta = frappe.get_meta(ref_doctype) if ref_doctype else None
	except frappe.DoesNotExistError:
		meta = None
	if not meta:
		# a deleted doctype, or a row whose doctype is unknown: none of its fields can be vouched for
		return set(VERSION_KEYS_OF_EVERY_DOCUMENT), {}

	permitted = readable_permlevels(meta)
	if permitted is None:
		return None

	parent_fields = {df.fieldname for df in meta.fields if df.permlevel in permitted}
	parent_fields.update(VERSION_KEYS_OF_EVERY_DOCUMENT)
	child_fields = {
		table_field.fieldname: {
			df.fieldname
			for df in (frappe.get_meta(table_field.options).fields or [])
			if df.permlevel in permitted
		}
		for table_field in meta.get_table_fields()
		if table_field.fieldname in parent_fields
	}
	return parent_fields, child_fields


def readable_version_json(data: str | None, ref_doctype: str | None) -> str | None:
	"""The data of a Version of `ref_doctype` as the session user may read it, as JSON again."""
	return filter_version_json(data, readable_version_fields(ref_doctype))


def filter_version_json(
	data: str | None, readable: tuple[set[str], dict[str, set[str]]] | None
) -> str | None:
	"""`data` with only the changes of the `readable` fields (readable_version_fields); None when it is not the data
	of a Version at all, unless nothing is hidden (`readable` is None), then `data` itself."""
	if readable is None:
		return data
	try:
		parsed = json.loads(data or "{}")
	except (TypeError, ValueError):
		return None
	if not isinstance(parsed, dict):
		return None

	parent_fields, child_fields = readable

	def is_entry(entry, size: int) -> bool:
		return isinstance(entry, list | tuple) and len(entry) >= size and isinstance(entry[0], str)

	if "changed" in parsed:
		parsed["changed"] = [
			entry for entry in parsed["changed"] or [] if is_entry(entry, 1) and entry[0] in parent_fields
		]
	for section in ("added", "removed"):
		if section in parsed:
			parsed[section] = [
				[entry[0], {key: value for key, value in entry[1].items() if key in child_fields[entry[0]]}]
				for entry in parsed[section] or []
				if is_entry(entry, 2) and entry[0] in child_fields and isinstance(entry[1], dict)
			]
	if "row_changed" in parsed:
		rows = []
		for entry in parsed["row_changed"] or []:
			if not (is_entry(entry, 4) and entry[0] in child_fields):
				continue
			changes = [
				change
				for change in entry[3] or []
				if is_entry(change, 1) and change[0] in child_fields[entry[0]]
			]
			if changes:
				rows.append([entry[0], entry[1], entry[2], changes])
		parsed["row_changed"] = rows

	return frappe.as_json(parsed, indent=None, separators=(",", ":"))


def select_version_data_readably(query) -> None:
	"""DatabaseQuery.prepare_args, for a list of Version read with permissions by anyone but Administrator.

	The data of each row is filtered once read (filter_version_rows), which needs the plain column and the doctype
	the row records: so the data is selected only as a plain column, with that doctype next to it. An expression of
	it, which the filter cannot read, is not selected; nor is it in a grouped list, whose row may mix Versions."""
	query.neoffice_version_data = None
	if query.flags.ignore_permissions or frappe.session.user == "Administrator":
		return
	from frappe.desk.reportview import extract_fieldnames

	def plain_data_column(field: str):
		plain = _PLAIN_VERSION_COLUMN.match(field)
		return plain if plain and plain.group("column").lower() == "data" else None

	# backwards: in a list of dicts DatabaseQuery.remove_field takes the field out
	for index in reversed(range(len(query.fields))):
		field = query.fields[index]
		if not field:
			continue
		reads_data = _RECORDED_DOCTYPE_COLUMN in field.lower() or any(
			column.rsplit(".", 1)[-1].strip("`").lower() == "data" for column in extract_fieldnames(field)
		)
		if reads_data and (query.group_by or not plain_data_column(field)):
			query.remove_field(index)

	slots = []
	for index, field in enumerate(query.fields):
		if field and (plain := plain_data_column(field)):
			slots.append((index, plain.group("alias") or "data"))
	if not slots:
		return

	query.fields.append(f"`tabVersion`.`ref_doctype` as {_RECORDED_DOCTYPE_COLUMN}")
	query.neoffice_version_data = (slots, len(query.fields) - 1)


def filter_version_rows(query, rows):
	"""DatabaseQuery.execute: the data of each Version of the list as the session user may read it, and the doctype
	select_version_data_readably added taken out again."""
	selected = getattr(query, "neoffice_version_data", None)
	if not selected or not isinstance(rows, list | tuple):
		return rows

	slots, recorded_doctype_index = selected
	readable_by_doctype = {}

	def readable(data, ref_doctype):
		if ref_doctype not in readable_by_doctype:
			readable_by_doctype[ref_doctype] = readable_version_fields(ref_doctype)
		return filter_version_json(data, readable_by_doctype[ref_doctype])

	out = []
	for row in rows:
		if isinstance(row, dict):
			ref_doctype = row.pop(_RECORDED_DOCTYPE_COLUMN, None)
			for _index, key in slots:
				if key in row:
					row[key] = readable(row[key], ref_doctype)
			out.append(row)
		else:
			values = list(row)
			ref_doctype = values.pop(recorded_doctype_index)
			for index, _key in slots:
				values[index] = readable(values[index], ref_doctype)
			out.append(tuple(values) if isinstance(row, tuple) else values)
	return out
