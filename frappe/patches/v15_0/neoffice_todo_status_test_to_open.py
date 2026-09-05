# //// Neoffice — added file (no upstream equivalent).
# //// Upstream ships ToDo.status as `Open\nClosed\nCancelled`. Our fork carried
# //// `test\nClosed\nCancelled\nOpen` (todo.json, 4e23539603 "last updates"): a stray option
# //// that was never a real status, and a reorder that cost `Open` its place as the implicit
# //// first choice. Both are reverted in the same commit as this patch.
# //// What that leaves behind is data: the stray option was selectable, so documents were
# //// actually saved on it (one on Osiris). Once the option is gone those rows hold a value
# //// the Select no longer offers — the form shows an empty status, a standard filter cannot
# //// reach them, and the next save is refused. This patch carries them over to `Open`, the
# //// status they were always meant to have.
# //// Idempotent: it matches on the stray value, so a second run finds nothing and updates
# //// nothing. Drop it once no site can still be restored from a pre-2026-09 backup.

import frappe


def execute():
	"""Carry every ToDo left on the removed `test` status over to `Open`."""
	stray = frappe.db.count("ToDo", {"status": "test"})
	if not stray:
		return

	# One UPDATE for the whole batch, no ORM triggers. `modified` is deliberately left alone:
	# repairing a fork accident is not a user edit and must not reorder anyone's list view.
	frappe.db.set_value("ToDo", {"status": "test"}, "status", "Open", update_modified=False)
	frappe.db.commit()

	print(f"neoffice_todo_status_test_to_open: {stray} ToDo moved from `test` to `Open`")
