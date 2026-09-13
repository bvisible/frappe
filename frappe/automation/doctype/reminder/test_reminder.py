# Copyright (c) 2023, Frappe Technologies and Contributors
# See license.txt

import frappe
from frappe.automation.doctype.reminder.reminder import create_new_reminder, send_reminders
from frappe.desk.doctype.notification_log.notification_log import get_notification_logs
from frappe.tests.utils import FrappeTestCase
from frappe.utils import add_to_date, now_datetime


class TestReminder(FrappeTestCase):
	def test_reminder(self):
		description = "TEST_REMINDER"

		# //// Neoffice — a reminder fires once it is DUE, never before (send_reminders, d17232d736).
		# //// Upstream's test set it one minute ahead and expected it at once: that is the head start
		# //// we removed. Ahead, it must wait; once due, it is sent (neoffice-maintenance#392).
		reminder = create_new_reminder(
			remind_at=add_to_date(now_datetime(), minutes=1, as_datetime=True, as_string=True),
			description=description,
		)

		send_reminders()
		self.assertNotIn(description, self.subjects(), msg="a reminder one minute ahead was sent early")

		# //// Neoffice — Reminder.validate refuses a past time: the minute goes by behind it
		frappe.db.set_value("Reminder", reminder.name, "remind_at", add_to_date(now_datetime(), minutes=-1))
		send_reminders()

		notifications = get_notification_logs()["notification_logs"]
		self.assertIn(
			description,
			[n.subject for n in notifications],
			msg=f"Failed to find reminder notification \n{notifications}",
		)

	def subjects(self):  # //// Neoffice — the subjects of the notifications the test user can see
		return [n.subject for n in get_notification_logs()["notification_logs"]]
