# //// Neoffice — added file (no upstream equivalent): pins keep_uids_at_or_above_floor, the guard
# //// against the last IMAP message coming back at every pull (frappe/email/receive.py).
import unittest

from frappe.email.receive import keep_uids_at_or_above_floor


class TestUidFloor(unittest.TestCase):
	def test_the_last_message_returned_again_is_dropped(self):
		# "UID 21:*" on a mailbox whose highest UID is 20: the server answers 20
		self.assertEqual(keep_uids_at_or_above_floor("UID 21:*", [b"20"]), [])

	def test_new_messages_are_kept(self):
		self.assertEqual(keep_uids_at_or_above_floor("UID 21:*", [b"21", b"22"]), [b"21", b"22"])

	def test_other_rules_are_left_alone(self):
		self.assertEqual(keep_uids_at_or_above_floor("UNSEEN", [b"3", b"7"]), [b"3", b"7"])
		self.assertEqual(keep_uids_at_or_above_floor("UID 1:101", [b"5"]), [b"5"])
