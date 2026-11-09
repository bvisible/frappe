# //// Neoffice — added file (no upstream equivalent). Guards neoffice-maintenance#1102: the words the
# //// link dialog shows (the tooltip of the cross of a Link field, the note after a save in the dialog)
# //// must exist in the French catalogue, or they show in English next to a French screen.
"""The French catalogue translates the words of the Link field's cross and of the form dialog's save note."""

import re
import unittest
from pathlib import Path

PO = Path(__file__).resolve().parents[1] / "locale" / "fr.po"


def msgstr_of(catalogue: str, msgid: str) -> str | None:
	"""The msgstr of a one-line msgid, None when the catalogue does not hold it."""
	match = re.search(rf'^msgid "{re.escape(msgid)}"\nmsgstr "(.*)"$', catalogue, re.M)
	return None if match is None else match.group(1)


class TestFormDialogCatalogue(unittest.TestCase):
	def setUp(self):
		self.catalogue = PO.read_text(encoding="utf-8")

	def test_the_cross_of_a_link_field_has_a_french_tooltip(self):
		"""controls/link.js titles the cross `Clear Link`; upstream's catalogue leaves it empty."""
		self.assertEqual(msgstr_of(self.catalogue, "Clear Link"), "Effacer le lien")

	def test_the_arrow_of_a_link_field_has_a_french_tooltip(self):
		self.assertEqual(msgstr_of(self.catalogue, "Open Link"), "Ouvrir le lien")

	def test_the_note_after_a_save_in_the_form_dialog_is_translated(self):
		"""form_dialog.js says only the linked record was saved when the document behind it is not."""
		msgstr = msgstr_of(self.catalogue, "Record saved. The current document is not saved yet.")
		self.assertTrue(msgstr, "the note of the form dialog has no French text")


if __name__ == "__main__":
	unittest.main()
