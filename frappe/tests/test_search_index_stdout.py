# //// Neoffice — added file (no upstream equivalent). The search index build must survive a
# //// stdout it cannot write to: under an RQ worker stdout is a pipe to supervisor, and writing
# //// to a closed one raises BrokenPipeError, which killed the whole build (tracker #170, #247).
"""Nothing in the sqlite search index build draws on a stdout that is not a terminal."""

import os
import sys
import unittest
from unittest.mock import patch

import frappe
from frappe.search.sqlite_search import SQLiteSearch, _stdout_is_a_terminal


class _Index(SQLiteSearch):
	"""SQLiteSearch is abstract; only the stdout guard is under test, so the one abstract
	method is stubbed and nothing here ever touches a database."""

	INDEX_NAME = "_neoffice_test.db"

	def get_search_filters(self):
		return {}


class _ClosedPipe:
	"""What supervisor hands an RQ worker once the other end is gone."""

	def isatty(self):
		return False

	def write(self, *a, **kw):
		raise BrokenPipeError(32, "Broken pipe")

	def flush(self):
		raise BrokenPipeError(32, "Broken pipe")


class _Terminal(_ClosedPipe):
	def isatty(self):
		return True

	def write(self, *a, **kw):
		self.written = True

	def flush(self):
		pass


class TestIndexBuildSurvivesAPipe(unittest.TestCase):
	def setUp(self):
		self.search = _Index.__new__(_Index)  # no DB, no schema: only the guard is under test
		self.search.warnings = []
		self.had_request = hasattr(frappe.local, "request")
		self.in_test = frappe.flags.in_test
		frappe.flags.in_test = False  # the guard we are testing is the one AFTER the in_test check
		self.ci = os.environ.pop("CI", None)

	def tearDown(self):
		frappe.flags.in_test = self.in_test
		if self.ci is not None:
			os.environ["CI"] = self.ci

	def test_a_closed_pipe_does_not_kill_the_progress_bar(self):
		with patch.object(sys, "stdout", _ClosedPipe()):
			self.search._update_progress("Setting up search tables", 0, 100)  # must not raise

	def test_a_terminal_still_gets_its_progress_bar(self):
		"""The other direction: the guard must not silence a real terminal."""
		terminal = _Terminal()
		with patch.object(sys, "stdout", terminal), patch(
			"frappe.search.sqlite_search.update_progress_bar"
		) as bar:
			self.search._update_progress("Indexing documents", 50, 100)
		bar.assert_called_once()

	def test_ci_still_gets_its_dots(self):
		"""CI has no terminal and does want the output: update_progress_bar has a CI branch."""
		os.environ["CI"] = "true"
		try:
			with patch.object(sys, "stdout", _ClosedPipe()), patch(
				"frappe.search.sqlite_search.update_progress_bar"
			) as bar:
				self.search._update_progress("Indexing documents", 50, 100)
			bar.assert_called_once()
		finally:
			os.environ.pop("CI", None)

	def test_a_web_request_never_draws(self):
		with patch.object(frappe.local, "request", object(), create=True), patch(
			"frappe.search.sqlite_search.update_progress_bar"
		) as bar:
			self.search._update_progress("Indexing documents", 50, 100)
		bar.assert_not_called()

	def test_the_warning_summary_takes_the_same_road(self):
		"""#170's fix and #247's now ask the same question in the same place."""
		with patch.object(sys, "stdout", _ClosedPipe()):
			self.assertFalse(_stdout_is_a_terminal())
			self.search._print_warning_summary()  # must not raise

	def test_a_stdout_that_cannot_answer_is_not_a_terminal(self):
		class Mute:
			def isatty(self):
				raise ValueError("I/O operation on closed file")

		with patch.object(sys, "stdout", Mute()):
			self.assertFalse(_stdout_is_a_terminal())
