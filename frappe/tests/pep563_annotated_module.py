# //// Neoffice — added file (no upstream equivalent). Fixture for tracker #244.
# //// A module with `from __future__ import annotations` (PEP 563) has EVERY
# //// annotation stored as a string, which used to make transform_parameter_types
# //// skip the whole argument check — silently, on every endpoint of such a module
# //// (all of suite's API). These functions reproduce that exact shape so the fix
# //// stays proven. Keep this file as-is when merging upstream: it tests our
# //// _resolved_annotations() hunk, which is inert once annotations are objects.
from __future__ import annotations

from frappe.utils.typing_validations import validate_argument_types


@validate_argument_types
def simple_types(a: int, b: float, c: bool):
	return a, b, c


@validate_argument_types
def optional_dict(a: str, b: dict[str, int] | None = None):
	return a, b


@validate_argument_types
def unresolvable_hint(a: DoesNotExistAnywhere):  # noqa: F821 — resolution must fail open
	return a
