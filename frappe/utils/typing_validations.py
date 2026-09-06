from collections.abc import Callable
from functools import lru_cache, wraps
from inspect import _empty, isclass, signature
from types import EllipsisType
# //// Neoffice — added get_type_hints to resolve PEP 563 deferred (stringified) annotations before the ForwardRef/str skip below, so whitelisted endpoints on modules using `from __future__ import annotations` get argument checking again (48b6d2dde2 "fix(typing): deferred annotations no longer skip the whole argument check")
from typing import ForwardRef, TypeVar, Union, get_type_hints

from pydantic import ConfigDict, PydanticUserError
from pydantic import TypeAdapter as PyTypeAdapter

from frappe.exceptions import FrappeTypeError

SLACK_DICT = {
	bool: (int, bool, float),
}
T = TypeVar("T")


FrappePydanticConfig = ConfigDict(arbitrary_types_allowed=True)


def validate_argument_types(func: Callable, apply_condition: Callable = lambda: True):
	@wraps(func)
	def wrapper(*args, **kwargs):
		"""Validate argument types of whitelisted functions.

		:param args: Function arguments.
		:param kwargs: Function keyword arguments."""

		if apply_condition():
			args, kwargs = transform_parameter_types(func, args, kwargs)

		return func(*args, **kwargs)

	return wrapper


def qualified_name(obj) -> str:
	"""
	Return the qualified name (e.g. package.module.Type) for the given object.

	Builtins and types from the :mod:typing package get special treatment by having the module
	name stripped from the generated name.

	"""
	discovered_type = obj if isclass(obj) else type(obj)
	module, qualname = discovered_type.__module__, discovered_type.__qualname__

	if module in {"typing", "types"}:
		return obj
	elif module in {"builtins"}:
		return qualname
	else:
		return f"{module}.{qualname}"


def raise_type_error(
	arg_name: str, arg_type: type, arg_value: object, current_exception: Exception | None = None
):
	"""
	Raise a TypeError with a message that includes the name of the argument, the expected type
	and the actual type of the value passed.

	"""
	raise FrappeTypeError(
		f"Argument '{arg_name}' should be of type '{qualified_name(arg_type)}' but got "
		f"'{qualified_name(arg_value)}' instead."
	) from current_exception


@lru_cache(maxsize=2048)
def TypeAdapter(type_):
	try:
		return PyTypeAdapter(type_, config=FrappePydanticConfig)
	except PydanticUserError as e:
		# Cannot set config for types BaseModel, TypedDict and dataclass
		if e.code == "type-adapter-config-unused":
			return PyTypeAdapter(type_)

		raise e


# //// Neoffice — resolve string annotations before the ForwardRef|str skip below.
# //// A module carrying `from __future__ import annotations` (PEP 563) stores EVERY
# //// annotation as a string, and that skip then dropped the WHOLE argument check —
# //// silently. 604 whitelisted endpoints of the fleet were in that case (431 in
# //// suite alone, plus payments, nora, letters, wiki, pos_next…): not one of their
# //// arguments was checked at the request boundary (tracker #244).
# //// Upstream carries the same skip and never noticed, because upstream's own
# //// whitelisted modules do not use PEP 563 — so this hunk is INERT wherever
# //// annotations are already objects, which is all of upstream's code. Re-apply it
# //// at the v16 merge; it becomes a true no-op only once Python resolves
# //// annotations lazily (PEP 649, 3.14+) or the modules drop the graft.
# //// Enforcement is deliberately staged — see _enforce_deferred_annotations().
def _resolved_annotations(func: Callable, annotations: dict) -> tuple[dict, bool]:
	"""Resolve PEP 563 annotations. Returns (annotations, whether they were strings).

	Best effort: an annotation naming something unresolvable stays a string and is
	skipped exactly as before, so nothing can start failing that did not already.
	"""
	if not any(isinstance(v, str) for v in annotations.values()):
		return annotations, False

	cached = getattr(func, "__neoffice_type_hints__", None)
	if cached is None:
		try:
			cached = get_type_hints(func)
		except Exception:  # unresolvable hint: keep the old behaviour
			cached = annotations
		try:
			func.__neoffice_type_hints__ = cached
		except Exception:  # builtins and slots cannot carry an attribute; fine
			pass

	return cached, cached is not annotations


def _enforce_deferred_annotations() -> bool:
	"""Refuse a wrong value on a PEP 563 endpoint, or only report it?

	Strict in tests and CI, so a wrong type is caught while the code is being
	written. Report-only on live traffic until `strict_deferred_type_checks` is
	set in site_config: those 604 endpoints have never been validated, and making
	them all strict in a single deploy would surface as brand-new errors on
	production requests that work today. Read the reports, fix what they name,
	then throw the switch.
	"""
	import frappe

	# flags is set by frappe.init(); guard it so a bare import can never raise here
	if getattr(frappe.local, "flags", None) and frappe.local.flags.in_test:
		return True
	return bool(frappe.conf.get("strict_deferred_type_checks"))


def _report_deferred_type_error(func: Callable, arg_name: str, arg_type, arg_value) -> None:
	"""Name the endpoint that would have been refused. Once a day, per argument.

	This runs on live traffic, so it logs the TYPE received and never the value:
	an argument can hold client data or a secret.
	"""
	import frappe

	endpoint = f"{getattr(func, '__module__', '?')}.{getattr(func, '__qualname__', '?')}"
	key = f"deferred_type_error::{endpoint}::{arg_name}"
	try:
		if frappe.cache().get_value(key):
			return
		frappe.cache().set_value(key, 1, expires_in_sec=86400)
		frappe.log_error(
			f"Unvalidated type on {arg_name} of {getattr(func, '__qualname__', '?')}"[:140],
			f"Endpoint: {endpoint}\n"
			f"Argument: {arg_name}\n"
			f"Declared: {qualified_name(arg_type)}\n"
			f"Received: {qualified_name(arg_value)}\n\n"
			"This module defers its annotations (PEP 563), so this argument was never "
			"type-checked. The call was let through unchanged. Fix the caller or the "
			"annotation, then set strict_deferred_type_checks in site_config to refuse it.",
		)
	except Exception:  # reporting must never break the request
		pass


def transform_parameter_types(func: Callable, args: tuple, kwargs: dict):
	"""
	Validate the types of the arguments passed to a function with the type annotations
	defined on the function.

	"""
	if not (args or kwargs) or not func.__annotations__:
		return args, kwargs

	from pydantic import ValidationError as PyValidationError

	annotations, deferred = _resolved_annotations(func, func.__annotations__)
	# //// Neoffice — see _enforce_deferred_annotations(): report-only on live traffic
	enforce = not deferred or _enforce_deferred_annotations()
	new_args, new_kwargs = list(args), kwargs

	# generate kwargs dict from args
	arg_names = func.__code__.co_varnames[: func.__code__.co_argcount]

	if not args:
		prepared_args = kwargs

	elif kwargs:
		arg_values = args or func.__defaults__ or []
		prepared_args = dict(zip(arg_names, arg_values, strict=False))
		prepared_args.update(kwargs)

	else:
		prepared_args = dict(zip(arg_names, args, strict=False))

	# check if type hints dont match the default values
	func_signature = signature(func)
	func_params = dict(func_signature.parameters)

	# check if the argument types are correct
	for current_arg, current_arg_type in annotations.items():
		if current_arg not in prepared_args:
			continue

		current_arg_value = prepared_args[current_arg]

		# if the type is a ForwardRef or str, ignore it
		if isinstance(current_arg_type, ForwardRef | str):
			continue
		elif any(isinstance(x, ForwardRef | str) for x in getattr(current_arg_type, "__args__", [])):
			continue

		# allow slack for Frappe types
		if current_arg_type in SLACK_DICT:
			current_arg_type = SLACK_DICT[current_arg_type]

		param_def = func_params.get(current_arg)

		# add default value's type in acceptable types
		if param_def.default is not _empty:
			if isinstance(current_arg_type, tuple):
				if type(param_def.default) not in current_arg_type:
					current_arg_type += (type(param_def.default),)
				current_arg_type = Union[current_arg_type]  # noqa: UP007

			elif param_def.default != current_arg_type:
				current_arg_type = Union[current_arg_type, type(param_def.default)]  # noqa: UP007
		elif isinstance(current_arg_type, tuple):
			current_arg_type = Union[current_arg_type]  # noqa: UP007

		# validate the type set using pydantic - raise a TypeError if Validation is raised or Ellipsis is returned
		try:
			current_arg_value_after = TypeAdapter(current_arg_type).validate_python(current_arg_value)
		except (TypeError, PyValidationError) as e:
			# //// Neoffice — report instead of refusing while enforcement is staged
			if not enforce:
				_report_deferred_type_error(func, current_arg, current_arg_type, current_arg_value)
				continue
			raise_type_error(current_arg, current_arg_type, current_arg_value, current_exception=e)

		if isinstance(current_arg_value_after, EllipsisType):
			# //// Neoffice — idem
			if not enforce:
				_report_deferred_type_error(func, current_arg, current_arg_type, current_arg_value)
				continue
			raise_type_error(current_arg, current_arg_type, current_arg_value)

		# //// Neoffice — a report-only pass must not cast either: production keeps the
		# //// exact behaviour it had before the annotations were resolved.
		if not enforce:
			continue

		# update the args and kwargs with possibly casted value
		if current_arg in kwargs:
			new_kwargs[current_arg] = current_arg_value_after
		else:
			new_args[arg_names.index(current_arg)] = current_arg_value_after

	return new_args, new_kwargs
