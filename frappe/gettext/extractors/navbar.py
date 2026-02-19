import importlib
from pathlib import Path

from frappe.utils import get_bench_path


def extract(fileobj, *args, **kwargs):
	"""Extract standard navbar and help items from a python file.

	:param fileobj: file-like object to extract messages from. Should be a
	    python file containing two global variables `standard_navbar_items` and
	    `standard_help_items` which are lists of dicts.
	"""
	module = get_module(fileobj.name)

	if hasattr(module, "standard_navbar_items"):
		standard_navbar_items = module.standard_navbar_items
		for nav_item in standard_navbar_items:
			if label := nav_item.get("item_label"):
				item_type = nav_item.get("item_type")
				yield (
					None,
					"_",
					label,
					[
						"Label of a standard navbar item",
						f"Type: {item_type}",
					],
				)

	if hasattr(module, "standard_help_items"):
		standard_help_items = module.standard_help_items
		for help_item in standard_help_items:
			if label := help_item.get("item_label"):
				item_type = help_item.get("item_type")
				yield (
					None,
					"_",
					label,
					[
						"Label of a standard help item",
						f"Type: {item_type}",
					],
				)


def get_module(path):
	_path = Path(path)
	bench_path = Path(get_bench_path())
	try:
		rel_path = _path.relative_to(bench_path)
	except ValueError:
		# Handle symlinked apps: resolve through bench apps/ symlinks
		apps_dir = bench_path / "apps"
		for app_dir in apps_dir.iterdir():
			if app_dir.is_symlink():
				real_app = app_dir.resolve()
				real_app_str = str(real_app).rstrip("/") + "/"
				if str(_path).startswith(real_app_str):
					suffix = _path.relative_to(real_app)
					rel_path = Path("apps") / app_dir.name / suffix
					break
		else:
			raise
	import_path = ".".join(rel_path.parts[2:]).rstrip(".py")
	return importlib.import_module(import_path)
