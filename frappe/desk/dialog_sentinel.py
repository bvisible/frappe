# //// Neoffice — added file (no upstream equivalent). The sentinel of the windows (design system §6.18, 17.11.2026): the
# //// desk (frappe/public/js/frappe/ui/dialog_sentinel.js) measures every window a person opens against the model of
# //// dialog_model.js and sends what it saw here; the daily job counts the departures of the day before, by rule and by
# //// window. On a site whose site_config holds `neo_dialog_sentinel: 1` only (our test instance): elsewhere the boot
# //// says nothing, the desk sends nothing, and this file writes nothing. What is kept: the window's title, the route,
# //// where it was opened from, its measures and the desk account that saw it, under sites/<site>/private/dialog_sentinel/,
# //// 30 days.
import json
import os
from collections import defaultdict

import frappe
from frappe.rate_limiter import rate_limit
from frappe.utils import add_days, cint, getdate, now_datetime, today

KEEP_DAYS = 30
MAX_LINE = 6000
KEYS = (
	"kind",
	"title",
	"eyebrow",
	"route",
	"opener",
	"family",
	"inputs",
	"size",
	"width",
	"air",
	"title_font",
	"label",
	"keys",
	"model",
	"centred",
	"footer_lines",
	"viewport",
	"departures",
	"cls",
	"id",
)
RULES = {
	"not-model": "not drawn by the window model (dialog_model.js)",
	"width": "a width that is none of the four sizes (440 / 560 / 800 / 1 140)",
	"size-family": "a size that does not fit what one does in it (a question in S, a form in L)",
	"air": "not 32 px of air above and beside the title",
	"eyebrow": "an entry, a form or a wizard without its eyebrow",
	"verb": "an action named « OK », « Valider », « Submit », « Oui » where a verb belongs",
	"keys": "the ink action does not show its key",
	"title": "the title is not Forum at --ws-h2",
	"centre": "the window is not centred",
	"footer": "the footer does not hold on one line",
	"hand-made-veil": "a veil drawn by hand over the whole screen",
}


def enabled() -> bool:
	return bool(cint(frappe.conf.get("neo_dialog_sentinel")))


def get_boot_flag() -> int:
	return 1 if enabled() else 0


def _folder() -> str:
	path = frappe.get_site_path("private", "dialog_sentinel")
	os.makedirs(path, exist_ok=True)
	return path


@frappe.whitelist(methods=["POST"])
@rate_limit(limit=600, seconds=60 * 60)
def record(sighting: str) -> None:
	"""Keep what the desk saw of one window (or of a veil drawn by hand). A desk account only, on a site where it is on."""
	if not enabled():
		return
	if frappe.get_cached_value("User", frappe.session.user, "user_type") != "System User":
		raise frappe.PermissionError
	try:
		data = json.loads(sighting) if isinstance(sighting, str) else None
	except ValueError:
		return
	if not isinstance(data, dict):
		return
	row = {k: data.get(k) for k in KEYS if k in data}
	row["departures"] = [d for d in (row.get("departures") or []) if isinstance(d, str) and d in RULES][:12]
	row["user"] = frappe.session.user
	row["at"] = now_datetime().isoformat(timespec="seconds")
	line = json.dumps(row, ensure_ascii=False, default=str)
	if len(line) > MAX_LINE:
		return
	with open(os.path.join(_folder(), f"{today()}.jsonl"), "a", encoding="utf-8") as f:
		f.write(line + "\n")


def build_report(day: str) -> dict:
	"""The departures of one day: per rule, how many distinct windows broke it, and the windows with their sightings."""
	path = os.path.join(_folder(), f"{day}.jsonl")
	windows: dict[str, dict] = {}
	if os.path.exists(path):
		with open(path, encoding="utf-8") as f:
			for line in f:
				try:
					row = json.loads(line)
				except ValueError:
					continue
				key = "|".join(str(row.get(k) or "") for k in ("kind", "title", "cls", "opener"))
				w = windows.setdefault(
					key,
					{
						k: row.get(k)
						for k in (
							"kind",
							"title",
							"eyebrow",
							"route",
							"opener",
							"family",
							"width",
							"label",
							"cls",
						)
					},
				)
				w["seen"] = w.get("seen", 0) + 1
				w.setdefault("users", set()).add(row.get("user"))
				w["departures"] = sorted(set(w.get("departures") or []) | set(row.get("departures") or []))
	by_rule: dict[str, list] = defaultdict(list)
	for w in windows.values():
		w["users"] = len(w["users"])
		for d in w["departures"]:
			by_rule[d].append(w)
	return {
		"day": day,
		"windows": len(windows),
		"clean": sum(1 for w in windows.values() if not w["departures"]),
		"rules": {r: {"says": RULES[r], "count": len(by_rule.get(r, []))} for r in RULES},
		"departures": {r: sorted(ws, key=lambda w: -w["seen"]) for r, ws in by_rule.items()},
	}


def _markdown(report: dict) -> str:
	lines = [
		f"# Windows of the desk, {report['day']}",
		"",
		f"{report['windows']} windows seen, {report['clean']} on the model.",
		"",
		"| Rule | Windows | What it says |",
		"|---|---:|---|",
	]
	for r, v in report["rules"].items():
		lines.append(f"| {r} | {v['count']} | {v['says']} |")
	for r, ws in report["departures"].items():
		lines += ["", f"## {r}", ""]
		for w in ws[:40]:
			lines.append(
				f"- « {w.get('title') or w.get('cls') or '?'} » ({w.get('family') or w.get('kind')}, "
				f"{w.get('width') or '-'} px, « {w.get('label') or ''} ») — {w.get('route') or ''} — "
				f"{w.get('opener') or ''} — seen {w['seen']}×"
			)
	return "\n".join(lines) + "\n"


def daily_report(day: str | None = None) -> None:
	"""Scheduled daily: the report of the day before, next to its sightings; sightings older than KEEP_DAYS go."""
	if not enabled():
		return
	day = day or str(add_days(today(), -1))
	report = build_report(day)
	folder = _folder()
	with open(os.path.join(folder, f"report-{day}.json"), "w", encoding="utf-8") as f:
		json.dump(report, f, ensure_ascii=False, indent=1, default=str)
	with open(os.path.join(folder, f"report-{day}.md"), "w", encoding="utf-8") as f:
		f.write(_markdown(report))
	limit = getdate(add_days(today(), -KEEP_DAYS))
	for name in os.listdir(folder):
		stem = name.split(".")[0].replace("report-", "")
		try:
			if getdate(stem) < limit:
				os.remove(os.path.join(folder, name))
		except Exception:
			continue


@frappe.whitelist()
def report(day: str | None = None) -> dict:
	"""The report of a day (today's so far by default), for System Manager: the bench's report reads it."""
	frappe.only_for("System Manager")
	return build_report(str(getdate(day)) if day else today())
