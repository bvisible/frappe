# Neoffice fork markers — divergences that cannot carry a `//// Neoffice` comment

Every Neoffice change to this fork is marked in place with `//// Neoffice — <reason>`, so that
`grep -rn "////"` maps our whole divergence from upstream before a merge.

A few divergences cannot carry a comment: whitespace at a place where no comment can be written
(a dropped final newline), binary files, and pure-data files with no comment syntax (`.json`).
They are recorded here instead, so the map stays complete.

Baseline for everything below: `git diff v15.89.0 version-15` (the merge base of this fork with
upstream is exactly the tag `v15.89.0`).

## Lot F1 — frappe/public/js

### Final newline dropped (whitespace only, no code difference)

Upstream ends these files with a newline; ours does not. It shows up as a one-line
`-`/`+` pair at the end of the diff plus `\ No newline at end of file`. Nothing else differs on
those lines — take upstream's version at the merge.

| File | Last line | Introduced by |
|---|---|---|
| `frappe/public/js/frappe/file_uploader/FileUploader.vue` | `</style>` | bd41f1e7a5 (2025-02-26 "Update neov2") |
| `frappe/public/js/frappe/form/form.js` | `frappe.validated = 0;` | bd41f1e7a5 (2025-02-26 "Update neov2") |
| `frappe/public/js/frappe/model/indicator.js` | `};` | bd41f1e7a5 (2025-02-26 "Update neov2") |

Each of the three files carries a `//// Neoffice` marker elsewhere that points here.

`frappe/public/js/frappe/ui/apps_switcher.html` has no final newline either, but it is a file we
added (no upstream equivalent), so it is not a divergence — see its header marker.

## Lot F2 — frappe/public/js

### Generated bundle: a marker there would be destroyed at the next build

| File | Why it carries no marker |
|---|---|
| `frappe/public/js/lib/neocockpit.global.js` | Minified build artefact, overwritten wholesale by `neoffice-devops/scripts/cockpit-release.sh` (`cp "$SRC/dist/neocockpit.global.js" frappe/public/js/lib/neocockpit.global.js`). Any comment added to it disappears at the next re-vendor, so it would be a marker that lies. |

What it is: the vendored NeoCockpit React bundle — the desk chrome (single sidebar rail absorbing
the header: search, NORA, notifications, user menu). Source repo `bvisible/frappe-sidebar-react`,
built with tsup, copied here and served raw from `/assets/frappe/js/lib/neocockpit.global.js` with
a `?v=N` cache-bust bumped in `frappe/hooks.py` by the same script. Added file, **no upstream
equivalent in v15.120 or develop** — it cannot conflict at the merge, it can only be lost by
someone deleting it.

Introduced by `d0268ef91a` (2026-06-10, "feat(cockpit): NeoCockpit is the default desk chrome —
single menu, no navbar"), then re-vendored 78 times up to `6ed439f1f6` (2026-09-02, `?v=79`);
every one of those commits is a `feat/chore(cockpit): re-vendor bundle v=N (frappe-sidebar-react
<sha>)` touching only this file and `hooks.py`. Consumers of the bundle and the release procedure:
see `neoffice-devops/CLAUDE.md` § "NeoCockpit — carte des consommateurs + release". Never edit it
by hand: change `frappe-sidebar-react` and run `cockpit-release.sh`.

### Final newline dropped (marked in place, listed here for completeness)

Same whitespace-only divergence as lot F1. Both files DO carry a `//// Neoffice` marker above
their last line, so `grep -rn "////"` finds them; take upstream's newline at the merge.

| File | Last line | Introduced by |
|---|---|---|
| `frappe/public/js/frappe/views/reports/report_view.js` | `};` | 4e23539603 (2024-09-23 "last updates") |
| `frappe/public/js/frappe/views/translation_manager.js` | `};` | bd41f1e7a5 (2025-02-26 "Update neov2") |

## Lot F3 — frappe (reste)

Covers `frappe/utils`, `frappe/www`, `frappe/core/doctype`, `frappe/templates`, `frappe/model`,
`frappe/desk`, `frappe/email`, `frappe/printing`, `frappe/integrations/doctype`, `frappe/commands`,
`frappe/gettext`, `frappe/types`, `frappe/public/css`, `frappe/handler.py`, `frappe/translate.py`,
`frappe/build.py`. The 50 source files of the lot carry their markers in place (118 of them); what
follows is what could not.

### DocType JSON — no comment syntax, so recorded here

These are the companions of the `.py` files marked in this lot (and their immediate neighbours in
the same doctype folders). A JSON file cannot carry a `//// Neoffice` line, so the divergence lives
here. **Every "field added" row below is a candidate for conversion to a Custom Field / Property
Setter** rather than a fork edit — that is the point of chantier #138: a Custom Field survives an
upstream merge, an edited shipped JSON does not.

| File | Divergence vs `v15.89.0` | Introduced by | Note |
|---|---|---|---|
| `frappe/core/doctype/user/user.json` | **field added**: `report_settings` (+ `field_order`) | bd41f1e7a5 (2025-02-26 "Update neov2", empty message), 0160892c88 (2025-12-14) | read/written by `save_user_report_settings` & co. at the end of `frappe/desk/reportview.py` (marked). Custom Field candidate. |
| `frappe/core/doctype/system_settings/system_settings.json` | **field added**: `report_settings` (+ `field_order`) | bd41f1e7a5, 0160892c88 | the site-wide default for the same feature (`save_global_report_settings`). Custom Field candidate. |
| `frappe/desk/doctype/workspace_link/workspace_link.json` | **field added**: `url`; `link_type` options `DocType\nPage\nReport` → `+ URL`; `link_to` gains `depends_on: eval:doc.link_type != "URL"` and a narrowed `mandatory_depends_on` | 59696f7bbd (2026-08-27 "feat(workspace): un lien de carte peut etre une URL, comme un raccourci") | Custom Field + Property Setter candidate. |
| `frappe/email/doctype/email_queue_recipient/email_queue_recipient.json` | `status` options `\nNot Sent\nSent` → `+ Error` | 4e23539603 (2024-09-23 "last updates", empty message) | mirrors the `DF.Literal` in `email_queue_recipient.py`, which IS marked. Set by the placeholder blocker in `email_queue.py`. Property Setter candidate. |
| `frappe/printing/doctype/print_format/print_format.json` | `pdf_generator` options `wkhtmltopdf` → `+ chrome` | c64ffb849d, cherry-picked from frappe develop `964dd6c034` | part of the Chrome PDF backport. Absent from upstream v15.120 → **disappears at a v16 merge**; keep our side at a v15 merge. |
| `frappe/printing/doctype/print_settings/print_settings.json` | **field added**: `use_chrome_for_standard_format` (+ `field_order`) | 40b4e486bb, cherry-picked from frappe develop `8649c18125` | same backport, same fate. |
| `frappe/custom/doctype/custom_field/custom_field.json` | `is_system_generated`: `in_list_view` → `1` | 4304bba972 (2026-03-18 "feat(columns): apply child table column config to DocType JSON") | pure display property → Property Setter candidate, not a fork edit. |
| `frappe/desk/doctype/workspace/workspace.json` | `title`, `parent_page`, `sequence_id`: `in_list_view` → `1` | 4304bba972 | idem. |
| `frappe/desk/doctype/todo/todo.json` | **none — reverted to upstream on 2026-09-04** (was: `status` options `Open\nClosed\nCancelled` → `test\nClosed\nCancelled\nOpen`) | 4e23539603 (2024-09-23 "last updates", empty message), reverted for #205 | The accident is undone — the list is upstream's again, `Open` first. The fleet *did* store `test` (one ToDo on Osiris), so the revert ships with `frappe/patches/v15_0/neoffice_todo_status_test_to_open.py`, which carries those rows to `Open`. Nothing left to reconcile at the merge. |

### Final newline dropped (marked in place, listed here for completeness)

Same whitespace-only divergence as lots F1 and F2: upstream ends the file with a newline, ours does
not, which shows as a `-`/`+` pair plus `\ No newline at end of file`. Every one of these files
carries a `//// Neoffice` marker that says so — take upstream's newline at the merge.

| File | Last line | Introduced by |
|---|---|---|
| `frappe/core/doctype/communication/email.py` | `)` | bd41f1e7a5 (2025-02-26 "Update neov2") |
| `frappe/desk/reportview.py` | `frappe.log_error(f"Failed to delete file …")` | bd41f1e7a5 |
| `frappe/integrations/doctype/s3_backup_settings/s3_backup_settings.py` | `print("Error uploading: %s" % (e))` | 68d7f3a760 (2025-07-03 "Update s3_backup_settings.py") |
| `frappe/templates/print_format/print_format.css` | `}` | bd41f1e7a5 |
| `frappe/templates/print_formats/pdf_header_footer.html` | `</html>` | bd41f1e7a5 |
| `frappe/templates/styles/standard.css` | `}` | bd41f1e7a5 |
| `frappe/translate.py` | `load_lang = get_translations_from_apps` | bd41f1e7a5 |
| `frappe/www/404.html` | `{% endblock %}` | bd41f1e7a5 |
| `frappe/www/login.html` | `{% block sidebar %}{% endblock %}` | 5d5ae8cec8 (2025-03-06 "Remove old login page") |

For four of them — `communication/email.py`, `translate.py`, `templates/print_format/print_format.css`
and `www/login.html` — the missing newline is the **only** difference from upstream v15: restoring it
takes those files back to zero divergence.

`frappe/templates/print_formats/chrome_pdf_header_footer.html` has no final newline either, but it is
a file we added (byte-identical to frappe develop at `4f365bfbf5`), so it is not a divergence.

### Defects found while marking lot F3 (marked in place, repeated here for the merge)

| Where | What |
|---|---|
| `frappe/utils/image.py`, `optimize_image()` | 🔴 Upstream added `exif = image.getexif()` (`d0eabcd4f6`, 2024-09-02, backport of #27341). Our merge `0b9b53c7ea` kept upstream's `exif=exif` argument in the `image.save()` call but **lost the assignment**. Every call raises `NameError`, the function's broad `except Exception` swallows it into a "Failed to optimize image" msgprint and returns the ORIGINAL bytes — image optimisation is silently off fleet-wide. Restoring the upstream line is the fix. |
| `frappe/utils/print_utils.py`, `attach_print()` | 🔴 Dead **and** broken: it came with the develop backport, but `frappe/__init__.py` still defines `attach_print` and that is the one every caller uses, so this copy runs from nowhere — and it calls `cint()` / `cstr()`, which this module never imports. |
| `frappe/utils/pdf.py`, `prepare_options()` | 🔴 The two margin defaults are **crossed**: the `if not options.get("margin-right")` branch sets `margin-left`, and the `margin-left` branch sets `margin-right`. Harmless only while both are unset. |
| `frappe/types/filter.py` | 🔴 Our copy is frappe develop's, which needs **Python 3.12+** (PEP 695). Upstream version-15 shipped its own 3.10-compatible backport in v15.103.3 — take upstream's file at the merge. |
| `frappe/utils/user.py`, `load_user()` | 🔴 Selects `view_interface`, which is **not** a field of the shipped User doctype — it must be a Custom Field from a Neoffice app. On any site without it, loading the boot info raises. |
| `frappe/utils/__init__.py` | 🔴 Module-level `import frappe` (upstream deliberately avoids it — circular import) and an unused `from redis.exceptions import ConnectionError` that **shadows the builtin** for this module and for every `from frappe.utils import *` consumer. |
| `frappe/utils/file_manager.py`, `is_safe_path()` | 🔴 A security guard widened with a hard-coded `/mnt/neoffice` prefix, by a commit with no message. |
