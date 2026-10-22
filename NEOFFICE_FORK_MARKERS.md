# Neoffice fork markers — divergences that cannot carry a `//// Neoffice` comment

Every Neoffice change to this fork is marked in place with `//// Neoffice — <reason>`, so that
`grep -rn "////"` maps our whole divergence from upstream before a merge.

A few divergences cannot carry a comment: whitespace at a place where no comment can be written
(a dropped final newline), binary files, and pure-data files with no comment syntax (`.json`).
They are recorded here instead, so the map stays complete.

Baseline for everything below: `git diff v15.124.0 version-15`. The fork was based on `v15.89.0`
until the merge of upstream `v15.123.1` (2026-10-20), then `v15.124.0` (2026-10-22); the rows below
were written against `v15.89.0` and re-checked at the first merge (lot M at the end lists what the
merge changed here). Since the merge,
`python3 neoffice-ci/scripts/fork_markers.py check --base v15.124.0 --head version-15
--upstream-base v15.124.0` answers 0 unmarked hunks.

## Lot F1 — frappe/public/js

### Final newline dropped — restored at the v15.123.1 merge

`FileUploader.vue`, `form/form.js` and `model/indicator.js` had lost the final newline upstream ends
them with (bd41f1e7a5, 2025-02-26 "Update neov2"). It was put back at the merge, with upstream's
import order in `FileUploader.vue`; nothing is left to record.

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

### Vendored third-party library: added files, no upstream equivalent

`frappe/public/js/lib/dotlottie-player/` — `@dotlottie/player-component@2.7.12`, the web component
that renders the `.lottie` animations our fork added (the 404 illustration in `www/404.html`, the
desk freeze overlay in `public/js/frappe/dom.js`, the processing overlay in `neoffice_theme`).

Until 2026-09-04 the player was not vendored at all: `www/404.html` and `www/app.html` each carried
a `<script type="module">` pointing at the publisher's CDN, which meant a third-party request on
every desk page load and on a public error page, and an install that could not render offline or
behind a restrictive CSP. #205 moved the bytes to our own origin, unchanged.

What is vendored is the publisher's **code-split ESM build, mirrored whole**, so the browser fetches
exactly what it fetched before — ~124 KB eagerly, a renderer only when a player actually plays:

| file | role |
|---|---|
| `dotlottie-player.mjs` | entry point, referenced as `/assets/frappe/js/lib/dotlottie-player/dotlottie-player.mjs?v=2.7.12` |
| `chunk-ODPU3M3Z.mjs`, `chunk-TRZ6EGBZ.mjs`, `chunk-HDDX7F4A.mjs`, `chunk-ZWH2ESXT.mjs` | imported eagerly by the entry |
| `lottie_svg-MJGYILXD-NRTSROOT.mjs`, `lottie_canvas-CDSUBMCL-MZNYH5VV.mjs`, `lottie_html-X3TYKVQI-L6774NQS.mjs`, `lottie_light-KMJEUZFY-TNG7ODX7.mjs`, `lottie_light_canvas-B5UTTNXA-PRI6IBWW.mjs`, `lottie_light_html-SLCECTRT-SYWXEBDN.mjs`, `lottie_worker-Q23FJ6ZR-YP5OKMTN.mjs` | renderers, `import()`ed on demand — one per `renderer` value the component accepts |
| `dotlottie-audio-75C54RUV.mjs`, `dotlottie-state-machine-manager-2E7RUGJG-NTQ25VSR.mjs` | optional features, `import()`ed on demand |

Every file carries a `//// Neoffice` header, so `grep -rn "////"` finds them; the entry's header
holds the full reason and the re-vendor recipe. Two things differ from the published bytes: those
headers, and the trailing `//# sourceMappingURL` comments, dropped because the `.map` files are not
vendored and each would 404. The chunk filenames are content-hashed by the publisher, so only the
entry needs the `?v=` cache-bust — bump it in **both** templates at a re-vendor.

**Added files, no upstream equivalent** in v15.120 or develop: they cannot conflict at the merge,
they can only be lost by someone deleting the directory. If a merge ever drops them, the two
templates 404 on the player and every Lottie animation silently stops rendering.

### Final newline dropped — restored at the v15.123.1 merge

`views/reports/report_view.js` and `views/translation_manager.js` (whose only divergence it was) end
with a newline again; their markers about it are gone.

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
| `frappe/desk/doctype/todo/todo.json` | **none — reverted to upstream on 2026-09-04** (was: `status` options `Open\nClosed\nCancelled` → `test\nClosed\nCancelled\nOpen`) | 4e23539603 (2024-09-23 "last updates", empty message), reverted for #205 | The accident is undone — the list is upstream's again, `Open` first. The fleet *did* store `test` (one ToDo on Osiris), so the revert ships with `frappe/patches/v15_0/neoffice_todo_status_test_to_open.py`, which carries those rows to `Open`. Nothing left to reconcile at the merge. At the v15.123.1 merge the file itself went back to upstream's bytes (the revert had re-serialised it with four-space indents and a new `modified`). |

### Translation catalogue — a `.po` cannot carry a marker the guard accepts

| File | Divergence vs `v15.89.0` | Introduced by | Note |
|---|---|---|---|
| `frappe/locale/fr.po` | French entries we fixed or added: grid and form strings, the account-enumeration messages of the #353 backport, and `Forgot Password` → « Mot de passe oublié » (the login heading, #365); the words of a job's steps by its nature, each with its context (« Construction site », « Workshop repair », « Maintenance », « Intervention », « Time job ») | fa0221ec33, 84c371c586, 94a6b5b015, 2026-09-12; 3dcacebc7f, 2026-10-08; 2026-10-12 | Never regenerate it with `bench update-po-files` (it rewrites and loses entries). At the v15.123.1 merge the catalogue was merged entry by entry on upstream's text: our msgstr wins wherever we had changed it (1 448 entries) or added the same msgid with another text (53), upstream's everywhere else, and the 161 entries only we have sit at the end of the file. Do the same at the next merge. The catalogue already failed `msgfmt -c` before these edits, on `Use % for any non empty value.` (a `python-format` flag on a string that is not a format): harmless for frappe's own compiler. |

### Final newline dropped — restored at the v15.123.1 merge

The nine files below had lost their final newline; it was put back at the merge and the markers that
said so are gone. `communication/email.py`, `translate.py` (save for one marked line) and
`templates/print_format/print_format.css` are back to upstream's bytes; `www/login.html` still has
its marked b2b and « Forgot Password » blocks.

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


`frappe/templates/print_formats/chrome_pdf_header_footer.html` has no final newline either, but it is
a file we added (byte-identical to frappe develop at `4f365bfbf5`), so it is not a divergence.

### Defects found while marking lot F3 (marked in place, repeated here for the merge)

Status at the v15.123.1 merge: `image.py` and `types/filter.py` are upstream's again, `attach_print()`
was removed (#205), the crossed margins were fixed (2026-09-03), the `/mnt/neoffice` widening was
removed (e6f13ac8dc) and `redis.exceptions.ConnectionError` is no longer imported. Still open:
`utils/user.py` (`view_interface`) and the module-level `import frappe` of `utils/__init__.py`.

| Where | What |
|---|---|
| `frappe/utils/image.py`, `optimize_image()` | 🔴 Upstream added `exif = image.getexif()` (`d0eabcd4f6`, 2024-09-02, backport of #27341). Our merge `0b9b53c7ea` kept upstream's `exif=exif` argument in the `image.save()` call but **lost the assignment**. Every call raises `NameError`, the function's broad `except Exception` swallows it into a "Failed to optimize image" msgprint and returns the ORIGINAL bytes — image optimisation is silently off fleet-wide. Restoring the upstream line is the fix. |
| `frappe/utils/print_utils.py`, `attach_print()` | 🔴 Dead **and** broken: it came with the develop backport, but `frappe/__init__.py` still defines `attach_print` and that is the one every caller uses, so this copy runs from nowhere — and it calls `cint()` / `cstr()`, which this module never imports. |
| `frappe/utils/pdf.py`, `prepare_options()` | 🔴 The two margin defaults are **crossed**: the `if not options.get("margin-right")` branch sets `margin-left`, and the `margin-left` branch sets `margin-right`. Harmless only while both are unset. |
| `frappe/types/filter.py` | 🔴 Our copy is frappe develop's, which needs **Python 3.12+** (PEP 695). Upstream version-15 shipped its own 3.10-compatible backport in v15.103.3 — take upstream's file at the merge. |
| `frappe/utils/user.py`, `load_user()` | 🔴 Selects `view_interface`, which is **not** a field of the shipped User doctype — it must be a Custom Field from a Neoffice app. On any site without it, loading the boot info raises. |
| `frappe/utils/__init__.py` | 🔴 Module-level `import frappe` (upstream deliberately avoids it — circular import) and an unused `from redis.exceptions import ConnectionError` that **shadows the builtin** for this module and for every `from frappe.utils import *` consumer. |
| `frappe/utils/file_manager.py`, `is_safe_path()` | 🔴 A security guard widened with a hard-coded `/mnt/neoffice` prefix, by a commit with no message. |

## Binary files, and the vendored pdf.js viewer

A binary diff has no line a marker could sit on, so `fork_markers.py check` excuses a binary we add
or change only when this manifest names its path, or when a pattern of a `| Binary |` table matches
it (neoffice-maintenance#414). Measured from `v15.89.0`, these are all of them.

The pdf.js viewer is declared whole, as an artifact: the check skips the directory, its text files
included, because none of it is ours to mark.

| Artifact | Why |
|---|---|
| `frappe/public/pdfjs/*` | The pdf.js viewer, vendored whole for the embedded print preview (`e1a3fd4e35`, 2026-06-12): the generic viewer build with its CMaps, standard fonts, wasm decoders, ICC profile and images (185 of its files are binaries). Third-party files with no upstream equivalent: take the next pdf.js release whole, never edit them. |

| Binary | Why |
|---|---|
| `frappe/public/fonts/forum/*` | Forum, the title typeface of the Neoffice design system, self-hosted for the cockpit (`a7b5b73deb`, 2026-06-10). No upstream equivalent. |
| `frappe/public/images/frappe-logo.png` | Upstream's Frappe logo, replaced by the Neoffice one (`c2b7ad6ad8`, 2025-03-16). **Keep ours at the merge.** |
| `frappe/public/images/frappe-framework-logo.png` | Same artwork, same commit. **Keep ours at the merge.** |
| `frappe/public/images/icon-logiciel-erp-gestion-business-app.jpg` | The Neoffice app icon (`82445e57e0`, 2025-06-27). No upstream equivalent. |
| `frappe/public/lottie/loader.lottie` | The Neoffice loader animation, played by the dotlottie player above (`bfd6894b1d`, 2025-03-26; redrawn in `83a0b03f45`, 2026-06-09). No upstream equivalent. |

A wheel, `markitdown-0.0.1-py3-none-any.whl`, sat at the root of the repository from `11b57094e7`
(2025-11-14): committed by accident and referenced by nothing, while benches install markitdown
from PyPI. It was removed rather than declared.

## Auto-marked (fork-markers workflow)

- `babel_extractors.csv` — added the row `**.ts,frappe.gettext.extractors.javascript.extract` — TypeScript files were never extracted into the POT: the CSV mapped `**.py`, `**.js`, `**.html` and `**.vue` but no `**.ts`, so every `__()` call in a `.ts` file was invisible to `bench generate-pot-file` in every app (0955ff3001 "fix(i18n): TypeScript files were never extracted into the POT")
- `frappe/public/js/lib/neocockpit.global.js` — re-vendored bundle v=80 then v=81 (frappe-sidebar-react `cf2878682`, `c763e62af`) — minified build artefact overwritten wholesale by the cockpit release script; any inline marker would land inside a JS template literal spanning these bundled lines and would not survive the next rebuild anyway, see Lot F2 above (e39dac47d1 "feat(cockpit): re-vendor bundle v=80 (frappe-sidebar-react cf2878682)", 78c21bdc50 "feat(cockpit): re-vendor bundle v=81 (frappe-sidebar-react c763e62af)")

## Lot M — the merge of upstream v15.123.1 (2026-10-20): files with no comment syntax

These still differ from upstream after the merge and cannot carry a marker (JSON, a minified
stylesheet, a `.bundle.js` the check does not mark). Every row is an old divergence the merge kept;
none of it was introduced by the merge. "TO REVIEW" rows are candidates to go back to upstream's file.

| File | Divergence vs `v15.123.1` | Introduced by |
|---|---|---|
| `package.json` | `frappe-datatable` comes from our fork `github:bvisible/datatable` (upstream v1.21.2 plus our styling fix `c77eddabd7`; this frappe asks for v1.21.3, whose only change is a tree-sorting fix in `datamanager.js`, not merged into the fork yet) instead of the npm release | our datatable fork; last re-merged with upstream on 2026-09-29 |
| `frappe/public/js/desk.bundle.js` | three imports: the sidebar template and the apps switcher (script + template) of our sidebar redesign; marked in place, but the check does not read `.bundle.js` files | 9999364ec6 (2025-10-28 "Refonte de l'interface utilisateur avec nouvelle sidebar et apps switcher") |
| `frappe/public/js/frappe/views/kanban/kanban_board.bundle.js` | a branch that turns a new ToDo kanban column into an option of the ToDo status field (`neoffice_custom_fields.events.add_status_options`); marked in place | 4c842a98fc (2023-10-30 "First change v15"), 12249186f3 |
| `frappe/public/css/animate.min.css` | added file: animate.css 4.1.1, vendored as published (MIT) | bd41f1e7a5 (2025-02-26 "Update neov2") |
| `frappe/public/lottie/animation-404.json` | added file: the Lottie animation of our 404 page (`www/404.html`) | bd41f1e7a5 |
| `frappe/core/page/dashboard_view/dashboard_view.json`, `frappe/core/page/permission_manager/permission_manager.json`, `frappe/desk/page/backups/backups.json`, `frappe/desk/page/leaderboard/leaderboard.json`, `frappe/desk/page/setup_wizard/setup_wizard.json`, `frappe/desk/page/user_profile/user_profile.json`, `frappe/printing/page/print/print.json`, `frappe/printing/page/print_format_builder/print_format_builder.json`, `frappe/printing/page/print_format_builder_beta/print_format_builder_beta.json` | Pages re-exported from a developer-mode site: `restrict_to_domain` set to `Core`, trailing spaces dropped, `creation`/`modified` moved. TO REVIEW: the domain restriction is the only functional part | 7446c62c35 (2023-11-15 "updates for v15 / restrict domains", no rationale) |
| `frappe/automation/workspace/tools/tools.json`, `frappe/core/workspace/users/users.json`, `frappe/core/workspace/welcome_workspace/welcome_workspace.json`, `frappe/website/workspace/website/website.json` | Workspaces trimmed (shortcuts and cards removed) and restricted to the `Core` domain, for our own workspace layout | 4e23539603 (2024-09-23 "last updates", no message), 7446c62c35 |
| `frappe/core/form_tour/doctype/doctype.json`, `frappe/custom/form_tour/custom_field/custom_field.json`, `frappe/printing/form_tour/letter_head/letter_head.json`, `frappe/printing/form_tour/print_format/print_format.json` | Form tours re-exported with the keys of a newer Form Tour schema (`hide_buttons`, `offset_x`…). TO REVIEW: no functional change identified | 4e23539603 |
| `frappe/custom/module_onboarding/customization/customization.json`, `frappe/website/module_onboarding/website/website.json`, `frappe/website/onboarding_step/add_blog_category/add_blog_category.json`, `frappe/website/onboarding_step/create_blogger/create_blogger.json`, `frappe/website/onboarding_step/enable_website_tracking/enable_website_tracking.json`, `frappe/website/onboarding_step/introduction_to_website/introduction_to_website.json`, `frappe/website/onboarding_step/web_page_tour/web_page_tour.json` | Onboarding: our help address instead of the ERPNext documentation link, form tours switched off on the website steps | 7446c62c35, 4e23539603 |
