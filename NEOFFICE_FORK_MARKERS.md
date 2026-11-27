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

## Before changing what a user sees: not here

Since 2026-11-25 the desk's interface of this fork (`frappe/public`: JS, SCSS, HTML, Vue) is upstream Frappe 15's text,
but for 7 files kept on purpose (`socketio_client.js`, `scanner/index.js`, `jquery-bootstrap.js`,
`views/kanban/kanban_board.bundle.js`, `web_form/web_form_list.js`, `file_uploader/FileUploader.vue`,
`scss/login.bundle.scss`; their reasons are below). What Neoffice changes in the desk lives in the `neoffice_theme` app, on
Frappe 15 and 16 alike: on Frappe 15 its `public/js/*_15.js` files put our text on Frappe's classes through
`neoffice.desk.patcher`, and `public/css/desk_fork_15.css` carries what our SCSS added; on Frappe 16 its models do the
same work. A change of the desk made here is lost on Frappe 16 without a word, and the theme's CI refuses it
(`scripts/interface_sources.py check`: a file of `frappe/public` marked `////` that its list does not know). One of the 7
files changes here, with its `////` marker, then the theme's `interface_sources.py carried <file>`. The server side (Python,
templates) still changes here, marked, as below.

## Lot F1 — frappe/public/js

### Final newline dropped — restored at the v15.123.1 merge

`FileUploader.vue`, `form/form.js` and `model/indicator.js` had lost the final newline upstream ends
them with (bd41f1e7a5, 2025-02-26 "Update neov2"). It was put back at the merge, with upstream's
import order in `FileUploader.vue`; nothing is left to record.

### Gone (2026-11-24): the global left sidebar of 2025-10

`frappe/public/js/frappe/ui/sidebar.js` is upstream's again; `ui/apps_switcher.js`, `ui/apps_switcher.html`,
`ui/sidebar.html` and the three images of the switcher (`public/images/mobile-app.svg`, `web.svg`,
`settings-gear.svg`) are gone, with the navbar's hamburger and most of `scss/desk/sidebar.scss`. The NeoCockpit had
replaced that sidebar since 2026-06-10; `workspace.js` reads the spaces of the boot itself. A merge that brings one of
these files back has nothing of ours in it.

### Gone to the theme (2026-11-25): the desk's SCSS

`scss/common/css_variables.scss`, `grid.scss`, `modal.scss`, `quill.scss`, `scss/desk/desktop.scss`, `global.scss`,
`index.scss`, `navbar.scss`, `page.scss` and `sidebar.scss` are upstream's again; `scss/desk/form_sidebar.scss` and
`list_sidebar.scss`, ours, are gone. What they added or changed is `neoffice_theme/public/css/desk_fork_15.css`, first of
the sheets `www/app.py` puts right after Frappe's bundles (`app_include_css_first`), and what they took out of upstream's
rules is written there as the value we left. `scss/login.bundle.scss` stays ours (the login page, outside the desk).

What the fork's interface still carries, on purpose: `socketio_client.js`, `scanner/index.js`, `jquery-bootstrap.js`,
`views/kanban/kanban_board.bundle.js`, `web_form/web_form_list.js`, `file_uploader/FileUploader.vue` (a component of the
bundle) and `scss/login.bundle.scss`.

### Gone to the theme (2026-11-25): the lists and reports

Upstream's again: `list/base_list.js`, `list_factory.js`, `list_filter.js`, `list_sidebar.js`, `list_sidebar.html`,
`list_view.js`, `views/reports/report_view.js`, `report_factory.js`, `report_utils.js`, `query_report.js`,
`ui/filters/filter.js`, `filter_list.js` and `ui/group_by/group_by.js`. What was ours in them is put on Frappe 15's
classes by `neoffice_theme/public/js/list_model_15.js`, which also says `frappe.neo_list_args_supported` as our
list_view.js did. `list/list_settings.js` was ours only for « DocType required » translated, thrown without a doctype:
upstream's, untranslated.

### Gone to the theme (2026-11-25): the desk

Upstream's again: `desk.bundle.js`, `frappe/desk.js`, `ui/page.js`, `ui/toolbar/toolbar.js`, `views/workspace/workspace.js`,
`dom.js`, `router.js`, `utils/utils.js`, `utils/datetime.js`, `model/model.js`, `model/user_settings.js`,
`model/indicator.js`, `translate.js`, `views/breadcrumbs.js`, `ui/toolbar/search_utils.js`, `ui/toolbar/fuzzy_match.js`,
`ui/toolbar/search.js`, `ui/notifications/notifications.js`, `views/factory.js`, `views/workspace/blocks/block.js`,
`blocks/paragraph.js`, `widgets/chart_widget.js`, `links_widget.js`, `quick_list_widget.js`,
`views/dashboard/dashboard_view.js`, `views/file/file_view.js`, `views/kanban/kanban_view.js`, `views/treeview.js`,
`utils/datatable.js`, `onboarding_tours/onboarding_tours.js`, `ui/chart.js`, `ui/page.html` and `ui/toolbar/navbar.html`. What
was ours in them is put on Frappe
15's objects by `neoffice_theme/public/js/desk_model_15.js`, as we wrote it, with Frappe 15's fingerprints.

Still ours here, on purpose: `socketio_client.js` (the backport of upstream develop 1940c3e301 and the reconnection after a
restart; its client is the bundle's own import, and its test is here), `scanner/index.js` (its reader is the bundle's own
import), `jquery-bootstrap.js` (the Bootstrap plugins registered on the one jQuery: a build matter),
`views/kanban/kanban_board.bundle.js` (a lazy bundle) and `web_form/web_form_list.js` (the portal, where the theme's desk
scripts are not loaded).

### Gone to the theme (2026-11-25): the form

Upstream's again: `form/form.js`, `layout.js`, `tab.js`, `save.js`, `link_selector.js`, `quick_entry.js`,
`success_action.js`, `form_viewers.js`, `toolbar.js`, `workflow.js`, `sidebar/form_sidebar.js`, `controls/date.js`,
`controls/datetime.js`, `controls/link.js`, `controls/text_editor.js`, `footer/base_timeline.js`,
`footer/version_timeline_content_builder.js`, `formatters.js`, `model/create_new.js` and `ui/field_group.js`. Each method
we had changed or added in them is put on Frappe 15's classes by `neoffice_theme/public/js/form_model_15.js`, as we wrote
it, with Frappe 15's fingerprint of each method it replaces; the two constructors we had changed (the link selector's,
the form viewers'), the form menu and the timeline are said there. The fork's test of the Preview / Print button moved
with its methods (`neoffice_theme/tests/js/the_preview_button_follows_print.test.js`).

### Gone to the theme (2026-11-25): the table of a form

`frappe/public/js/frappe/form/grid.js`, `grid_row.js` and `grid_row_form.js` are upstream's again. Each method we had
changed or added in them (the full-screen table, the in-table « + Add row », the pinned columns, the widths without the
11-column cap, the horizontal scroll, the bulk edit, the duplicated rows, the backport of upstream frappe#22862) is put on
Frappe 15's classes by `neoffice_theme/public/js/grid_model_15.js`, as we wrote it, with Frappe 15's fingerprint of
each method it replaces (a merge that rewrites one is said in the desk's console). `scss/common/grid.scss` stays here
until the theme carries the sheets. A merge that changes the three files has nothing of ours in them.

### Gone to the theme (2026-11-25): the e-mail composer and what the desk says about a request

`frappe/public/js/frappe/views/communication.js` and `frappe/public/js/frappe/request.js` are upstream's again. The
composer as Neoffice has it (CC / BCC on one row, the template beside the schedule, the signature looked up only by
who may read Email Account) is `neoffice_theme/public/js/communication_composer.js`, on Frappe 15 and 16; its drafts
moved from IndexedDB to localStorage, an old one read and carried over the first time it is asked for. `frappe.call`
(no « Connection Lost » from navigator.onLine) and `frappe.request.call` (one « Connection Lost » for a request that
reached no server, the refusal that asks whether the session lives, the `silent` reading refused in the console) are
`neoffice_theme/public/js/request_model.js`, put through the theme's patcher with Frappe 15's fingerprints; its test
(`neoffice_theme/tests/test_a_background_refusal_opens_no_window.py`) replaces the fork's. A merge that changes either
file has nothing of ours in it; a rewritten `frappe.call` or `frappe.request.call` is said in the desk's console.

## Lot F2 — frappe/public/js

### Gone to the theme (2026-11-24): the cockpit and the sheets of its pieces

The cockpit's bundle (`public/js/lib/neocockpit.global.js`), its sheet (`public/css/cockpit.css`), the pill of a list's filters
(`public/js/lib/cockpit_filter_pill.js`) and the sheets of a form's compact head, of the floating arrows, of the form dialog and
of the search (`scss/desk/compact_header.scss`, `floating_nav_arrows.scss`, `form_dialog.scss`, and what we had added to
`scss/common/awesomeplete.scss` and `scss/desk/frappe_datatable.scss`, upstream's again) have one copy, `neoffice_theme`'s
(`first_assets.py`). This fork loads them at the place they had, right after Frappe's own files: `www/app.py` reads
`app_include_css_first` and `app_include_js_first` (marked there). Nothing to record here any more; a merge that brings one of
these files back has nothing of ours in it.

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
| `frappe/locale/fr.po`, `frappe/locale/de.po`, `frappe/locale/it.po` | The entries of the sign-in by code (27 in French, 32 in German, 33 in Italian, appended at the end of each file; the three before the last three are the written-out link of the mail and the « Copy the link » button of the link's page, the last three the « Open the app » button of the link's page for the mobile app): the mail (`templates/emails/passwordless_code.html`), the link's page (`www/login-link.html`), the two-step `/login` (`www/login.html`, `login.js`) and the engine's refusals (`frappe/passwordless.py`). Filled in place with `scripts/translate/fill_po_in_place.py`, never regenerated. | 2026-11-03 (neoffice-maintenance#2192) | Same rule as above at the next merge: keep our msgstr, upstream's elsewhere. |
| `frappe/locale/fr.po` | `Clear Link` (the tooltip of the cross of a Link field; upstream's msgstr is empty) → « Effacer le lien »; one entry appended at the end for the form dialog (« Record saved. The current document is not saved yet. », `form_dialog.js`). | 2026-11-09 (neoffice-maintenance#1102) | Same rule as above at the next merge: keep our msgstr. |
| `frappe/locale/fr.po` | Four entries appended at the end, each with its context, for strings of our own code: the quotation's last stage `Order` / « Sales document stage » → « Commande » (`form/form_hero.js`; the bare word is a sort order elsewhere), the column menu's `Freeze` / `Unfreeze` / « Column menu of a table » → « Figer » / « Ne plus figer » (`utils/datatable.js`), and the heatmap tooltip's `on {0}` / « Day of a heatmap » → « le {0} » (`ui/chart.js`). | 2026-11-09 (neoffice-maintenance#2421) | Same rule as above at the next merge: keep our msgstr. |
| `frappe/locale/fr.po` | `Change Password` (the User form button) → « Changer le mot de passe » instead of upstream's « Changez le Mot de Passe » (sentence case, infinitive). | 2026-11-11 (neoffice-maintenance#2501) | Same rule as above at the next merge: keep our msgstr. |
| `frappe/locale/fr.po` | Two entries appended at the end for what a screen reader hears while one types in a link field (`public/js/frappe/form/controls/link.js`, its marked wrapper of Awesomplete): `{0} results found` → « {0} résultats trouvés », `{0}, list item {1} of {2}` → « {0}, élément {1} sur {2} de la liste ». `No results found` and `Begin typing for results.` are translated already. | 2026-11-17 (neoffice-maintenance#2782) | Awesomplete's own texts, written in English by the library. |
| `frappe/locale/fr.po` | Three entries appended at the end for the « You have been signed out » window (`public/js/frappe/desk.js`, `handle_session_expired`): `Your session` → « Votre session » (its eyebrow; not the bare `Session`, which an app of the desk translates « Séance »), `You have been signed out` → « Vous avez été déconnecté », and its sentence. `Sign in again` stays empty here: the catalogue already says « Se reconnecter ». | 2026-11-17 (neoffice-maintenance#2776) | Same rule as above at the next merge: keep our msgstr. |

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
| `package.json`, `yarn.lock` | `frappe-datatable` comes from our fork `github:bvisible/datatable` instead of the npm release: upstream v1.22.0 (the version this frappe asks for since 15.126.1: resized column widths kept on refresh, tree rows kept grouped when sorted) plus our styling fix `c77eddabd7`, the Link value list fix `bf47fa3` and the sort fix `9202fab` (a query report sorts its rows: the list view is reloaded before a sort only when the table is its own); and, since `-neoffice.6`, a table that rests when nobody scrolls it, with value lists made on first use (`src/scroll-list.js`, neoffice-maintenance#3262); `yarn.lock` pins the fork's commit (`db1d740a`, version `1.22.0-neoffice.6`; its `dist/` is what frappe imports and is rebuilt at each change). **The fork's `version` is raised at every change** (`-neoffice.N`) and copied into `yarn.lock`: yarn caches a package by name and version, so a site that had any commit of the fork under one version kept it whatever commit the lock named | our datatable fork; last re-merged with upstream v1.22.0 on 2026-11-13 |
| `package.json`, `yarn.lock` | The realtime server runs `socket.io` 4.8 instead of 4.7: `socket.io` is `^4.8.1`, and the `resolutions` hold `socket.io-parser` at `~4.2.7` and `socket.io-adapter` at `~2.5.8`, so that `engine.io` 6.6.11, `socket.io-parser` 4.2.7 and `ws` 8.21 are installed (resource-exhaustion flaws of the realtime transport, Dependabot alerts of 2026-10-29). Drop the `resolutions` and the bump when upstream's lock is past these versions (neoffice-maintenance#1984). | the commit that adds this row (2026-10-29) |
| `frappe/public/js/desk.bundle.js` | three imports: the sidebar template and the apps switcher (script + template) of our sidebar redesign; marked in place, but the check does not read `.bundle.js` files | 9999364ec6 (2025-10-28 "Refonte de l'interface utilisateur avec nouvelle sidebar et apps switcher") |
| `frappe/public/js/frappe/views/kanban/kanban_board.bundle.js` | a branch that turns a new ToDo kanban column into an option of the ToDo status field (`neoffice_custom_fields.events.add_status_options`); marked in place | 4c842a98fc (2023-10-30 "First change v15"), 12249186f3 |
| `frappe/public/css/animate.min.css` | added file: animate.css 4.1.1, vendored as published (MIT) | bd41f1e7a5 (2025-02-26 "Update neov2") |
| `frappe/public/lottie/animation-404.json` | added file: the Lottie animation of our 404 page (`www/404.html`) | bd41f1e7a5 |
| `frappe/core/page/dashboard_view/dashboard_view.json`, `frappe/core/page/permission_manager/permission_manager.json`, `frappe/desk/page/backups/backups.json`, `frappe/desk/page/leaderboard/leaderboard.json`, `frappe/desk/page/setup_wizard/setup_wizard.json`, `frappe/desk/page/user_profile/user_profile.json`, `frappe/printing/page/print/print.json`, `frappe/printing/page/print_format_builder/print_format_builder.json`, `frappe/printing/page/print_format_builder_beta/print_format_builder_beta.json` | Pages re-exported from a developer-mode site: `restrict_to_domain` set to `Core`, trailing spaces dropped, `creation`/`modified` moved. TO REVIEW: the domain restriction is the only functional part | 7446c62c35 (2023-11-15 "updates for v15 / restrict domains", no rationale) |
| `frappe/automation/workspace/tools/tools.json`, `frappe/core/workspace/users/users.json`, `frappe/core/workspace/welcome_workspace/welcome_workspace.json`, `frappe/website/workspace/website/website.json` | Workspaces trimmed (shortcuts and cards removed) and restricted to the `Core` domain, for our own workspace layout | 4e23539603 (2024-09-23 "last updates", no message), 7446c62c35 |
| `frappe/core/form_tour/doctype/doctype.json`, `frappe/custom/form_tour/custom_field/custom_field.json`, `frappe/printing/form_tour/letter_head/letter_head.json`, `frappe/printing/form_tour/print_format/print_format.json` | Form tours re-exported with the keys of a newer Form Tour schema (`hide_buttons`, `offset_x`…). TO REVIEW: no functional change identified | 4e23539603 |
| `frappe/custom/module_onboarding/customization/customization.json`, `frappe/website/module_onboarding/website/website.json`, `frappe/website/onboarding_step/add_blog_category/add_blog_category.json`, `frappe/website/onboarding_step/create_blogger/create_blogger.json`, `frappe/website/onboarding_step/enable_website_tracking/enable_website_tracking.json`, `frappe/website/onboarding_step/introduction_to_website/introduction_to_website.json`, `frappe/website/onboarding_step/web_page_tour/web_page_tour.json` | Onboarding: our help address instead of the ERPNext documentation link, form tours switched off on the website steps | 7446c62c35, 4e23539603 |

## Contracts

What an upstream merge must keep (`scripts/fork_contracts.py` of `bvisible/neoffice-ci`).

| Contract | Rule | Pattern | Why |
| --- | --- | --- | --- |
| `frappe/desk/form/load.py` | present | `readable_version_fields\(doc\.doctype\)` | neoffice-maintenance#2296 (audit Q15): the history of a form shows a reader only the fields they may read. At the v16 merge upstream's own filter of `get_versions` (develop eb3c578130, 82e99a101e) takes its place: then write upstream's call here instead (`readable_permlevels\(doc\.meta\)`), never delete the row |
| `frappe/desk/form/load.py` | present | `narrow = getattr\(doc, "readable_versions", None\)` | neoffice-maintenance#3205: a document filters its own history by ROW for its reader (`readable_versions(versions)` of its controller; the booking settings show an account bound to one company the history of its own line only). Upstream has no equivalent: keep it at every merge |
| `frappe/model/db_query.py` | present | `args\.tables \+= child_join \+ self\.child_join_condition\(child\)` | neoffice-maintenance#3221: a child table joined into its parent's list (report view columns of a child table) follows the child's own `permission_query_conditions` hook, in the join's ON clause of the executed query only (never in the user part that `validate_generated_query` checks: a hook's subquery was refused as an illegal query); a booking's health answers stay out of a desk account's report view. Upstream joins child tables on the parent alone: keep it at every merge |
| `frappe/core/doctype/data_export/exporter.py` | present | `if \(permitted := self\.permitted_child_rows\(c, doc\.name\)\) is not None:` | neoffice-maintenance#3221: the child rows of a Data Export follow the child doctype's own `permission_query_conditions` hook (a booking's health answers left through the export to an account that may not read them). Upstream reads them by a raw query: keep it at every merge |
| `frappe/core/doctype/version/version.py` | present | `self\.data = readable_version_json\(self\.data, self\.ref_doctype\)` | #2296: the Version form and every read of a Version as a document keep only the fields the reader may read of the recorded document |
| `frappe/core/doctype/version/version.py` | present | `data = json\.loads\(readable_version_json\(self\.data, self\.ref_doctype\)` | #2296: the diffs of the Version form are drawn from the readable fields only |
| `frappe/model/db_query.py` | present | `select_version_data_readably\(self\)` | #2296: a list of Version selects its data as a plain column next to the recorded doctype, never inside an expression |
| `frappe/model/db_query.py` | present | `result = filter_version_rows\(self, result\)` | #2296: a list of Version gives each row's data with only the fields its reader may read |
| `frappe/desk/doctype/number_card/number_card.py` | absent | `nc\.module\.isnull\(\) . nc\.module == ""` | neoffice-maintenance#2863: without brackets the list of number cards gave every card without a module to every desk user, whose form then refused them; keep the brackets until upstream's own (frappe#39388, develop) arrives with v16 |
| `frappe/core/doctype/user/user.js` | present | `frm\.roles_editor && !frm\.is_new\(\)` | neoffice-maintenance#3087: the role editor is built for a saved account and stays on the form; upstream's validate copied its checks into a new account too, which got the roles of the account opened before it, unseen |
| `frappe/public/js/frappe/socketio_client.js` | present | `me\.restore_subscriptions\(\);` | neoffice-maintenance#3104: a re-connection of the realtime socket joins again every room the tab still follows, as upstream does (upstream develop 1940c3e301 and 6b25ca0fa4, backported with their tests and c3a77201a0); the call sits in our « connect » handler because the socket.connect() of #889 fires no « reconnect ». At the merge of a frappe that carries upstream's own listener, keep one of the two calls, never both, and never neither |
| `frappe/public/js/frappe/socketio_client.js` | absent | `this\.socket\.io\.on\("reconnect"` | neoffice-maintenance#3104: upstream develop restores the rooms on the manager's « reconnect » (6b25ca0fa4); we call restore_subscriptions() from our « connect » handler instead, which also covers the socket.connect() of #889. Both at once join every room twice: a merge that brings upstream's listener back keeps one of the two calls |
| `frappe/permissions.py` | present | `if _withheld_for\(doctype_meta\.name, user\):` | neoffice-maintenance#3421: the one test of `get_role_permissions` that gives no right on a document the request holds back from the account (`frappe.flags.what_was_given`, set by NORA only, empty unless the site key `nora_what_was_given` is on); upstream's own function has no such test, so a merge that rewrites the function drops it without a conflict and NORA answers again about documents an account was not given |
