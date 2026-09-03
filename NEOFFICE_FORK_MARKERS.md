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
