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
