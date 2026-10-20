# //// Neoffice — added file (no upstream equivalent): a one-off DEBUG script, not a test (11b57094e7
# //// 2025-11-14 "feat: Support icon name resolution in App Customization", which does not mention it). It
# //// connects to the site "prod.local" at import time and prints get_workspace_sidebar_items("Selling")
# //// with a `_debug_sort_order` key that no code sets any more. Nothing imports it. TO REVIEW: delete it.
import frappe
import json
from frappe.desk.desktop import get_workspace_sidebar_items

frappe.init(site="prod.local")
frappe.connect()
frappe.set_user("Administrator")

result = get_workspace_sidebar_items("Selling")

pages_info = []
for p in result['pages'][:15]:
    pages_info.append({
        'name': p['name'],
        'sort': p.get('_debug_sort_order', 'N/A')
    })

print("===RESULT_JSON===")
print(json.dumps(pages_info, indent=2))
print("===END_JSON===" )
