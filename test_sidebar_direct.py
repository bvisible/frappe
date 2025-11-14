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
