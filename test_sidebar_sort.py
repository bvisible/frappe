import frappe

def test_sidebar_sorting():
    """Test the sidebar sorting logic"""
    frappe.connect()
    frappe.set_user("Administrator")

    # Test with a workspace from Commercial app
    current_workspace = "Selling"

    print(f"\n=== Testing with current_workspace: {current_workspace} ===\n")

    # Find which app it belongs to
    app_workspace_link = frappe.db.get_value(
        "App Customization Workspace",
        {"workspace_name": current_workspace},
        ["parent", "hidden"],
        as_dict=True
    )

    print(f"1. App workspace link: {app_workspace_link}")

    if app_workspace_link and not app_workspace_link.hidden:
        app_name = app_workspace_link.parent
        print(f"2. App name: {app_name}")

        # Get app customization
        app_custom = frappe.get_doc("App Customization", app_name)
        print(f"3. App custom enabled: {app_custom.enabled}")
        print(f"   is_virtual: {app_custom.is_virtual}")
        print(f"   manage_all_workspaces: {app_custom.manage_all_workspaces}")

        if app_custom.enabled and (app_custom.is_virtual or app_custom.manage_all_workspaces):
            # Get all workspaces for this app
            workspace_configs = frappe.get_all(
                "App Customization Workspace",
                filters={"parent": app_name, "hidden": 0},
                fields=["workspace_name", "sort_order"],
                order_by="sort_order asc, idx asc"
            )

            print(f"\n4. Workspace configs ({len(workspace_configs)}):")
            for w in workspace_configs:
                print(f"   - {w['workspace_name']}: sort_order={w['sort_order']}")

            current_app_workspaces = [w['workspace_name'] for w in workspace_configs]
            workspace_sort_order = {w['workspace_name']: w['sort_order'] for w in workspace_configs}

            print(f"\n5. workspace_sort_order dict: {workspace_sort_order}")
            print(f"6. Dict is truthy: {bool(workspace_sort_order)}")
            print(f"7. current_app_workspaces is not None: {current_app_workspaces is not None}")

if __name__ == "__main__":
    test_sidebar_sorting()
