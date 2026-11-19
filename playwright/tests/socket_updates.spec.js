import { test, expect } from "../support";
import { doc_room_joined, doctype_room_joined, realtime_connected } from "../support/realtime";

test.describe("Realtime updates", () => {
	test.beforeEach(async ({ page, desk }) => {
		await page.goto("/app/todo");
		await realtime_connected(page);
		await desk.clear_filters();
	});

	test("Shows version conflict warning", async ({ page, desk, api }) => {
		const doc = await api.insert_doc("ToDo", { description: "old" });
		await page.goto(`/app/todo/${doc.name}`);
		await realtime_connected(page);
		await doc_room_joined(page, api, "ToDo", doc.name);
		await desk.fill_field("status", "Cancelled", "Select");

		await api.update_doc("ToDo", doc.name, { status: "Closed" });
		await page.getByRole("button", { name: "Refresh", exact: true }).click();
		await expect(desk.get_field("status", "Select")).toHaveValue("Closed");
	});

	test("List view updates in realtime on insert", async ({ page, api }) => {
		const original = "Added for realtime update";
		const updated = "Updated for realtime update";
		await doctype_room_joined(page, api, "ToDo");
		const doc = await api.insert_doc("ToDo", { description: original });
		await expect(page.getByText(original).first()).toBeVisible();

		await api.update_doc("ToDo", doc.name, { description: updated });
		await expect(page.getByText(updated).first()).toBeVisible();
	});

	// //// Neoffice — backport of upstream develop d72bfc61a2 (neoffice-maintenance#3104), as written there;
	// //// drop at the merge of a frappe that carries it.
	test("Keeps a doctype room while another view uses it", async ({ page, api }) => {
		await doctype_room_joined(page, api, "ToDo");
		await page.evaluate(() => {
			frappe.realtime.doctype_subscribe("ToDo");
			frappe.realtime.doctype_unsubscribe("ToDo");
		});
		await doctype_room_joined(page, api, "ToDo");
	});

	// //// Neoffice — backport of upstream develop 3dee46a77f (neoffice-maintenance#3104), as written there;
	// //// drop at the merge of a frappe that carries it.
	test("List view leaves its doctype room only once", async ({ page, api }) => {
		await doctype_room_joined(page, api, "ToDo");
		await page.evaluate(() => {
			frappe.realtime.doctype_subscribe("ToDo");
			cur_list.disable_realtime_updates();
			cur_list.disable_realtime_updates();
		});
		await doctype_room_joined(page, api, "ToDo");
	});

	// //// Neoffice — backport of upstream develop df73b79278 (neoffice-maintenance#3104); the form's route is
	// //// version-15's /app/ where upstream develop has /desk/. Drop at the merge of a frappe that carries it.
	test("Restores rooms after reconnect", async ({ page, api }) => {
		const doc = await api.insert_doc("ToDo", { description: "reconnect" });
		await page.goto(`/app/todo/${doc.name}`);
		await realtime_connected(page);
		await page.evaluate(() => frappe.realtime.doctype_subscribe("ToDo"));
		await doc_room_joined(page, api, "ToDo", doc.name);

		await page.evaluate(
			() =>
				new Promise((resolve) => {
					frappe.realtime.socket.io.once("reconnect", resolve);
					frappe.realtime.socket.io.engine.close();
				})
		);
		await doctype_room_joined(page, api, "ToDo");
		await doc_room_joined(page, api, "ToDo", doc.name);
	});

	// //// Neoffice — neoffice-maintenance#3104: the socket.connect() of our #889 brings a refused socket back with
	// //// « connect » only, never the manager's « reconnect »: the rooms come back on that path too (the call in
	// //// the « connect » handler of socketio_client.js). disconnect() then connect() is not the server's refusal
	// //// of #889's real path (a middleware that refuses during a restart): it plays the same event on the client,
	// //// « connect » with no « reconnect ». Drop with that call.
	test("Restores rooms after a manual reconnect", async ({ page, api }) => {
		const doc = await api.insert_doc("ToDo", { description: "manual reconnect" });
		await page.goto(`/app/todo/${doc.name}`);
		await realtime_connected(page);
		await doc_room_joined(page, api, "ToDo", doc.name);

		await page.evaluate(
			() =>
				new Promise((resolve) => {
					const socket = frappe.realtime.socket;
					socket.once("connect", resolve);
					socket.disconnect();
					socket.connect();
				})
		);
		await doc_room_joined(page, api, "ToDo", doc.name);
	});

	test("Receives msgprint from server", async ({ page, api }) => {
		const msg = "msgprint sent via realtime";
		await api.call("frappe.tests.ui_test_helpers.publish_realtime", {
			event: "msgprint",
			message: msg,
		});
		await expect(page.getByText(msg).first()).toBeVisible();
	});

	test("Recieves custom messages from server", async ({ page, api }) => {
		const event = "playwright_event";
		await page.evaluate((event) => {
			window.realtime_event_received = false;
			frappe.realtime.on(event, () => {
				window.realtime_event_received = true;
			});
		}, event);

		await api.call("frappe.tests.ui_test_helpers.publish_realtime", { event });
		await expect.poll(() => page.evaluate(() => window.realtime_event_received)).toBe(true);
	});

	test("Runs the progress handler of a subscribed task", async ({ page, api }) => {
		await page.evaluate(() => {
			window.task_progress = null;
			frappe.realtime.subscribe("playwright_task", {
				progress: (data) => (window.task_progress = data.percent),
			});
		});

		await api.call("frappe.tests.ui_test_helpers.publish_realtime", {
			task_id: "playwright_task",
			message: { percent: 50 },
		});
		await expect.poll(() => page.evaluate(() => window.task_progress)).toBe(50);
	});

	test("Ignores progress of a task subscribed without handlers", async ({ page, api }) => {
		const errors = [];
		page.on("pageerror", (error) => errors.push(error.message));
		await page.evaluate(() => {
			window.task_progress = null;
			frappe.realtime.task_subscribe("playwright_task");
			frappe.realtime.on("task_progress", (data) => (window.task_progress = data.percent));
		});

		await api.call("frappe.tests.ui_test_helpers.publish_realtime", {
			task_id: "playwright_task",
			message: { percent: 50 },
		});
		await expect.poll(() => page.evaluate(() => window.task_progress)).toBe(50);
		expect(errors).toEqual([]);
	});

	test("Progress bar", async ({ page, api }) => {
		const title = "RealTime Progress";
		await api.call("frappe.tests.ui_test_helpers.publish_progress", { title });
		await expect(page.getByText(title).first()).toBeVisible();
	});
});
