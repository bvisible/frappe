// //// Neoffice — added file (no upstream equivalent)
// neoffice-maintenance#3104: after a re-connection of the realtime socket, the client joins again every room the tab
// still follows (the documents of `open_docs`, the doctypes still counted), once per re-connection, as upstream develop
// does (6b25ca0fa4), on both paths: the manager's automatic reconnection (it fires « reconnect » and the socket's
// « connect ») and the manual socket.connect() of #889 after a refused connection (« connect » only), which is why the
// call sits in our « connect » handler. The first connection joins nothing more than form-load did. The desk never
// leaves a document's room when a form closes (form-unload sends doc_close, not doc_unsubscribe): such a document is
// joined again too, as upstream does.
//
// public/js/frappe/socketio_client.js runs here in a bare context with a stand-in for socket.io and the desk's form
// views. This file lives outside public/ so that it is never served. No CI of ours runs it yet
// (neoffice-maintenance#3117); from the app's folder: node --test frappe/tests/js/socketio_client.test.js
// (SOCKETIO_CLIENT=<file> plays it on another version of the client).

const test = require("node:test");
const assert = require("node:assert");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");

const FILE =
	process.env.SOCKETIO_CLIENT ||
	path.join(__dirname, "..", "..", "public", "js", "frappe", "socketio_client.js");

function load() {
	const timers = [];
	const socketHandlers = {};
	const managerHandlers = {};
	const docHandlers = {};
	const emitted = [];
	const socket = {
		connected: false,
		on(event, fn) {
			(socketHandlers[event] = socketHandlers[event] || []).push(fn);
		},
		off() {},
		emit(event, ...args) {
			emitted.push([event, ...args]);
		},
		connect() {
			socket.connectCalls = (socket.connectCalls || 0) + 1;
		},
		io: {
			on(event, fn) {
				(managerHandlers[event] = managerHandlers[event] || []).push(fn);
			},
		},
	};
	const window = { location: { protocol: "https:", origin: "https://zz.example" } };
	const context = {
		console: { log() {}, error() {} },
		setTimeout: (fn, ms) => timers.push({ fn, ms }),
		clearTimeout() {},
		Math,
		window,
		document: { addEventListener() {}, visibilityState: "visible" },
		io: () => socket,
		$: () => ({
			on(events, fn) {
				for (const event of events.split(" ")) docHandlers[event] = fn;
			},
		}),
		frappe: {
			provide() {},
			boot: { sitename: "zz" },
			flags: {},
			views: { formview: {} },
			request: { check_session: () => Promise.resolve("active"), cleanup() {} },
			app: { handle_session_expired() {} },
		},
	};
	vm.createContext(context);
	const source = fs.readFileSync(FILE, "utf8").replace(/^import .*$/m, "");
	vm.runInContext(source, context);
	const realtime = context.frappe.realtime;
	realtime.init(9000);
	const fire = (handlers, event, ...args) =>
		(handlers[event] || []).forEach((fn) => fn(...args));
	const pass = () => (context.frappe.flags.doc_subscribe = false); // the throttle's second has passed
	// one form view per doctype, as the desk keeps them: it shows one document at a time
	const show = (doctype, docname, first) => {
		const frm = { doctype, docname, doc: {}, is_new: () => false };
		context.frappe.views.formview[doctype] = { frm };
		window.cur_frm = frm;
		if (first) docHandlers["form-load"]({}, frm);
		pass();
		docHandlers["form-refresh"]({}, frm);
		pass();
		return frm;
	};
	return {
		realtime,
		socket,
		emitted,
		timers,
		show,
		// a form view switches to another document, as the desk does it: Form.refresh calls switch_doc, whose
		// setup_docinfo_change_listener calls frappe.realtime.doc_subscribe directly (form.js), then form-load
		// comes within the same second (its own doc_subscribe is held by the throttle)
		switchTo(doctype, docname) {
			const frm = { doctype, docname, doc: {}, is_new: () => false };
			context.frappe.views.formview[doctype] = { frm };
			window.cur_frm = frm;
			realtime.doc_subscribe(doctype, docname);
			docHandlers["form-load"]({}, frm);
			pass();
			docHandlers["form-refresh"]({}, frm);
			pass();
			return frm;
		},
		// she leaves the form for a list: the desk sends doc_close; the form view keeps its document
		leave(frm) {
			docHandlers["form-unload"]({}, frm);
			window.cur_frm = null;
		},
		connect() {
			socket.connected = true;
			fire(socketHandlers, "connect");
		},
		drop() {
			socket.connected = false;
			fire(socketHandlers, "disconnect", "transport close");
		},
		// socket.io-client's automatic reconnection: the manager's « reconnect » and the socket's « connect »
		autoReconnect() {
			socket.connected = true;
			fire(socketHandlers, "connect");
			fire(managerHandlers, "reconnect", 1);
		},
		// a connection refused by the server's middleware during a restart: #889 schedules socket.connect()
		refused() {
			socket.connected = false;
			fire(socketHandlers, "connect_error", { message: "Unauthorized: Bad Gateway" });
		},
		runTimers(atMost = Infinity) {
			const due = timers.filter((t) => (t.ms || 0) <= atMost);
			timers.splice(0, timers.length, ...timers.filter((t) => (t.ms || 0) > atMost));
			due.forEach((t) => t.fn());
		},
	};
}

// the rooms joined in `emitted` since `from`
const joins = (emitted, from) =>
	emitted
		.slice(from)
		.filter(([event]) => event === "doc_subscribe" || event === "doctype_subscribe")
		.map((e) => e.join(" "))
		.sort();

// a tab after some work: two scans opened in the scan form, an invoice opened and left for its list, a list view
function worked() {
	const c = load();
	c.connect();
	c.show("Document Scan", "SCAN-A", true);
	const invoice = c.show("Purchase Invoice", "PINV-1", true);
	c.leave(invoice);
	c.realtime.doctype_subscribe("Purchase Invoice");
	c.show("Document Scan", "SCAN-B", true); // the same form view now shows SCAN-B
	return c;
}

const PATHS = [
	["an automatic reconnection", (c) => c.autoReconnect()],
	[
		"the manual socket.connect() of #889 after a refused connection",
		(c) => {
			c.refused();
			c.runTimers(30000); // schedule_reconnect's timer: socket.connect()
			assert.ok(c.socket.connectCalls >= 1, "#889 asked for the connection again");
			c.connect(); // the server accepts it: « connect » only, no manager « reconnect »
		},
	],
];

test("the first connection joins nothing more than form-load did", () => {
	const c = load();
	c.connect();
	assert.deepStrictEqual(joins(c.emitted, 0), []);
	c.show("Document Scan", "SCAN-A", true);
	assert.deepStrictEqual(joins(c.emitted, 0), ["doc_subscribe Document Scan SCAN-A"]);
});

test("a form closed the desk's way stays in open_docs", () => {
	const c = worked();
	assert.strictEqual(c.realtime.open_docs.size, 3, "doc_close leaves nothing");
	assert.ok(
		c.emitted.some(([event]) => event === "doc_close"),
		"form-unload sends doc_close"
	);
	assert.ok(!c.emitted.some(([event]) => event === "doc_unsubscribe"));
});

for (const [path_, reconnect] of PATHS) {
	test(`${path_} joins again, once, every room the tab still follows`, () => {
		const c = worked();
		const before = c.emitted.length;
		c.drop();
		reconnect(c);
		assert.deepStrictEqual(joins(c.emitted, before), [
			"doc_subscribe Document Scan SCAN-A",
			"doc_subscribe Document Scan SCAN-B",
			"doc_subscribe Purchase Invoice PINV-1",
			"doctype_subscribe Purchase Invoice",
		]);
	});

	test(`${path_}: three documents opened in a row in the same form view are all joined again`, () => {
		// the case of the third review (19.11): Form.refresh → switch_doc → doc_subscribe directly, then form-load
		const c = load();
		c.connect();
		c.switchTo("ToDo", "TODO-1");
		c.switchTo("ToDo", "TODO-2");
		c.switchTo("ToDo", "TODO-3");
		assert.strictEqual(c.realtime.open_docs.size, 3);
		const before = c.emitted.length;
		c.drop();
		reconnect(c);
		assert.deepStrictEqual(joins(c.emitted, before), [
			"doc_subscribe ToDo TODO-1",
			"doc_subscribe ToDo TODO-2",
			"doc_subscribe ToDo TODO-3",
		]);
	});
}

test("a doctype room left by every view is not joined again; one another view still uses is", () => {
	const c = worked();
	c.realtime.doctype_subscribe("Purchase Invoice"); // a second view of the same doctype
	c.realtime.doctype_unsubscribe("Purchase Invoice"); // one of the two leaves: the room stays
	c.realtime.doctype_subscribe("Supplier");
	c.realtime.doctype_unsubscribe("Supplier"); // the only view of Supplier leaves
	const before = c.emitted.length;
	c.drop();
	PATHS[1][1](c); // the manual path of #889: red without our call in « connect »
	assert.deepStrictEqual(
		joins(c.emitted, before).filter((j) => j.startsWith("doctype")),
		["doctype_subscribe Purchase Invoice"]
	);
});

test("a room a page joined itself is joined again, as upstream does", () => {
	const c = worked();
	// a page outside any form follows a document (a shopping list, a catalogue, a chat)
	c.realtime.doc_subscribe("Shopping List", "SL-1");
	const before = c.emitted.length;
	c.drop();
	PATHS[1][1](c);
	assert.ok(joins(c.emitted, before).includes("doc_subscribe Shopping List SL-1"));
});
