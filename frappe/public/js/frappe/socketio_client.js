import { io } from "socket.io-client";

frappe.provide("frappe.realtime");

class RealTimeClient {
	constructor() {
		this.open_tasks = {};
		this.open_docs = new Set();
		// //// Neoffice — state of schedule_reconnect() below (neoffice-maintenance#889).
		this.reconnect_tries = 0;
		this.reconnect_timer = null;
	}

	on(event, callback) {
		if (this.socket) {
			this.connect();
			this.socket.on(event, callback);
		}
	}

	off(event, callback) {
		if (this.socket) {
			this.socket.off(event, callback);
		}
	}

	connect() {
		if (this.lazy_connect) {
			this.socket.connect();
			this.lazy_connect = false;
		}
	}

	emit(event, ...args) {
		this.connect();
		this.socket.emit(event, ...args);
	}

	init(port = 9000, lazy_connect = false) {
		if (frappe.boot.disable_async) {
			return;
		}

		if (this.socket) {
			return;
		}
		this.lazy_connect = lazy_connect;
		let me = this;

		// Enable secure option when using HTTPS
		if (window.location.protocol == "https:") {
			this.socket = io(this.get_host(port), {
				secure: true,
				withCredentials: true,
				reconnectionAttempts: 3,
				autoConnect: !lazy_connect,
			});
		} else if (window.location.protocol == "http:") {
			this.socket = io(this.get_host(port), {
				withCredentials: true,
				reconnectionAttempts: 3,
				autoConnect: !lazy_connect,
			});
		}

		if (!this.socket) {
			console.log("Unable to connect to " + this.get_host(port));
			return;
		}

		this.socket.on("connect_error", function (err) {
			console.error("Error connecting to socket.io:", err.message);
			// //// Neoffice — after a `bench restart` the socket server is back before the web
			// //// server, so its connection middleware (get_user_info) is refused and socket.io
			// //// destroys the socket: `socket.active` is false and it never retries by itself, so
			// //// every open desk page lost realtime until reloaded (neoffice-maintenance#889).
			// //// Remove if upstream retries after a refusal by the connection middleware.
			if (!me.socket.active) {
				me.schedule_reconnect();
			}
		});

		// //// Neoffice — the manager also gives up after `reconnectionAttempts` (3, about 7 s):
		// //// a restart that takes longer leaves a socket that is neither connected nor trying.
		this.socket.io.on("reconnect_failed", function () {
			me.schedule_reconnect();
		});

		this.socket.on("connect", function () {
			me.reconnect_tries = 0;
		});

		// //// Neoffice — a tab that comes back to the foreground reconnects at once instead of
		// //// waiting for the next retry (timers of hidden tabs are throttled).
		document.addEventListener("visibilitychange", function () {
			if (
				document.visibilityState === "visible" &&
				!me.lazy_connect &&
				!me.socket.connected
			) {
				me.reconnect_tries = 0;
				me.socket.connect();
			}
		});

		this.socket.on("msgprint", function (message) {
			frappe.msgprint(message);
		});

		this.socket.on("progress", function (data) {
			if (data.progress) {
				data.percent = (flt(data.progress[0]) / data.progress[1]) * 100;
			}
			if (data.percent) {
				frappe.show_progress(
					data.title || __("Progress"),
					data.percent,
					100,
					data.description,
					true
				);
			}
		});

		this.setup_listeners();

		$(document).on("form-load form-rename", function (e, frm) {
			if (!frm.doc || frm.is_new()) {
				return;
			}
			me.doc_subscribe(frm.doctype, frm.docname);
		});

		$(document).on("form-refresh", function (e, frm) {
			if (!frm.doc || frm.is_new()) {
				return;
			}
			me.doc_open(frm.doctype, frm.docname);
		});

		$(document).on("form-unload", function (e, frm) {
			if (!frm.doc || frm.is_new()) {
				return;
			}

			me.doc_close(frm.doctype, frm.docname);
		});
	}

	// //// Neoffice — retry with a back-off (2 s, 4 s ... 30 s), one timer at a time and at most
	// //// MAX_RECONNECT_TRIES times; a tab coming back to the foreground starts again from zero.
	schedule_reconnect() {
		const MAX_RECONNECT_TRIES = 30;
		if (
			this.reconnect_timer ||
			this.lazy_connect ||
			this.reconnect_tries >= MAX_RECONNECT_TRIES
		) {
			return;
		}
		this.reconnect_tries = (this.reconnect_tries || 0) + 1;
		const wait = Math.min(30000, 1000 * 2 ** this.reconnect_tries);
		this.reconnect_timer = setTimeout(() => {
			this.reconnect_timer = null;
			if (!this.socket.connected) {
				this.socket.connect();
			}
		}, wait);
	}

	get_host(port = 9000) {
		let host = window.location.origin;
		if (window.dev_server) {
			let parts = host.split(":");
			port = frappe.boot.socketio_port || port.toString() || "9000";
			if (parts.length > 2) {
				host = parts[0] + ":" + parts[1];
			}
			host = host + ":" + port;
		}
		return host + `/${frappe.boot.sitename}`;
	}

	subscribe(task_id, opts) {
		this.emit("task_subscribe", task_id);
		this.emit("progress_subscribe", task_id);

		this.open_tasks[task_id] = opts;
	}
	task_subscribe(task_id) {
		this.emit("task_subscribe", task_id);
	}
	task_unsubscribe(task_id) {
		this.emit("task_unsubscribe", task_id);
	}
	doctype_subscribe(doctype) {
		this.emit("doctype_subscribe", doctype);
	}
	doctype_unsubscribe(doctype) {
		this.emit("doctype_unsubscribe", doctype);
	}
	doc_subscribe(doctype, docname) {
		if (frappe.flags.doc_subscribe) {
			console.log("throttled");
			return;
		}
		if (this.open_docs.has(`${doctype}:${docname}`)) {
			return;
		}

		frappe.flags.doc_subscribe = true;

		// throttle to 1 per sec
		setTimeout(function () {
			frappe.flags.doc_subscribe = false;
		}, 1000);

		this.emit("doc_subscribe", doctype, docname);
		this.open_docs.add(`${doctype}:${docname}`);
	}
	doc_unsubscribe(doctype, docname) {
		this.emit("doc_unsubscribe", doctype, docname);
		return this.open_docs.delete(`${doctype}:${docname}`);
	}
	doc_open(doctype, docname) {
		this.emit("doc_open", doctype, docname);
	}
	doc_close(doctype, docname) {
		this.emit("doc_close", doctype, docname);
	}
	setup_listeners() {
		this.socket.on("task_status_change", (data) => {
			this.process_response(data, data.status.toLowerCase());
		});
		this.socket.on("task_progress", (data) => {
			this.process_response(data, "progress");
		});
	}
	process_response(data, method) {
		if (!data) {
			return;
		}

		// success
		let opts = this.open_tasks[data.task_id];
		if (!opts) {
			return;
		}
		if (opts[method]) {
			opts[method](data);
		}

		// "callback" is std frappe term
		if (method === "success") {
			if (opts.callback) opts.callback(data);
		}

		// always
		frappe.request.cleanup(opts, data);
		if (opts.always) {
			opts.always(data);
		}

		// error
		if (data.status_code && data.status_code > 400 && opts.error) {
			opts.error(data);
		}
	}

	publish(event, message) {
		if (this.socket) {
			this.emit(event, message);
		}
	}
}

frappe.realtime = new RealTimeClient();

// backward compatbility
frappe.socketio = frappe.realtime;
