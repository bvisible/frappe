//// Neoffice — added file (no upstream equivalent). The sentinel of the windows (design system §6.18, 17.11.2026): on a
//// site whose site_config holds `neo_dialog_sentinel: 1` (our test instance; off everywhere else, nothing is sent), every
//// window anyone opens is measured against the model of dialog_model.js and its sighting sent once per page and per
//// window to frappe.desk.dialog_sentinel.record, which keeps it for the daily report of the departures. Jérémy, 17.11:
//// « je ne vais pas passer sur tous les dialogues […] trouver une solution pour tester l'intégralité, puis s'assurer que
//// par la suite, ce soit solide ». It also notes a veil drawn by hand (a fixed layer over the whole screen that is not a
//// window of the model), which no window of the model is. It measures, it never changes a window, and it never throws.

frappe.provide("frappe.ui.dialog_sentinel");

(function () {
	const D = frappe.ui.Dialog;
	if (!D || D.prototype.__dialog_sentinel) return;
	const P = D.prototype;
	const S = frappe.ui.dialog_sentinel;
	const on = () => !!(frappe.boot && frappe.boot.neo_dialog_sentinel);
	const NOT_VALUES = new Set([
		"HTML",
		"Button",
		"Section Break",
		"Column Break",
		"Tab Break",
		"Heading",
		"Fold",
	]);
	const VAGUE = /^(ok|valider|submit|soumettre|confirm|confirmer|oui|yes|non|no)$/i;
	const WIDTHS = [440, 560, 800, 1140];
	const sent = new Set();

	// Where the window was opened from: the first frame of the stack that is not Frappe's own window code.
	function opener(stack) {
		const lines = String(stack || "")
			.split("\n")
			.slice(1);
		for (const l of lines) {
			if (
				/dialog(_model|_sentinel)?\.js|desk\.bundle|messages\.js|libs\.bundle|jquery/.test(
					l
				)
			)
				continue;
			const m = l.match(/(\/assets\/[^\s)]+?|https?:\/\/[^\s)]+?)(\?[^:\s)]*)?:(\d+):\d+/);
			if (m) return m[1].replace(/^https?:\/\/[^/]+/, "") + ":" + m[3];
		}
		return "";
	}
	const token = (name) =>
		parseFloat(getComputedStyle(document.body).getPropertyValue(name)) || null;
	const text_of = ($b) => {
		const el = $b && $b[0];
		if (!el) return "";
		const c = el.cloneNode(true);
		c.querySelectorAll(".neo-kbd").forEach((k) => k.remove());
		return (c.textContent || "").replace(/\s+/g, " ").trim();
	};
	const shown = (el) => {
		if (!el) return false;
		const r = el.getBoundingClientRect();
		return r.width > 0 && r.height > 0 && getComputedStyle(el).visibility !== "hidden";
	};

	function family(d, inputs) {
		const M = frappe.ui.dialog_model || {};
		if (d.__dlg_question) return "question";
		if (M.is_wizard && M.is_wizard(d)) return "wizard";
		if (!inputs) return d.confirm_dialog || d === frappe.msg_dialog ? "message" : "reading";
		return inputs <= 4 ? "entry" : "form";
	}

	function measure(d) {
		const W = d.$wrapper && d.$wrapper[0];
		if (!W || !shown(W.querySelector(".modal-content"))) return null;
		const dlg = W.querySelector(".modal-dialog"),
			content = W.querySelector(".modal-content");
		const cr = content.getBoundingClientRect();
		const title = W.querySelector(".modal-title");
		const eyebrow = W.querySelector(".modal-eyebrow:not(.hide)");
		const first = eyebrow && shown(eyebrow) && eyebrow.textContent.trim() ? eyebrow : title;
		const fields = (d.fields || []).filter(
			(f) => f && f.fieldtype && !NOT_VALUES.has(f.fieldtype) && !f.hidden
		);
		const fam = family(d, fields.length);
		const $p = d.get_primary_btn && d.get_primary_btn();
		const primary = $p && $p[0] && shown($p[0]) ? $p[0] : null;
		const label = primary ? text_of($p) : "";
		const tcs = title && getComputedStyle(title);
		const footer = W.querySelector(".modal-footer");
		const btns = footer ? [...footer.querySelectorAll(".btn")].filter(shown) : [];
		const tops = btns.map((b) => Math.round(b.getBoundingClientRect().top));
		const width = Math.round((dlg || content).getBoundingClientRect().width);
		const out = {
			title: ((title && title.textContent) || "").replace(/\s+/g, " ").trim().slice(0, 120),
			eyebrow: eyebrow ? eyebrow.textContent.trim().slice(0, 60) : "",
			route: (frappe.get_route_str() || "").split("/").slice(0, 2).join("/"),
			opener: d.__sentinel_opener || "",
			family: fam,
			inputs: fields.length,
			size: d.size || "",
			width,
			air: first
				? {
						top: Math.round(first.getBoundingClientRect().top - cr.top),
						left: title
							? Math.round(title.getBoundingClientRect().left - cr.left)
							: null,
				  }
				: null,
			title_font: tcs
				? {
						ff: tcs.fontFamily.split(",")[0].replace(/["']/g, "").trim(),
						fs: parseFloat(tcs.fontSize),
				  }
				: null,
			label: label.slice(0, 60),
			keys: !!(primary && primary.querySelector(".neo-kbd")),
			model: !!d.__dlg_shell,
			centred: Math.abs(cr.left + cr.width / 2 - innerWidth / 2) <= 4,
			footer_lines: new Set(tops.map((t) => Math.round(t / 6))).size,
			viewport: [innerWidth, innerHeight],
		};
		out.departures = departures(out);
		return out;
	}

	// The departures from the model (design system §6.18), each named by the id the daily report counts.
	function departures(m) {
		const out = [];
		const near = (a, b, t) => a != null && b != null && Math.abs(a - b) <= t;
		const wide = m.viewport[0] > 900;
		if (!m.model) out.push("not-model");
		if (wide && !WIDTHS.some((w) => near(m.width, w, 3))) out.push("width");
		if (wide && (m.family === "question" || m.family === "message") && m.width > 443)
			out.push("size-family");
		if (wide && m.family === "form" && m.width < 797) out.push("size-family");
		if (wide && m.air && (!near(m.air.top, 32, 3) || !near(m.air.left, 32, 3)))
			out.push("air");
		if ((m.family === "entry" || m.family === "form" || m.family === "wizard") && !m.eyebrow)
			out.push("eyebrow");
		if (m.label && VAGUE.test(m.label) && m.family !== "message") out.push("verb");
		if (m.label && !m.keys && !matchMedia("(hover: none)").matches) out.push("keys");
		const h2 = token("--ws-h2");
		if (
			m.title_font &&
			(!/forum/i.test(m.title_font.ff) || (h2 && !near(m.title_font.fs, h2, 0.6)))
		)
			out.push("title");
		if (wide && !m.centred) out.push("centre");
		if (wide && m.footer_lines > 1) out.push("footer");
		return out;
	}

	function send(kind, data) {
		const key =
			kind +
			"|" +
			(data.title || data.cls || "") +
			"|" +
			(data.opener || "") +
			"|" +
			(data.departures || []).join(",");
		if (sent.has(key)) return;
		sent.add(key);
		frappe
			.xcall("frappe.desk.dialog_sentinel.record", {
				sighting: JSON.stringify({ kind, ...data }),
			})
			.catch(() => {});
	}
	S.measure = measure;
	S.departures = departures;

	const make = P.make;
	P.make = function () {
		if (on()) {
			try {
				this.__sentinel_opener = opener(new Error().stack);
			} catch (e) {
				// never in the way of a window
			}
		}
		return make.apply(this, arguments);
	};
	const show = P.show;
	P.show = function () {
		const r = show.apply(this, arguments);
		if (on()) {
			// A window is drawn after its fade (about a second on the desk): it is measured once it stands, 3 s at most.
			const d = this;
			const started = Date.now();
			const look = () => {
				try {
					const W = d.$wrapper && d.$wrapper[0];
					const standing = W && d.display && parseFloat(getComputedStyle(W).opacity) > 0.95;
					const m = standing ? measure(d) : null;
					if (m) return send("window", m);
				} catch (e) {
					return; // never in the way of a window
				}
				if (d.display !== false && Date.now() - started < 3000) setTimeout(look, 250);
			};
			setTimeout(look, 450);
		}
		return r;
	};

	// A veil drawn by hand: a layer fixed over (nearly) the whole screen, with a colour or a blur, above the page, that
	// is not a window of the model (nor its backdrop, nor the cockpit's own frame).
	function watch_veils() {
		if (!window.MutationObserver) return;
		const seen = new WeakSet();
		const check = (el) => {
			if (!(el instanceof HTMLElement) || seen.has(el)) return;
			seen.add(el);
			if (el.closest(".modal, .modal-backdrop, .dlg-model, .neocockpit, .nc-shell")) return;
			const cs = getComputedStyle(el);
			if (cs.position !== "fixed" || cs.display === "none" || cs.visibility === "hidden")
				return;
			const r = el.getBoundingClientRect();
			if (r.width < innerWidth * 0.85 || r.height < innerHeight * 0.85) return;
			if ((parseInt(cs.zIndex, 10) || 0) < 100 || cs.pointerEvents === "none") return;
			const bg = cs.backgroundColor.match(/rgba?\(([^)]+)\)/);
			const alpha = bg
				? bg[1].split(",")[3] !== undefined
					? parseFloat(bg[1].split(",")[3])
					: 1
				: 0;
			if (alpha < 0.1 && (!cs.backdropFilter || cs.backdropFilter === "none")) return;
			send("veil", {
				cls: ((el.className && el.className.toString()) || el.tagName).slice(0, 80),
				id: el.id || "",
				route: (frappe.get_route_str() || "").split("/").slice(0, 2).join("/"),
				departures: ["hand-made-veil"],
			});
		};
		new MutationObserver((list) => {
			if (!on()) return;
			for (const rec of list)
				for (const n of rec.addedNodes)
					if (n.nodeType === 1) setTimeout(() => check(n), 300);
		}).observe(document.body, { childList: true, subtree: true });
	}
	$(document).on("app_ready", () => on() && watch_veils());

	P.__dialog_sentinel = true;
})();
