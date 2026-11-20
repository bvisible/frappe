//// Neoffice — added file (no upstream equivalent). One model for every window of the desk: design system §6.18
//// (bvisible/neoffice-design-system, 16.11.2026). Jérémy, 16.11: « toujours avoir le même type de dialogue […] quitte à
//// vraiment le mettre dans frappe pour que on le tienne tout le temps la même chose ». Measured that day on osiris: 482
//// frappe.ui.Dialog in our apps, 13 widths, 14 step bars for 16 wizards, 12 wizards that a click beside closed, Escape in a
//// field that asked or did nothing. Imported by desk.bundle.js right after dialog.js, so every frappe.ui.Dialog (ours,
//// ERPNext's, HRMS's) gets, without a line of its own:
////   - the shell: centred, an eyebrow above the title (`eyebrow`, `set_eyebrow`), footer links at the left
////     (`add_footer_link`), four sizes (small / default / large / extra-large = 440 / 560 / 800 / 1140, drawn by the theme);
////   - steps (form A of the design system: named pills under the title, « ← Retour », « Suivant → »): `steps` with fields,
////     or `set_steps` / `go_to_step` / `set_back` for a wizard that draws its own content;
////   - Nora's place under the title and the steps (`nora_about`, `set_nora`, 20.11.2026, neoffice-maintenance#2836);
////   - the keys, shown after the buttons' labels: ↵ the ink action, ⌘↵ / Ctrl+↵ from anywhere, Esc to leave, ⇧↵ « Retour »;
////   - leaving (moved here from neoffice_theme's « Dialog guard », #1191, 05.10): untouched, a click beside or Escape closes;
////     changed, they ask « Abandonner les modifications ? »; the cross closes (Jérémy, 05.10: « La croix, ben non »). A
////     wizard (18.10) is never left by a click beside or Escape; its cross asks where to go.
//// The look (air, sizes, step bar, keys) is the theme's (neoffice_theme/public/css/dialog_model.css, tokens --dlg-*).

frappe.provide("frappe.ui.dialog_model");

(function () {
	const D = frappe.ui.Dialog;
	if (!D || D.prototype.__dialog_model) return;
	const P = D.prototype;
	const M = frappe.ui.dialog_model;

	// Escape or a click beside first closes what is open inside the window: a list, a calendar, a menu.
	const POPUP_OPEN = ".awesomplete > ul:not([hidden]), .datepicker.active, .dropdown-menu.show";
	// What a person types into: a key aimed at one of these outside the window belongs to it, not to the window.
	const EDITABLE = "input, textarea, select, [contenteditable], [contenteditable] *";
	const NOT_VALUES = new Set([
		"HTML",
		"Button",
		"Section Break",
		"Column Break",
		"Tab Break",
		"Heading",
		"Fold",
	]);
	const MULTILINE = new Set([
		"Small Text",
		"Text",
		"Long Text",
		"Text Editor",
		"Markdown Editor",
		"HTML Editor",
		"Code",
		"JSON",
	]);
	// A wizard drawn by hand is known by its step bar (18.10: no common class, a common shape).
	const STEP_BAR =
		'[class*="-steps"], [class*="-stepper"], [class*="wizard-step"], .modal-steps:not(.hide)';

	const is_mac = () =>
		/Mac|iPhone|iPad|iPod/.test(navigator.platform || navigator.userAgent || "");
	M.keys = {
		enter: "↵",
		force: () => (is_mac() ? "⌘↵" : "Ctrl+↵"),
		esc: "Esc",
		back: "⇧↵",
	};
	const esc_html = (t) => frappe.utils.escape_html(t == null ? "" : String(t));
	const dom = (w) => (w && w.jquery ? w[0] : w);
	const visible = (el) => {
		if (!el) return false;
		const r = el.getBoundingClientRect();
		return r.width > 0 && r.height > 0;
	};
	function popup_open() {
		return Array.prototype.some.call(document.querySelectorAll(POPUP_OPEN), visible);
	}
	function set_key($el, key) {
		if (!$el || !$el.length) return;
		$el.children(".neo-kbd").remove();
		if (key)
			$el.append('<span class="neo-kbd" aria-hidden="true">' + esc_html(key) + "</span>");
	}

	// ---- the shell ------------------------------------------------------------------------------------------------------

	function shell(d) {
		if (d.__dlg_shell || !d.$wrapper) return;
		d.__dlg_shell = true;
		d.$wrapper.addClass("dlg-model").data("dlg-model", d);
		d.$wrapper.find(".modal-dialog").addClass("modal-dialog-centered");
		const $ts = d.header.find(".title-section");
		if (!$ts.children(".modal-eyebrow").length) {
			$('<div class="modal-eyebrow hide"></div>').prependTo($ts);
		}
		if (!d.footer.children(".modal-footer-links").length) {
			$('<div class="modal-footer-links"></div>').prependTo(d.footer);
		}
		if (d.$wrapper.find(".modal-steps").length === 0) {
			$('<div class="modal-steps hide" role="list"></div>').insertAfter(d.header);
		}
		d.get_close_btn().attr({ title: __("Close") + " (Esc)", "aria-label": __("Close") });
		if (d.eyebrow) d.set_eyebrow(d.eyebrow);
		// A window that rewrites its ink button (.text(), .html()) takes its key off with it: the key comes back.
		const p = d.get_primary_btn()[0];
		if (p && window.MutationObserver) {
			new MutationObserver(function () {
				const $p = $(p);
				if (!$p.hasClass("hide") && !$p.children(".neo-kbd").length && $p.text().trim()) {
					set_key($p, d.key_hint());
				}
			}).observe(p, { childList: true });
		}
	}

	P.set_eyebrow = function (text) {
		this.eyebrow = text || "";
		const $e = this.header && this.header.find(".modal-eyebrow");
		if (!$e || !$e.length) return;
		$e.text(this.eyebrow).toggleClass("hide", !this.eyebrow);
	};

	// The size of a window already built (design system 6.18: small / default / large / extra-large = 440 / 560 / 800 /
	// 1140): Frappe gives a size at construction only, and some windows are built by Frappe itself (a quick entry, the
	// email composer), then made wider. A size named here is the model's, never a width of one's own.
	P.set_size = function (size) {
		const SIZES = { small: "modal-sm", large: "modal-lg", "extra-large": "modal-xl" };
		this.size = SIZES[size] ? size : "";
		const $d = this.$wrapper && this.$wrapper.find(".modal-dialog");
		if (!$d || !$d.length) return;
		$d.removeClass("modal-sm modal-lg modal-xl");
		if (SIZES[size]) $d.addClass(SIZES[size]);
	};

	// A link at the left of the footer (« Ouvrir le formulaire complet », « Utiliser un modèle »).
	P.add_footer_link = function (label, action, key) {
		this.footer.removeClass("hide");
		const $l = $('<button type="button" class="btn btn-link dlg-link"></button>').text(label);
		set_key($l, key);
		this.footer.children(".modal-footer-links").append($l);
		if (action) $l.on("click", action);
		return $l;
	};

	// ---- steps (form A) -------------------------------------------------------------------------------------------------

	// Declarative steps: [{ label, title?, fields, validate?(values, dialog), answer?(values) }]. Each step's fields follow a
	// Section Break of their own; only the current step's sections show; the primary button is « Suivant → » until the last
	// step, where it is the dialog's own primary_action_label and runs primary_action with every value.
	function prepare_steps(d) {
		if (
			!Array.isArray(d.steps) ||
			!d.steps.length ||
			!d.steps.every((s) => s && Array.isArray(s.fields))
		) {
			return;
		}
		d.__dlg_steps = d.steps.map((s, i) => ({
			label: s.label || String(i + 1),
			title: s.title || null,
			validate: s.validate || null,
			answer: s.answer || null,
			fieldnames: s.fields.map((f) => f && f.fieldname).filter(Boolean),
		}));
		d.fields = [];
		d.steps.forEach((s, i) => {
			d.fields.push({ fieldtype: "Section Break", fieldname: "__step_" + i });
			s.fields.forEach((f) => d.fields.push(f));
		});
		d.__dlg_final = { label: d.primary_action_label, action: d.primary_action };
		d.__dlg_title = d.title;
	}

	function step_sections(d, i) {
		const names = ["__step_" + i].concat(d.__dlg_steps[i].fieldnames);
		const out = new Set();
		names.forEach((n) => {
			const f = d.fields_dict[n];
			const w = f && (dom(f.wrapper) || dom(f.$wrapper));
			const sec = w && w.closest && w.closest(".form-section");
			if (sec) out.add(sec);
		});
		return out;
	}

	function step_missing(d, i) {
		const out = [];
		d.__dlg_steps[i].fieldnames.forEach((n) => {
			const f = d.fields_dict[n];
			if (!f || !f.df || !f.df.reqd || NOT_VALUES.has(f.df.fieldtype)) return;
			const w = dom(f.wrapper) || dom(f.$wrapper);
			if (!visible(w)) return;
			const v = f.get_value ? f.get_value() : null;
			if (v === undefined || v === null || v === "" || (Array.isArray(v) && !v.length)) {
				out.push({ fieldname: n, label: __(f.df.label || n) });
			}
		});
		return out;
	}

	function show_missing(d, missing) {
		// The theme's « what is missing » (§6.16) when it is there, else the fields marked in place.
		const keep = window.neoffice && window.neoffice.keep;
		if (keep && d.__neo_ss && keep.on_dialog_attempt)
			return keep.on_dialog_attempt(d, missing);
		missing.forEach((m) => {
			const f = d.fields_dict[m.fieldname];
			f && f.$wrapper && f.$wrapper.addClass("has-error");
		});
		const first = d.fields_dict[missing[0].fieldname];
		first && first.$input && first.$input.focus();
	}

	// The values as they are on screen. Not get_values(): for a required field whose typing is not yet in its model (the
	// control sets it a moment after the keystrokes), it puts the input back to the model's empty value.
	const TYPED = new Set([
		"Data",
		"Int",
		"Float",
		"Currency",
		"Percent",
		"Small Text",
		"Text",
		"Long Text",
		"Link",
		"Select",
	]);
	function values_now(d) {
		const out = {};
		Object.keys(d.fields_dict || {}).forEach((k) => {
			const f = d.fields_dict[k];
			if (!f || !f.df || NOT_VALUES.has(f.df.fieldtype) || typeof f.get_value !== "function")
				return;
			try {
				const v = f.get_value();
				if (v !== undefined && v !== null && v !== "") out[k] = v;
			} catch (e) {
				// a control that cannot say its value is left out
			}
		});
		return out;
	}
	// What is typed goes into the controls' model before the dialog's own get_values() reads it.
	function commit_typing(d) {
		return Promise.all(
			Object.keys(d.fields_dict || {}).map((k) => {
				const f = d.fields_dict[k];
				if (
					!f ||
					!f.df ||
					!TYPED.has(f.df.fieldtype) ||
					typeof f.get_input_value !== "function"
				)
					return null;
				try {
					const v = f.get_input_value();
					if (v === undefined || v === null || v === f.value || (v === "" && !f.value))
						return null;
					return f.set_value(v);
				} catch (e) {
					return null;
				}
			})
		);
	}
	M.values_now = values_now;

	P.step_next = function () {
		const d = this;
		const S = d.__dlg_steps;
		if (!S || d.__dlg_stepping) return;
		const i = d.__dlg_step || 0;
		const missing = step_missing(d, i);
		if (missing.length) return show_missing(d, missing);
		if (S[i].validate && S[i].validate.call(d, values_now(d), d) === false) return;
		if (i < S.length - 1) return d.go_to_step(i + 1);
		d.__dlg_stepping = true;
		commit_typing(d).then(() => {
			d.__dlg_stepping = false;
			const all = d.get_values();
			if (!all) return;
			d.primary_action_fulfilled = true;
			d.__dlg_final.action && d.__dlg_final.action.call(d, all);
		});
	};

	// A wizard that draws its own content: set_steps(["Client", "Factures"…] or [{ label }], current) puts the bar.
	P.set_steps = function (steps, current) {
		this.__dlg_bar = (steps || []).map((s) => (typeof s === "string" ? { label: s } : s));
		this.neo_wizard = this.neo_wizard === false ? false : true;
		render_bar(this, current || 0);
		refresh_keys(this);
	};

	P.go_to_step = function (i, opts) {
		const d = this;
		if (d.__dlg_steps) {
			i = Math.max(0, Math.min(i, d.__dlg_steps.length - 1));
			d.__dlg_step = i;
			const show = step_sections(d, i);
			d.__dlg_steps.forEach((s, k) => {
				if (k === i) return;
				step_sections(d, k).forEach((sec) => !show.has(sec) && sec.classList.add("hide"));
			});
			show.forEach((sec) => sec.classList.remove("hide"));
			const last = i === d.__dlg_steps.length - 1;
			d.set_title(d.__dlg_steps[i].title || d.__dlg_title || "");
			const $p = d
				.get_primary_btn()
				.removeClass("hide")
				.text(last ? d.__dlg_final.label || __("Create") : __("Next") + " →");
			d.footer.removeClass("hide");
			$p.off("click").on("click", () => d.step_next());
			d.set_back(i > 0 ? () => d.go_to_step(i - 1) : null);
			render_bar(d, i);
			refresh_keys(d);
			if (!(opts && opts.no_focus)) {
				const first = Array.from(show)
					.map((sec) =>
						sec.querySelector(
							"input:not([type=hidden]):not([readonly]), textarea, select"
						)
					)
					.find(Boolean);
				first && window.setTimeout(() => first.focus(), 30);
			}
			d.on_step && d.on_step(i);
			return;
		}
		render_bar(d, i);
		refresh_keys(d);
	};

	// « ← Retour » at the left of the footer, and ⇧↵. null takes it away.
	P.set_back = function (fn, label) {
		const d = this;
		d.__dlg_back = fn || null;
		let $b = d.footer.find(".dlg-back");
		if (!fn) return $b.remove();
		if (!$b.length) {
			$b = $(
				'<button type="button" class="btn btn-link dlg-link dlg-back"></button>'
			).prependTo(d.footer.children(".modal-footer-links"));
		}
		$b.text(label || "← " + __("Back"));
		set_key($b, M.keys.back);
		$b.off("click").on("click", () => d.__dlg_back && d.__dlg_back());
		d.footer.removeClass("hide");
	};

	function render_bar(d, current) {
		const bar = d.__dlg_steps || d.__dlg_bar;
		const $bar = d.$wrapper.find(".modal-steps");
		if (!bar || !bar.length) return $bar.addClass("hide").empty();
		d.__dlg_bar_current = current;
		const values = d.__dlg_steps ? values_now(d) : {};
		const html = bar
			.map((s, i) => {
				const state = i < current ? "done" : i === current ? "now" : "todo";
				let label = s.label;
				if (state === "done") {
					try {
						const a =
							s.answer &&
							(typeof s.answer === "function" ? s.answer(values) : s.answer);
						if (a) label = a;
					} catch (e) {
						// a step that cannot name its answer keeps its name
					}
				}
				const disc = state === "done" ? "✓" : String(i + 1);
				const go = state === "done" && (d.__dlg_steps || d.on_step_click);
				return (
					'<button type="button" role="listitem" class="dlg-step is-' +
					state +
					'"' +
					(go ? ' data-step="' + i + '"' : " disabled") +
					(state === "now" ? ' aria-current="step"' : "") +
					'><span class="dlg-step-disc" aria-hidden="true">' +
					disc +
					'</span><span class="dlg-step-label">' +
					esc_html(label) +
					"</span></button>"
				);
			})
			.join("");
		const count =
			'<span class="dlg-step-count">' +
			esc_html(__("Step {0} of {1}", [current + 1, bar.length])) +
			" · " +
			esc_html((bar[current] || {}).label || "") +
			'</span><span class="dlg-step-track"><span style="width:' +
			Math.round(((current + 1) / bar.length) * 100) +
			'%"></span></span>';
		$bar.html(html + count).removeClass("hide");
		$bar.find("[data-step]")
			.off("click")
			.on("click", function () {
				const i = parseInt(this.dataset.step, 10);
				if (d.__dlg_steps) d.go_to_step(i);
				else d.on_step_click && d.on_step_click(i);
			});
	}

	// ---- Nora's place ---------------------------------------------------------------------------------------------------

	//// Neoffice — Nora's place in every window (design system 6.18, neoffice-maintenance#2836, 20.11.2026). Jérémy: « dans
	//// la structure même du dialogue, on peut dire, voilà, c'est ici que Nora peut échanger des données […] compatible aussi
	//// bien pour la vue simple que la vue avancée ». Under the title and the step bar, above the content. The window says what
	//// it is about (`nora_about(doctype, name, context)`); Nora answers there with what she already says of that thing
	//// elsewhere. The source is an app's (`frappe.ui.dialog_model.nora_source(doctype, name, context, dialog)`, a promise of
	//// what to say or null): Frappe knows no Nora, neoffice_theme registers its answering bar. Nothing to say: no place.
	//// `set_nora(said)` draws an answer given by hand, `set_nora(null)` takes it away. What is said:
	////   { tone: "attention" | "info", sentence and detail: HTML with <b> only, items: [{ label, meta, amount, href, tone }]
	////     unfolded in the place (each opens in a new tab: the window and what was typed stay), unfold: the label of the
	////     gesture that unfolds them, more: { label, href }, actions: [{ label, action(dialog) }] }.
	//// Nothing in it is a value of the window: a click there is never « something typed » for the leaving rules.
	const NOT_BOLD = /<(?!\/?b>)[^>]*>/gi;
	const only_bold = (html) =>
		String(html || "")
			.replace(NOT_BOLD, "")
			.trim();
	// Only a page of the desk: an item never leads anywhere else (javascript:, another site).
	const desk_route = (href) =>
		typeof href === "string" && /^\/app\/[^\s"'<>]*$/.test(href) ? href : null;

	function nora_place(d) {
		if (!d.$wrapper) return null;
		let $n = d.$wrapper.find(".modal-nora");
		if (!$n.length) {
			$n = $('<div class="modal-nora hide" role="status" aria-live="polite"></div>');
			const $steps = d.$wrapper.find(".modal-steps");
			$n.insertAfter($steps.length ? $steps : d.header);
		}
		return $n;
	}

	function nora_item(item) {
		const href = desk_route(item.href);
		const inner =
			"<b>" +
			esc_html(item.label) +
			"</b>" +
			'<span class="modal-nora-meta">' +
			esc_html(item.meta || "") +
			"</span>" +
			'<span class="modal-nora-amount">' +
			esc_html(item.amount || "") +
			"</span>";
		const tone = ' data-tone="' + esc_html(item.tone || "") + '"';
		if (!href) return '<span class="modal-nora-item"' + tone + ">" + inner + "</span>";
		return (
			'<a class="modal-nora-item"' +
			tone +
			' href="' +
			esc_html(href) +
			'" target="_blank" rel="noopener">' +
			inner +
			'<span class="modal-nora-out" aria-hidden="true">↗</span></a>'
		);
	}

	P.set_nora = function (said) {
		const d = this;
		const $n = nora_place(d);
		if (!$n) return;
		const sentence = said ? only_bold(said.sentence) : "";
		if (!sentence) {
			d.nora_said = null;
			$n.addClass("hide").empty().removeAttr("data-tone");
			return;
		}
		d.nora_said = said;
		const items = (said.items || []).filter((i) => i && i.label);
		const more = said.more && desk_route(said.more.href) ? said.more : null;
		const unfold = said.unfold || __("See the details");
		let list = "";
		if (items.length) {
			list =
				'<div class="modal-nora-list" hidden>' +
				items.map(nora_item).join("") +
				(more
					? '<a class="modal-nora-more" href="' +
					  esc_html(more.href) +
					  '" target="_blank" rel="noopener">' +
					  esc_html(more.label) +
					  " ↗</a>"
					: "") +
				"</div>";
		}
		const actions = (said.actions || []).filter((a) => a && a.label);
		const detail = only_bold(said.detail);
		$n.attr("data-tone", said.tone === "attention" ? "attention" : "info").html(
			'<span class="modal-nora-orb" aria-hidden="true"></span>' +
				'<div class="modal-nora-body">' +
				'<p class="modal-nora-say">' +
				sentence +
				"</p>" +
				list +
				(items.length || actions.length || detail
					? '<div class="modal-nora-acts">' +
					  (items.length
							? '<button type="button" class="modal-nora-act modal-nora-unfold" aria-expanded="false">' +
							  esc_html(unfold) +
							  "</button>"
							: "") +
					  actions
							.map(
								(a, k) =>
									'<button type="button" class="modal-nora-act" data-act="' +
									k +
									'">' +
									esc_html(a.label) +
									"</button>"
							)
							.join("") +
					  (detail ? '<span class="modal-nora-detail">' + detail + "</span>" : "") +
					  "</div>"
					: "") +
				"</div>"
		);
		$n.attr("aria-label", "Nora").removeClass("hide");
		$n.find(".modal-nora-unfold").on("click", function () {
			const $list = $n.find(".modal-nora-list");
			const open = $list.prop("hidden");
			$list.prop("hidden", !open);
			$(this)
				.attr("aria-expanded", open ? "true" : "false")
				.text(open ? __("Hide") : unfold);
		});
		$n.find("[data-act]").on("click", function () {
			const a = actions[parseInt(this.dataset.act, 10)];
			if (a && typeof a.action === "function") a.action(d);
		});
	};

	// The window says what it is about; the last thing asked wins (a customer changed twice in a row).
	P.nora_about = function (doctype, name, context) {
		const d = this;
		const ask = (d.__nora_ask = (d.__nora_ask || 0) + 1);
		const source = M.nora_source;
		if (!doctype || !name || typeof source !== "function") {
			d.set_nora(null);
			return Promise.resolve(null);
		}
		return Promise.resolve()
			.then(() => source(doctype, name, context || {}, d))
			.then((said) => {
				if (d.__nora_ask === ask) d.set_nora(said || null);
				return said || null;
			})
			.catch(() => {
				// Nora silent is a window as it was, never a broken one.
				if (d.__nora_ask === ask) d.set_nora(null);
				return null;
			});
	};

	// ---- keys ---------------------------------------------------------------------------------------------------------

	function has_multiline(d) {
		return Object.keys(d.fields_dict || {}).some((k) => {
			const f = d.fields_dict[k];
			if (!f || !f.df || !MULTILINE.has(f.df.fieldtype)) return false;
			return visible(dom(f.wrapper) || dom(f.$wrapper));
		});
	}
	// The key shown on the ink button: the theme's save_state reads it too.
	P.key_hint = function () {
		return has_multiline(this) ? M.keys.force() : M.keys.enter;
	};
	function refresh_keys(d) {
		const $p = d.get_primary_btn();
		if ($p.length && !$p.hasClass("hide")) set_key($p, d.key_hint());
		if (d.__dlg_question) set_key(d.get_secondary_btn(), M.keys.esc);
	}
	M.refresh_keys = refresh_keys;

	// ---- what « changed » means (from the theme's guard, 05.10) -------------------------------------------------------

	let pointer = { t: 0 };
	document.addEventListener(
		"pointerdown",
		function (e) {
			const el = e.target;
			pointer = {
				t: Date.now(),
				target: el,
				popup: popup_open(),
				scrollbar: !!(
					el &&
					el.clientWidth &&
					(e.offsetX >= el.clientWidth || e.offsetY >= el.clientHeight)
				),
			};
		},
		true
	);

	function values_of(d) {
		const out = {};
		try {
			Object.keys(d.fields_dict || {}).forEach(function (key) {
				const f = d.fields_dict[key];
				if (
					!f ||
					!f.df ||
					NOT_VALUES.has(f.df.fieldtype) ||
					typeof f.get_value !== "function"
				)
					return;
				const v = f.get_value();
				if (v === undefined || v === null || v === "") return;
				out[key] = v;
			});
			return JSON.stringify(out);
		} catch (e) {
			return "";
		}
	}
	function in_standard_field(d, el) {
		return (d.fields_list || []).some(function (f) {
			if (!f || !f.df || NOT_VALUES.has(f.df.fieldtype)) return false;
			const w = dom(f.wrapper) || dom(f.$wrapper);
			return !!(w && w.contains && w.contains(el));
		});
	}
	function changed(d) {
		const g = d.__neo_guard;
		if (!g) return false;
		return g.custom || (g.baseline != null && values_of(d) !== g.baseline);
	}
	function is_dirty(d) {
		if (d.neo_closable === true) return false;
		if (typeof d.neo_is_dirty === "function") {
			try {
				return !!d.neo_is_dirty();
			} catch (e) {
				return true;
			}
		}
		return changed(d);
	}
	function is_wizard(d) {
		if (d.neo_wizard === false) return false;
		if (d.neo_wizard === true || d.__dlg_steps || d.__dlg_bar) return true;
		const body = d.$wrapper && d.$wrapper.find(".modal-content")[0];
		return !!(body && Array.prototype.some.call(body.querySelectorAll(STEP_BAR), visible));
	}
	function title_of(d) {
		const plain = typeof d.title === "string" && d.title && d.title.indexOf("<") === -1;
		const t = plain ? d.title : $(d.header).find(".modal-title").text();
		return (t || "").replace(/\s+/g, " ").trim().slice(0, 80);
	}
	// Bootstrap 4 stacks a second modal under the first one's colour: both go above the topmost open window.
	function stack_above(q) {
		let top = 1050;
		$(".modal.show")
			.not(q.$wrapper)
			.each(function () {
				top = Math.max(top, parseInt(getComputedStyle(this).zIndex, 10) || 1050);
			});
		q.$wrapper.css("z-index", top + 20);
		const m = q.$wrapper.data("bs.modal");
		if (m && m._backdrop) $(m._backdrop).css("z-index", top + 10);
	}

	// ---- the questions ------------------------------------------------------------------------------------------------

	// A small window over the one it darkens; Enter is its ink action, Escape (or a click beside, or its cross) keeps the work.
	function question(opts) {
		const q = new frappe.ui.Dialog({
			title: opts.title,
			size: "small",
			primary_action_label: opts.primary,
			primary_action: () => {
				q.__dlg_answer = "primary";
				q.hide();
			},
			secondary_action_label: opts.keep,
			secondary_action: () => q.hide(),
		});
		q.__dlg_question = true;
		q.confirm_dialog = true; // ui/keyboard.js: Enter clicks the primary button
		q.neo_closable = true;
		q.$wrapper.addClass("neo-discard-question dlg-question");
		q.get_primary_btn().addClass("neo-discard-btn");
		if (opts.message)
			$('<p class="dlg-question-text"></p>').html(opts.message).appendTo(q.$body);
		(opts.extra || []).forEach((x) => {
			$('<button type="button" class="btn btn-secondary btn-sm"></button>')
				.text(x.label)
				.insertBefore(q.get_secondary_btn())
				.on("click", () => {
					q.__dlg_answer = x.key;
					q.hide();
				});
		});
		q.show();
		stack_above(q);
		refresh_keys(q);
		// No focus on a button: Enter is the ink action, Escape keeps the work (Jérémy, 16.11).
		q.$wrapper.one("shown.bs.modal", () =>
			q.$wrapper.find(".modal-content").attr("tabindex", "-1").trigger("focus")
		);
		return q;
	}

	function confirm_discard(d, opts) {
		opts = opts || {};
		const g = d.__neo_guard || (d.__neo_guard = {});
		if (g.asking) return g.asking;
		g.asking = new Promise(function (resolve) {
			const title = opts.title !== undefined ? opts.title : title_of(d);
			const message =
				opts.message ||
				d.neo_discard_message ||
				(title
					? __("The changes made in {0} will be lost.", ["« " + esc_html(title) + " »"])
					: __("The changes made here will be lost."));
			const q = question({
				title: __("Discard the changes?", null, "Dialog guard"),
				message: message,
				primary: __("Discard", null, "Dialog guard"),
				keep: __("Keep editing", null, "Dialog guard"),
			});
			q.onhide = function () {
				g.asking = null;
				resolve(q.__dlg_answer === "primary");
			};
		});
		return g.asking;
	}

	// The question of a wizard's cross (18.10): on a new document « Revenir à l'assistant », « Continuer sur la fiche vide »,
	// « Revenir au menu »; elsewhere « Revenir à l'assistant », « Quitter l'assistant ».
	function back_from_new(frm) {
		const here = frappe.get_route_str();
		const prev = (frappe.route_history || [])
			.slice()
			.reverse()
			.find(function (r) {
				const str = (r || []).join("/");
				return (
					str &&
					str !== here &&
					!(r[0] === "Form" && frm && frm.doc && r[2] === frm.doc.name) &&
					!(r[0] === "Form" && /^new(-|$)/.test(String(r[2] || "")))
				);
			});
		frappe.set_route(prev && prev.length ? prev : "");
	}
	function ask_leave_wizard(d) {
		if (d.__dlg_asking_leave) return;
		d.__dlg_asking_leave = true;
		const frm = d.__dlg_frm;
		const over_new = !!(frm && typeof frm.is_new === "function" && frm.is_new());
		const q = question({
			title: __("Leave the wizard?"),
			message: esc_html(__("Nothing has been created yet.")),
			primary: over_new ? __("Back to the menu") : __("Leave the wizard"),
			keep: __("Back to the wizard"),
			extra: over_new ? [{ key: "stay", label: __("Continue on the empty card") }] : [],
		});
		q.onhide = function () {
			d.__dlg_asking_leave = false;
			const a = q.__dlg_answer;
			if (!a) return;
			leave(d);
			if (a === "primary" && over_new) back_from_new(frm);
		};
	}

	// Bootstrap ignores hide() while a window is still fading in: the gesture waits for the end of the fade, it is never lost.
	function hide_now(d) {
		const m = d.$wrapper && d.$wrapper.data("bs.modal");
		if (m && m._isTransitioning) {
			// fading in: closed at the end of the fade; fading out: already going
			if (m._isShown) d.$wrapper.one("shown.bs.modal", () => d.hide());
			return;
		}
		d.hide();
	}
	function leave(d) {
		d.neo_leaving = true;
		hide_now(d);
	}
	function has_way_out(d) {
		const $x = d.get_close_btn();
		return $x.length > 0 && $x.is(":visible");
	}
	function request_leave(d) {
		const $x = d.get_close_btn();
		// A window that owns its closing: its cross decides (the form dialog, an Activities wizard).
		if ($x.attr("data-dismiss") !== "modal") {
			$x.trigger("click");
			return;
		}
		if (!is_dirty(d)) return leave(d);
		confirm_discard(d).then(function (discard) {
			if (discard) leave(d);
		});
	}

	function arm(d) {
		const m = d.$wrapper.data("bs.modal");
		const c = m && (m._config || m.options);
		if (c) {
			c.backdrop = "static";
			c.keyboard = false;
		}
		// ui/keyboard.js would cancel it on Escape besides.
		d.no_cancel_flag = true;
	}

	function install(d) {
		if (d.__neo_guard_on) return;
		d.__neo_guard_on = true;
		const w = d.$wrapper[0];
		const box = d.$wrapper.find(".modal-dialog")[0];
		const first_gesture = function (e) {
			const g = d.__neo_guard;
			if (!e.isTrusted || !g || g.baseline != null || !box || !box.contains(e.target))
				return;
			g.baseline = values_of(d);
		};
		w.addEventListener("pointerdown", first_gesture, true);
		w.addEventListener("keydown", first_gesture, true);
		w.addEventListener("beforeinput", first_gesture, true);
		const typed = function (e) {
			const g = d.__neo_guard;
			if (!g || !box || !box.contains(e.target)) return;
			if (!e.isTrusted && e.type !== "awesomplete-selectcomplete") return;
			if (!in_standard_field(d, e.target) || g.baseline == null) g.custom = true;
		};
		["input", "change", "paste", "drop", "awesomplete-selectcomplete"].forEach(function (t) {
			w.addEventListener(t, typed, true);
		});
		// A click beside: Bootstrap fires hidePrevented on a static window.
		d.$wrapper.on("hidePrevented.bs.modal", function (e) {
			if (e.target !== w) return;
			e.preventDefault(); // no shake: the model answers
			const g = d.__neo_guard;
			if (!g || g.asking || d.__dlg_question) return;
			if (
				pointer.target !== w ||
				Date.now() - pointer.t > 3000 ||
				pointer.popup ||
				pointer.scrollbar
			)
				return;
			if (is_wizard(d)) return; // 18.10: a wizard is never left by a click beside
			if (!has_way_out(d)) return;
			request_leave(d);
		});
		// A wizard's cross asks where to go.
		w.addEventListener(
			"click",
			function (e) {
				const x = e.target && e.target.closest && e.target.closest(".btn-modal-close");
				if (!x || !w.contains(x) || x.getAttribute("data-dismiss") !== "modal") return;
				if (!is_wizard(d) || d.__dlg_question) return;
				e.preventDefault();
				e.stopPropagation();
				ask_leave_wizard(d);
			},
			true
		);
		d.$wrapper.on("show.bs.modal shown.bs.modal", function () {
			arm(d);
		});
	}

	// ---- Escape and Enter, for the window on top ----------------------------------------------------------------------

	// The window on top, read on the screen: Bootstrap may announce « shown » (and Frappe set cur_dialog) seconds after the
	// window is drawn, and a key pressed meanwhile must not fall through.
	function top_dialog() {
		let top = null;
		let z = -1;
		// A window still fading in counts (Bootstrap shows it before it adds .show); one fading out does not.
		document.querySelectorAll(".modal.dlg-model").forEach(function (el) {
			const d = $(el).data("dlg-model");
			if (
				!d ||
				d.is_minimized ||
				d.neo_leaving ||
				!visible(el.querySelector(".modal-dialog"))
			)
				return;
			if (!el.classList.contains("show") && !(d.is_visible === true)) return;
			const zi = parseInt(getComputedStyle(el).zIndex, 10) || 0;
			if (zi >= z) {
				z = zi;
				top = d;
			}
		});
		return top;
	}
	document.addEventListener(
		"keydown",
		function (e) {
			if (e.defaultPrevented || e.isComposing) return;
			const d = top_dialog();
			if (!d || !d.__dlg_shell || d.static) return;
			const t = e.target;
			// Typing elsewhere (a field of a side panel) keeps its keys. A link or a button left focused behind the window
			// (a form that kept the focus while its window opened on load) does not: Escape and Enter are the window's.
			if (
				t &&
				t.closest &&
				!t.closest(".modal") &&
				t !== document.body &&
				t.matches &&
				t.matches(EDITABLE)
			)
				return;
			if (e.key === "Escape") {
				// Escape closes what is open inside the window first, and leaves a table cell or a row's form.
				if (popup_open() || document.querySelector(".grid-row-open")) return;
				if (t && t.closest && t.closest(".form-grid, .grid-row")) return;
				e.preventDefault();
				e.stopPropagation();
				if (d.__dlg_question) return hide_now(d); // the answer that keeps the work
				if (is_wizard(d)) return; // 18.10: nothing
				if (!has_way_out(d) || (d.__neo_guard && d.__neo_guard.asking)) return;
				request_leave(d);
				return;
			}
			if (e.key !== "Enter" || e.altKey) return;
			const multiline = !!(t && (t.tagName === "TEXTAREA" || t.isContentEditable));
			const $p = d.get_primary_btn();
			const can =
				$p.length &&
				!$p.hasClass("hide") &&
				!$p.prop("disabled") &&
				!$p.hasClass("disabled");
			if (e.metaKey || e.ctrlKey) {
				if (!can) return;
				e.preventDefault();
				e.stopPropagation();
				$p.trigger("click");
				return;
			}
			if (e.shiftKey) {
				if (multiline || !d.__dlg_back) return;
				e.preventDefault();
				e.stopPropagation();
				d.__dlg_back();
				return;
			}
			// A plain Enter in a text input is FieldGroup's (catch_enter_as_submit), on a button or a link the browser's.
			// What is left (the focus on the window itself, a question): the ink action, here and only here, so that
			// ui/keyboard.js, which knows a question only once Bootstrap says it is shown, does not click it a second time.
			if (multiline || (d.no_submit_on_enter && !d.confirm_dialog)) return;
			if (
				t &&
				t.closest &&
				t.closest("input, select, textarea, button, a, [role=button], .awesomplete")
			)
				return;
			if (!can) return;
			e.preventDefault();
			e.stopPropagation();
			$p.trigger("click");
		},
		true
	);

	// ---- wiring -------------------------------------------------------------------------------------------------------

	const make = P.make;
	P.make = function () {
		try {
			prepare_steps(this);
		} catch (e) {
			console.warn("[dialog model] steps", e);
		}
		const out = make.apply(this, arguments);
		try {
			shell(this);
		} catch (e) {
			console.warn("[dialog model] shell", e); // never block a window
		}
		return out;
	};

	const show = P.show;
	P.show = function () {
		if (this.static) return show.apply(this, arguments);
		// A new opening starts clean. Not `is_visible`: before the first show it is the layout's method of that name.
		if (this.display !== true || !this.__neo_guard) {
			this.__neo_guard = { baseline: null, custom: false, asking: null };
			this.neo_leaving = false;
			const f = window.cur_frm;
			this.__dlg_frm =
				this.frm ||
				(f &&
				typeof f.is_new === "function" &&
				f.is_new() &&
				frappe.get_route()[0] === "Form"
					? f
					: null);
		}
		const r = show.apply(this, arguments);
		try {
			shell(this);
			if (this.__dlg_steps && this.__dlg_step === undefined)
				this.go_to_step(0, { no_focus: true });
			install(this);
			arm(this);
			refresh_keys(this);
		} catch (e) {
			console.warn("[dialog model]", e); // never block a window
		}
		return r;
	};

	const set_primary_action = P.set_primary_action;
	P.set_primary_action = function () {
		const out = set_primary_action.apply(this, arguments);
		try {
			this.__dlg_shell && refresh_keys(this);
		} catch (e) {
			// the key is a hint, never a reason to fail
		}
		return out;
	};

	// ---- the messages and questions of messages.js, on the model ------------------------------------------------------

	const confirm = frappe.confirm;
	frappe.confirm = function (
		message,
		confirm_action,
		reject_action,
		primary_label,
		secondary_label
	) {
		const d = confirm.call(
			this,
			message,
			confirm_action,
			reject_action,
			primary_label || "Confirm",
			secondary_label || "Cancel"
		);
		try {
			d.$wrapper.find(".modal-dialog").addClass("modal-sm");
			d.__dlg_question = true;
			// A short question is the title (« Archiver ce client ? »), not a sentence under « Confirmer ».
			const text = typeof message === "string" ? message.trim() : "";
			if (text && text.indexOf("<") === -1 && text.length <= 110) {
				d.set_title(esc_html(text));
				d.$body.find(".frappe-confirm-message").remove();
			}
			refresh_keys(d);
		} catch (e) {
			// the question still works as Frappe drew it
		}
		return d;
	};

	const warn = frappe.warn;
	frappe.warn = function () {
		const d = warn.apply(this, arguments);
		try {
			d.$wrapper.find(".modal-dialog").addClass("modal-sm");
			// §6.16: never a red button, the action is ink.
			d.standard_actions
				.find(".btn-danger")
				.removeClass("btn-danger")
				.addClass("btn-primary");
			d.__dlg_question = true;
			d.confirm_dialog = true;
			refresh_keys(d);
		} catch (e) {
			// drawn as Frappe drew it
		}
		return d;
	};

	// The action of a prompt is named by its verb (design system 6.18), never Frappe's « Submit » (« Valider »): a prompt that
	// names none says « Continue ».
	const prompt = frappe.prompt;
	frappe.prompt = function (fields, callback, title, primary_label) {
		return prompt.call(this, fields, callback, title, primary_label || __("Continue"));
	};

	// A message has its « OK » (Jérémy, 05.11: a cross alone reads as a block), and Enter takes it.
	const msgprint = frappe.msgprint;
	const msgprint_model = function (msg) {
		const out = msgprint.apply(this, arguments);
		try {
			const d = out || frappe.msg_dialog;
			if (d && d.set_primary_action && d.$wrapper) {
				if (!d.has_primary_action) d.set_primary_action(__("OK"), () => d.hide());
				d.confirm_dialog = true;
				const wide =
					!!(msg && (msg.wide || msg.as_table)) || !!d.$body.find("table").length;
				d.$wrapper.find(".modal-dialog").toggleClass("modal-sm", !wide);
				refresh_keys(d);
			}
		} catch (e) {
			// a message is never blocked by its decoration
		}
		return out;
	};
	msgprint_model.__neo_ok = true; // neoffice_theme's own « OK » steps aside
	frappe.msgprint = msgprint_model;
	window.msgprint = msgprint_model;

	M.changed = changed;
	M.is_dirty = is_dirty;
	M.is_wizard = is_wizard;
	M.confirm_discard = confirm_discard;
	M.ask_leave_wizard = ask_leave_wizard;
	M.stack_above = stack_above;
	M.question = question;
	// The name the theme and the apps have called since 05.10 (#1191): wizard_parts.js, access_refusal.js.
	window.neoffice = window.neoffice || {};
	window.neoffice.dialogs = Object.assign(window.neoffice.dialogs || {}, {
		changed,
		is_dirty,
		confirm_discard,
		stack_above,
	});
	P.__neo_guard_installed = true; // the theme's guard steps aside
	P.__dialog_model = true;
})();
