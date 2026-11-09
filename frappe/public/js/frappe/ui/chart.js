import { Chart } from "frappe-charts/dist/frappe-charts.esm";

frappe.provide("frappe.ui");

//// Neoffice — upstream: `frappe.Chart = Chart;` (the raw frappe-charts class). Replaced by the
//// ManagedChart subclass below (fe363d6fd6, 2026-06-12 "fix(charts): destroy stranded frappe-charts
//// instances on re-create"): a second chart on the same wrapper left the previous instance's
//// ResizeObserver alive, drawing into a detached SVG — endless "removeChild … not a child of this
//// node" errors that froze scrolling on workspaces. Same commit: utils.js make_chart and
//// widgets/chart_widget.js. No upstream equivalent (v15 or develop).
// Re-creating a chart on a wrapper that already hosts one strands the
// previous instance: frappe-charts keeps an internal ResizeObserver per
// instance, and a stranded one keeps calling draw() against a SVG that is
// no longer in the DOM — endless "removeChild … not a child of this node"
// errors that freeze scrolling (seen on workspace indicator charts).
// Subclass the lib once, here, so EVERY call site (make_chart, form
// dashboards, query reports…) gets the previous instance destroyed first.
class ManagedChart extends Chart {
	constructor(parent, options) {
		const host =
			typeof parent === "string"
				? document.querySelector(parent)
				: parent instanceof jQuery
				? parent[0]
				: parent;
		if (host && host.__frappe_chart && host.__frappe_chart.destroy) {
			try {
				host.__frappe_chart.destroy();
			} catch (e) {
				// previous instance already torn down with its DOM
			}
		}
		super(parent, options);
		if (host) host.__frappe_chart = this;
		//// Neoffice — the heatmap's labels in the user's language: see localize_heatmap below.
		if (this.type === "heatmap") localize_heatmap(this);
	}
}
frappe.Chart = ManagedChart;

//// Neoffice — frappe-charts writes the heatmap's month and weekday labels, its « Less » / « More »
//// legend and the date of its tooltip (« on Jan 2026, 05 ») in English, from constants of its own:
//// the lib has no language option. An item's form dashboard showed English months and days to a
//// French user (maintenance#2421). The labels are rewritten after every draw (the lib redraws
//// through this.draw, on update and on resize) with the browser's own names of months and days in
//// the user's language, and the tooltip's date is written as the desk writes a date. No upstream
//// equivalent (v15 or develop); remove when frappe-charts takes a language.
const HEATMAP_MONTHS = [
	"JAN",
	"FEB",
	"MAR",
	"APR",
	"MAY",
	"JUN",
	"JUL",
	"AUG",
	"SEP",
	"OCT",
	"NOV",
	"DEC",
];
const HEATMAP_DAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

function localize_heatmap(chart) {
	const lang = frappe.boot && frappe.boot.lang;
	if (!lang || lang === "en") return;
	const short_name = (options, date) =>
		new Intl.DateTimeFormat(lang, options).format(date).replace(/\.$/, "");
	// The lib's indexes: months from January, days from Sunday (4 January 2026 is a Sunday).
	const months = HEATMAP_MONTHS.map((name, i) =>
		short_name({ month: "short" }, new Date(2026, i, 1)).toUpperCase()
	);
	const days = HEATMAP_DAYS.map((name, i) =>
		short_name({ weekday: "short" }, new Date(2026, 0, 4 + i))
	);
	const legend = { Less: __("Less"), More: __("More") };

	const relabel = () => {
		if (!chart.container) return;
		chart.container.querySelectorAll("text.domain-name").forEach((text) => {
			const i = HEATMAP_MONTHS.indexOf(text.textContent);
			if (i >= 0) text.textContent = months[i];
		});
		chart.container.querySelectorAll("text.subdomain-name").forEach((text) => {
			const i = HEATMAP_DAYS.indexOf(text.textContent);
			if (i >= 0) text.textContent = days[i];
			else if (legend[text.textContent]) text.textContent = legend[text.textContent];
		});
	};
	const draw = chart.draw;
	chart.draw = function (...args) {
		const result = draw.apply(this, args);
		relabel();
		return result;
	};
	relabel();

	// The tooltip's title is " on <Mon> <yyyy>, <dd>", built by the lib from the square's yyyy-mm-dd.
	const tip = chart.tip;
	if (!tip || !tip.setValues) return;
	const set_values = tip.setValues;
	tip.setValues = function (x, y, title = {}, ...rest) {
		const match = / on (\w{3}) (\d{4}), (\d{1,2})$/.exec(title.name || "");
		const month = match ? HEATMAP_MONTHS.indexOf(match[1].toUpperCase()) : -1;
		if (month >= 0) {
			const mm = String(month + 1).padStart(2, "0");
			const dd = match[3].padStart(2, "0");
			const day = frappe.datetime.str_to_user(`${match[2]}-${mm}-${dd}`);
			title = { ...title, name: " " + __("on {0}", [day], "Day of a heatmap") };
		}
		return set_values.call(this, x, y, title, ...rest);
	};
}

frappe.ui.RealtimeChart = class RealtimeChart extends frappe.Chart {
	constructor(element, socketEvent, maxLabelPoints = 8, data) {
		super(element, data);
		if (data.data.datasets[0].values.length > maxLabelPoints) {
			frappe.throw(
				__(
					"Length of passed data array is greater than value of maximum allowed label points!"
				)
			);
		}
		this.currentSize = data.data.datasets[0].values.length;
		this.socketEvent = socketEvent;
		this.maxLabelPoints = maxLabelPoints;

		this.start_updating = function () {
			frappe.realtime.on(this.socketEvent, (data) => {
				this.update_chart(data.label, data.points);
			});
		};

		this.stop_updating = function () {
			frappe.realtime.off(this.socketEvent);
		};

		this.update_chart = function (label, data) {
			if (this.currentSize >= this.maxLabelPoints) {
				this.removeDataPoint(0);
			} else {
				this.currentSize++;
			}
			this.addDataPoint(__(label), data);
		};
	}
};
