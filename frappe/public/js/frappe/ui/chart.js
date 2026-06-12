import { Chart } from "frappe-charts/dist/frappe-charts.esm";

frappe.provide("frappe.ui");

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
	}
}
frappe.Chart = ManagedChart;

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
