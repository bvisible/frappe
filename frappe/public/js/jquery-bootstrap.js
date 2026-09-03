import jQuery from "jquery";
import Alert from "bootstrap/js/dist/alert";
import Button from "bootstrap/js/dist/button";
import Carousel from "bootstrap/js/dist/carousel";
import Collapse from "bootstrap/js/dist/collapse";
import Dropdown from "bootstrap/js/dist/dropdown";
import Modal from "bootstrap/js/dist/modal";
import Popover from "bootstrap/js/dist/popover";
import Scrollspy from "bootstrap/js/dist/scrollspy";
import Tab from "bootstrap/js/dist/tab";
import Toast from "bootstrap/js/dist/toast";
import Tooltip from "bootstrap/js/dist/tooltip";
import Util from "bootstrap/js/dist/util";

window.jQuery = jQuery;
window.$ = jQuery;

//// Neoffice — added block (9999364ec6, 2025-10-28 "Refonte de l'interface utilisateur avec
//// nouvelle sidebar et apps switcher"; block runs to the export statement below). Upstream only
//// sets window.jQuery / window.$ and re-exports the Bootstrap classes — it relies on
//// bootstrap/js/dist/* having registered themselves on the ONE jQuery instance esbuild resolved.
//// That rework made apps load from symlinked benches, and esbuild then resolved two copies of
//// jquery: the plugins attached to the copy the desk bundle does not use, so $.fn.modal,
//// $.fn.popover, $.fn.tooltip … were undefined at runtime (every dialog and every filter popover
//// dead). Re-registering each plugin explicitly on window.jQuery makes the binding independent
//// of how many jQuery instances the bundler produced. No upstream equivalent (v15.120 or
//// develop). TO REVIEW at the merge: the real fix is the esbuild dedupe/alias for jquery — drop
//// this shim once the bundler resolves a single instance for symlinked apps.
// Manually register Bootstrap plugins on window.jQuery to fix symlink bundling issue
// This ensures Bootstrap plugins are available even when esbuild creates multiple jQuery instances
const registerPlugin = (name, Plugin) => {
	const jQueryInterface = function (config, ...args) {
		return this.each(function () {
			let data = jQuery(this).data(`bs.${name}`);

			if (!data) {
				data = new Plugin(this, typeof config === "object" ? config : {});
				jQuery(this).data(`bs.${name}`, data);
			}

			if (typeof config === "string") {
				if (typeof data[config] === "undefined") {
					throw new TypeError(`No method named "${config}"`);
				}
				data[config](...args);
			}
		});
	};

	jQuery.fn[name] = jQueryInterface;
	jQuery.fn[name].Constructor = Plugin;
	jQuery.fn[name].noConflict = function () {
		jQuery.fn[name] = undefined;
		return jQueryInterface;
	};
};

registerPlugin("alert", Alert);
registerPlugin("button", Button);
registerPlugin("carousel", Carousel);
registerPlugin("collapse", Collapse);
registerPlugin("dropdown", Dropdown);
registerPlugin("modal", Modal);
registerPlugin("popover", Popover);
registerPlugin("scrollspy", Scrollspy);
registerPlugin("tab", Tab);
registerPlugin("toast", Toast);
registerPlugin("tooltip", Tooltip);

export {
	Util,
	Alert,
	Button,
	Carousel,
	Collapse,
	Dropdown,
	Modal,
	Popover,
	Scrollspy,
	Tab,
	Toast,
	Tooltip,
};
