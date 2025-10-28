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
