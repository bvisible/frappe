# //// Neoffice — added file (no upstream equivalent). A request marked `silent` (a reading the page makes in the background)
# //// opened « Not permitted » when its refusal was a bare `raise frappe.PermissionError()` (neoffice-maintenance#3072, #2991).
"""The 403 of a request marked `silent` opens no window; a request that is not silent keeps what the server says.

request.js is run in node with the little of the desk it touches (a window is `frappe.msgprint`, the signed-out window
`frappe.app.handle_session_expired`): nothing of frappe is imported, and `$.ajax` is the point where the answer comes in.
"""

import json
import shutil
import subprocess
import unittest
from pathlib import Path

REQUEST_JS = Path(__file__).resolve().parents[1] / "public" / "js" / "frappe" / "request.js"

HARNESS = r"""
const vm = require("node:vm");
const fs = require("node:fs");
const [file, raw] = process.argv.slice(1);
const cases = JSON.parse(raw);
const source = fs.readFileSync(file, "utf8");

async function play(c) {
	const seen = { windows: [], signed_out: 0, errors: 0, console: [] };
	let failed = null;
	const chain = { attr: () => chain, prop: () => chain, ajaxSend: () => chain, ajaxComplete: () => chain };
	const $ = () => chain;
	$.isPlainObject = (v) => !!v && Object.getPrototypeOf(v) === Object.prototype;
	$.isArray = Array.isArray;
	$.extend = Object.assign;
	$.ajax = (args) => {
		// the question « who is signed in? » of on_refusal, answered by the case
		if (String(args.url).includes("get_logged_user")) {
			const answer = {
				done(fn) { if (c.session === "alive") fn({ message: "e2e-staff@yopmail.com" }); return answer; },
				fail(fn) { if (c.session !== "alive") fn({ status: 403 }); return answer; },
			};
			return answer;
		}
		const call = { done: () => call, always: () => call, fail(fn) { failed = fn; return call; } };
		return call;
	};
	const frappe = {
		provide(name) {
			let at = frappe;
			for (const part of name.split(".").slice(1)) at = at[part] = at[part] || {};
		},
		session: { user: "e2e-staff@yopmail.com", logged_in_user: "e2e-staff@yopmail.com" },
		msgprint: (m) => seen.windows.push(typeof m === "string" ? m : m.message),
		app: { handle_session_expired: () => (seen.signed_out += 1) },
		csrf_token: "t",
		boot: {},
		utils: { play_sound() {} },
		dom: { freeze() {}, unfreeze() {} },
	};
	const sandbox = {
		frappe, $, __: (s) => s, window: { addEventListener() {} }, document: {}, setTimeout, Promise, JSON, Object, Array, String, Date,
		console: { log() {}, error() {}, warn: (...a) => seen.console.push(a.join(" ")) },
	};
	vm.runInNewContext(source, sandbox);
	frappe.request.call({ url: "/api/method/" + c.method, args: {}, type: "POST", silent: c.silent, error: () => (seen.errors += 1) });
	const body = c.body;
	failed({
		status: 403,
		statusCode: () => ({ status: 403 }),
		getResponseHeader: () => (body ? "application/json" : "text/html"),
		responseText: body ? JSON.stringify(body) : "<html>login</html>",
		responseJSON: body || undefined,
	}, "error");
	await new Promise((r) => setTimeout(r, 20));
	return seen;
}

(async () => {
	const out = [];
	for (const c of cases) out.push(await play(c));
	console.log(JSON.stringify(out));
})();
"""

BARE = {"exc_type": "PermissionError"}
SAID = {"exc_type": "PermissionError", "_error_message": "You may read only your own record."}
GENERIC = (
	"You do not have enough permissions to access this resource. Please contact your manager to get access."
)


def played(*cases: dict) -> list:
	out = subprocess.run(
		[shutil.which("node"), "-e", HARNESS, str(REQUEST_JS), json.dumps(list(cases))],
		capture_output=True,
		text=True,
		check=True,
		timeout=30,
	)
	return json.loads(out.stdout.strip().splitlines()[-1])


def case(silent, body, session="alive", method="frappe.client.get"):
	return {"silent": silent, "body": body, "session": session, "method": method}


class TestABackgroundRefusalOpensNoWindow(unittest.TestCase):
	@classmethod
	def setUpClass(cls):
		if not shutil.which("node"):
			raise AssertionError("node is needed to run the desk's request.js (the CI installs it)")

	def test_a_silent_reading_refused_bare_opens_no_window_and_says_it_in_the_console(self):
		(seen,) = played(case(True, BARE, method="nora.api.company_knowledge.view"))
		self.assertEqual(seen["windows"], [], "no « Not permitted » for a reading nobody asked for")
		self.assertEqual(seen["signed_out"], 0)
		self.assertEqual(seen["errors"], 1, "the caller still learns the refusal")
		self.assertTrue(
			any("nora.api.company_knowledge.view" in line for line in seen["console"]), seen["console"]
		)

	def test_a_silent_reading_refused_with_a_sentence_opens_no_window_either(self):
		(seen,) = played(case(True, SAID))
		self.assertEqual(seen["windows"], [])
		self.assertTrue(any("own record" in line for line in seen["console"]), seen["console"])

	def test_a_request_that_is_not_silent_keeps_the_servers_sentence_or_says_not_permitted(self):
		said, bare = played(case(False, SAID), case(False, BARE))
		self.assertEqual(
			said["windows"], ["You may read only your own record."], "its own sentence, not the generic one"
		)
		self.assertEqual(bare["windows"], [GENERIC])

	def test_a_session_that_is_gone_is_still_said_once_even_for_a_silent_reading(self):
		(seen,) = played(case(True, None, session="expired"))
		self.assertEqual(seen["windows"], [])
		self.assertEqual(
			seen["signed_out"], 1, "« You have been signed out »: about the person, not the request"
		)


if __name__ == "__main__":
	unittest.main()
