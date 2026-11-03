# //// Neoffice — added file (no upstream equivalent).
"""Signing in with a code, or the link beside it, sent to the mailbox.

Upstream's "login with email link" mints a key, mails it, and `login_via_key` consumes it on the GET that opens
the link. Four things are wrong with it for people who are not staff on the same machine:

* a mail scanner opens the link before the person does and burns the key ("invalid or expired");
* `login_as` skips two-factor authentication, so the link is a way around it;
* the person lands on `/me`, not on the page they came from, and a stranger cannot open an account with it;
* the code is the key itself, so there is nothing to type on another device.

This module is the replacement, and the one door every Neoffice surface signs in through (the desk's `/login`,
the shop's dialog, POS Next's guest ordering):

    request_code(email)          the mail carries a 6-digit code AND a link; the answer never says if the address is known
    verify_code(email, code)     signs in, or proves the mailbox of an address that has no account yet
    sign_in_with_link(token)     what the button of the landing page `/login-link` posts (a GET never signs in)
    consume_proof(proof)         the Python door of an app that creates the account (the shop, the till)

What it guarantees, and what each line cost to learn:

* Codes are 6 digits from `secrets`, kept as an HMAC (keyed by the site's encryption key) in the cache, never in
  clear: 10 minutes, 5 attempts counted ATOMICALLY and per address (an address nobody holds counts too, so the answer
  of an unknown address is the same as a known one's), one live code per address (a new request kills the old one),
  consumed with a single Redis DELETE so two parallel correct answers cannot both sign in.
* The link opens a page, and only its button signs in. The button needs the cookie set when the code was asked for:
  a scanner has none, and neither has an attacker who sends their own link to somebody else (login CSRF). Opened
  elsewhere (a phone's mail app), the page says to type the code instead.
* Nothing tells a known address from an unknown one: same answer, same cookie, same cooldown, same lock. The mail is
  sent by a background job, so the answer does not wait for SMTP either.
* An address nobody has an account for gets a code only where sign-up is open; the proof of its mailbox is then
  handed to the app, which creates the account. The core never creates one.
* Never offered to Administrator, Guest, a disabled user or a user who must pass two-factor authentication (they use
  their password): the second factor cannot be skipped by a mailbox. Staff (System Users) only when
  `System Settings.login_code_system_users` is ticked.
* Counted per connection with the LAST `X-Forwarded-For` entry (nginx's own), never `rate_limit`'s first one, which a
  caller writes (measured on the shops, 2026-10-30: 25 of 25 requests passed a limit of 20).
"""

import hashlib
import hmac
import re
import secrets
from urllib.parse import urlparse

import frappe
from frappe import _
from frappe.utils import cint, get_request_host, get_url, validate_email_address

CODE_DIGITS = 6
DEFAULT_EXPIRY_MINUTES = 10
MAX_ATTEMPTS = 5
RESEND_AFTER = 30  # seconds between two mails to one address
SENDS_PER_HOUR = 5  # mails to one address
REQUESTS_PER_PEER_HOUR = 60
VERIFICATIONS_PER_PEER_HOUR = 200
PROOF_MINUTES = 15
NONCE_COOKIE = "pwl_nonce"
CODE_RE = re.compile(r"^\d{%d}$" % CODE_DIGITS)

LANDING_ROUTE = "login-link"


# --------------------------------------------------------------------------------------------- settings


def _expiry_minutes() -> int:
	minutes = cint(frappe.get_system_settings("login_with_email_link_expiry")) or DEFAULT_EXPIRY_MINUTES
	return min(max(minutes, 5), 30)


def can_send_mail() -> bool:
	"""A code nobody receives locks the customer out of an account that has no password: without a working default
	outgoing account the entry is not offered, and the page falls back to the password."""
	if frappe.conf.get("mail_server"):  # the site config's own server, which frappe falls back to
		return True
	return bool(frappe.db.exists("Email Account", {"enable_outgoing": 1, "default_outgoing": 1}))


def is_enabled() -> bool:
	return bool(cint(frappe.get_system_settings("login_with_email_link"))) and can_send_mail()


def system_users_allowed() -> bool:
	return bool(cint(frappe.get_system_settings("login_code_system_users")))


def signup_is_open() -> bool:
	"""Whether an address with no account may be offered one: Website Settings' "Disable Signup" is off, and no app
	closes it for this site. An app closes it by listing a function under the `passwordless_signup_closed` hook (the
	shop does on a site for business accounts only, where an account is opened by a person, not by a mailbox)."""
	from frappe.website.utils import is_signup_disabled

	if cint(is_signup_disabled()):
		return False
	return not any(frappe.get_attr(path)() for path in frappe.get_hooks("passwordless_signup_closed"))


@frappe.whitelist(allow_guest=True)  # nosemgrep: frappe-semgrep-rules.rules.security.guest-whitelisted-method
def status():
	"""What a sign-in page needs to know to offer the code: whether it is on, and how long it lives."""
	return {"enabled": is_enabled(), "expires_in": _expiry_minutes() * 60, "resend_in": RESEND_AFTER}


# --------------------------------------------------------------------------------------------- primitives


def _secret() -> bytes:
	from frappe.utils.password import get_encryption_key

	return get_encryption_key().encode()


def _mac(kind: str, *parts: str) -> str:
	message = "\x1f".join((kind, *parts)).encode()
	return hmac.new(_secret(), message, hashlib.sha256).hexdigest()


def _key(kind: str, identity: str) -> str:
	return f"passwordless:{kind}:{identity}"


def _peer_ip() -> str:
	"""The address of the connection nginx accepted: the one entry of X-Forwarded-For a caller cannot write."""
	try:
		forwarded = frappe.get_request_header("X-Forwarded-For") or ""
	except (AttributeError, RuntimeError):  # no request is bound
		forwarded = ""
	entries = [part.strip() for part in forwarded.split(",") if part.strip()]
	if entries:
		return entries[-1]
	return (getattr(frappe.local, "request_ip", None) or "").strip() or "unknown"


def _hit(name: str, identity: str, seconds: int) -> int:
	"""Count one event of `identity` under `name`; how many the window has seen, this one included. The window is set
	before the count in one atomic call, so a process that dies between the two cannot leave a counter with no end."""
	cache = frappe.cache
	key = cache.make_key(_key(name, identity))
	cache.set(key, 0, ex=seconds, nx=True)
	return int(cache.incr(key))


def _forget_counter(name: str, identity: str):
	frappe.cache.delete(frappe.cache.make_key(_key(name, identity)))


def _take(key: str) -> bool:
	"""Delete `key` and say whether this call is the one that deleted it: the single-use guarantee."""
	return frappe.cache.delete(frappe.cache.make_key(key)) == 1


def _too_many(message: str):
	frappe.throw(message, frappe.TooManyRequestsError)


def _normalise(email) -> str:
	address = email.strip().lower() if isinstance(email, str) else ""
	if not address or len(address) > 140 or not validate_email_address(address, throw=False):
		return ""
	return address


def _clean_brand(brand) -> str:
	text = re.sub(r"[\x00-\x1f\x7f<>]", "", brand) if isinstance(brand, str) else ""
	return text.strip()[:80]


def _default_brand() -> str:
	return frappe.get_website_settings("app_name") or frappe.get_system_settings("app_name") or _("Frappe")


def _safe_redirect(redirect_to) -> str | None:
	"""Where the person goes after signing in: a path of THIS site, nothing else. `login.sanitize_redirect` would send a
	foreign address to /app, which is a dead end for a customer, and it needs a request; this refuses it instead."""
	if not isinstance(redirect_to, str):
		return None
	text = redirect_to.strip()
	if not text or len(text) > 500 or re.search(r"[\x00-\x1f\x7f\\]", text):
		return None
	parsed = urlparse(text)
	if parsed.scheme or parsed.netloc:
		request = getattr(frappe.local, "request", None)
		if (
			not request
			or parsed.scheme not in ("http", "https")
			or parsed.netloc != get_request_host(request)
		):
			return None
		text = parsed.path + (f"?{parsed.query}" if parsed.query else "")
	if not text.startswith("/") or text.startswith("//"):
		return None
	return text


# --------------------------------------------------------------------------------------------- who may sign in


def signable_user(address: str) -> str | None:
	"""The name of the User this address signs in as with a code, or None when it cannot (no account, disabled, a
	standard user, a staff account where staff are not allowed, or a user who must pass two-factor authentication)."""
	row = frappe.db.get_value("User", address, ["name", "enabled", "user_type"], as_dict=True)
	if not row or not cint(row.enabled) or row.name in frappe.STANDARD_USERS:
		return None
	if row.user_type == "System User" and not system_users_allowed():
		return None

	from frappe.twofactor import should_run_2fa

	if should_run_2fa(row.name):
		return None
	return row.name


# --------------------------------------------------------------------------------------------- asking for a code


@frappe.whitelist(
	allow_guest=True, methods=["POST"]
)  # nosemgrep: frappe-semgrep-rules.rules.security.guest-whitelisted-method
def request_code(email, redirect_to=None, brand=None):
	"""Mail a code and a link to `email`. The answer is the same whether the address has an account, may open one or
	is nobody's: only the mailbox learns anything. Guest-callable on purpose, limited per address and per connection.
	"""
	if not is_enabled():
		return {"ok": False, "reason": "disabled"}

	address = _normalise(email)
	if not address:
		return {"ok": False, "reason": "invalid_address"}

	if _hit("request_peer", _peer_ip(), 3600) > REQUESTS_PER_PEER_HOUR:
		_too_many(_("Too many sign-in requests from this connection. Try again in an hour."))

	answer = {"ok": True, "resend_in": RESEND_AFTER, "expires_in": _expiry_minutes() * 60}
	address_id = _mac("address", address)

	# The cooldown counts every address, known or not: it must not tell them apart.
	if _hit("cooldown", address_id, RESEND_AFTER) > 1:
		return answer
	if _hit("sends", address_id, 3600) > SENDS_PER_HOUR:
		_too_many(_("Too many codes were asked for this address. Try again in an hour."))

	user = signable_user(address)
	nonce = secrets.token_urlsafe(16)
	if user or (signup_is_open() and not frappe.db.exists("User", address)):
		_start_challenge(address, address_id, nonce, redirect_to, brand, known=bool(user))
	_set_nonce_cookie(nonce)  # always, so that the response's headers do not tell either
	return answer


def _start_challenge(address, address_id, nonce, redirect_to, brand, known):
	minutes = _expiry_minutes()
	seconds = minutes * 60
	code = f"{secrets.randbelow(10**CODE_DIGITS):0{CODE_DIGITS}d}"
	link_token = secrets.token_urlsafe(32)
	challenge_id = secrets.token_hex(8)
	challenge = {
		"id": challenge_id,
		"code": _mac("code", address, challenge_id, code),
		"link": _mac("link", link_token),
		"nonce": _mac("nonce", nonce),
		"redirect_to": _safe_redirect(redirect_to),
		"known": known,
	}
	frappe.cache.set_value(_key("challenge", address_id), challenge, expires_in_sec=seconds)
	frappe.cache.set_value(
		_key("link", challenge["link"]), {"address": address, "id": challenge_id}, expires_in_sec=seconds
	)
	_forget_counter("attempts", address_id)

	frappe.enqueue(
		"frappe.passwordless.send_code_email",
		queue="short",
		now=bool(frappe.flags.in_test),
		address=address,
		code=code,
		link_token=link_token,
		minutes=minutes,
		brand=_clean_brand(brand) or _default_brand(),
		lang=frappe.local.lang,
		known=known,
	)


def _set_nonce_cookie(nonce: str):
	manager = getattr(frappe.local, "cookie_manager", None)
	if manager:
		manager.set_cookie(NONCE_COOKIE, nonce, max_age=_expiry_minutes() * 60, httponly=True, samesite="Lax")


def send_code_email(address, code, link_token, minutes, brand, lang=None, known=True):
	"""The job that mails the code. A failure is logged, never shown: the person asked, the answer was already given."""
	frappe.local.lang = lang or frappe.local.lang
	link = get_url(f"/{LANDING_ROUTE}?token={link_token}", allow_header_override=False)
	try:
		frappe.sendmail(
			recipients=[address],
			subject=_("{0} is your sign-in code for {1}").format(code, brand),
			template="passwordless_code",
			args={"code": code, "link": link, "minutes": minutes, "brand": brand, "known": known},
			now=True,
		)
	except Exception:
		frappe.clear_messages()
		frappe.log_error("Sign-in code email could not be sent", frappe.get_traceback())


# --------------------------------------------------------------------------------------------- answering


def _refused(reason: str) -> dict:
	return {"ok": False, "reason": reason}


@frappe.whitelist(
	allow_guest=True, methods=["POST"]
)  # nosemgrep: frappe-semgrep-rules.rules.security.guest-whitelisted-method
def verify_code(email, code):
	"""Check the code. Signs in when the address has an account; otherwise hands back the proof of its mailbox."""
	address = _normalise(email)
	code = re.sub(r"\s+", "", code) if isinstance(code, str) else ""
	if not address or not CODE_RE.match(code):
		return _refused("invalid")

	if _hit("verify_peer", _peer_ip(), 3600) > VERIFICATIONS_PER_PEER_HOUR:
		_too_many(_("Too many attempts from this connection. Try again in an hour."))

	address_id = _mac("address", address)
	challenge_key = _key("challenge", address_id)
	challenge = frappe.cache.get_value(challenge_key, expires=True)

	# Counted whether or not a code is live, so an address nobody holds locks at the same attempt as one that has one.
	if _hit("attempts", address_id, _expiry_minutes() * 60) > MAX_ATTEMPTS:
		_take(challenge_key)
		return _refused("locked")

	expected = challenge["code"] if challenge else _mac("code", address, "none", "none")
	given = _mac("code", address, challenge["id"] if challenge else "none", code)
	if not (challenge and hmac.compare_digest(expected, given)):
		return _refused("invalid")

	if not _take(challenge_key):  # a parallel call with the same correct code got there first
		return _refused("invalid")
	_take(_key("link", challenge["link"]))
	return _finish(address, challenge)


def _finish(address: str, challenge: dict) -> dict:
	user = signable_user(address)
	if user:
		return sign_in(user, challenge.get("redirect_to"))
	if frappe.db.exists("User", address) or not signup_is_open():
		return _refused("invalid")
	return {"ok": True, "status": "unknown_address", "proof": _mint_proof(address)}


def _mint_proof(address: str) -> str:
	proof = secrets.token_urlsafe(24)
	frappe.cache.set_value(
		_key("proof", _mac("proof", proof)), {"address": address}, expires_in_sec=PROOF_MINUTES * 60
	)
	return proof


def consume_proof(proof) -> str | None:
	"""The address whose mailbox `proof` was minted for, once: for the app that creates the account."""
	if not isinstance(proof, str) or not proof:
		return None
	key = _key("proof", _mac("proof", proof))
	record = frappe.cache.get_value(key, expires=True)
	if not record or not _take(key):
		return None
	return record["address"]


def sign_in(user: str, redirect_to: str | None = None) -> dict:
	"""Open the session of `user`, whom a code or a link has just proven to be at their mailbox. Public so that an app
	which created the account from a proof ends the same way."""
	from frappe.auth import LoginManager

	redirect_to = _safe_redirect(redirect_to)
	manager = getattr(frappe.local, "login_manager", None) or LoginManager()
	try:
		manager.login_as(user)
	except (frappe.PermissionError, frappe.AuthenticationError, frappe.ValidationError):
		frappe.clear_messages()
		return _refused("not_allowed")
	return {"ok": True, "status": "signed_in", "user": user, "redirect_to": _landing(user, redirect_to)}


def _landing(user: str, redirect_to: str | None) -> str:
	desk_user = frappe.db.get_value("User", user, "user_type") == "System User"
	# The page they came from, except the desk for a customer: it has nothing for them (a dead end).
	if redirect_to and (desk_user or not (redirect_to == "/app" or redirect_to.startswith("/app/"))):
		return redirect_to
	if desk_user:
		from frappe.apps import get_default_path

		return get_url(get_default_path() or "/app")
	return get_url("/me")


# --------------------------------------------------------------------------------------------- the link


def link_state(token) -> dict:
	"""What the landing page should show for `token`, without consuming anything: a scanner can open it freely.

	`ready` the cookie of the browser that asked is here, the button signs in
	`other_device` the link is live but this browser did not ask: type the code
	`needs_code` the address has no account yet: the code is the way to open one
	`invalid` unknown, expired or replaced
	"""
	if not isinstance(token, str) or not token:
		return {"status": "invalid"}
	link = frappe.cache.get_value(_key("link", _mac("link", token)), expires=True)
	if not link:
		return {"status": "invalid"}
	challenge = frappe.cache.get_value(_key("challenge", _mac("address", link["address"])), expires=True)
	if not challenge or challenge["id"] != link["id"]:
		return {"status": "invalid"}
	if not challenge.get("known"):
		return {"status": "needs_code"}
	sent = frappe.request.cookies.get(NONCE_COOKIE) if getattr(frappe.local, "request", None) else None
	if sent and hmac.compare_digest(challenge["nonce"], _mac("nonce", sent)):
		return {"status": "ready"}
	return {"status": "other_device"}


@frappe.whitelist(
	allow_guest=True, methods=["POST"]
)  # nosemgrep: frappe-semgrep-rules.rules.security.guest-whitelisted-method
def sign_in_with_link(token):
	"""What the landing page's button posts: signs in, then sends the browser on. Guest-callable on purpose."""
	if _hit("verify_peer", _peer_ip(), 3600) > VERIFICATIONS_PER_PEER_HOUR:
		_too_many(_("Too many attempts from this connection. Try again in an hour."))

	if link_state(token)["status"] != "ready":
		return _link_refused()

	link_key = _key("link", _mac("link", token))
	link = frappe.cache.get_value(link_key, expires=True)
	challenge_key = _key("challenge", _mac("address", link["address"]))
	challenge = frappe.cache.get_value(challenge_key, expires=True)
	if not _take(challenge_key):
		return _link_refused()
	_take(link_key)

	user = signable_user(link["address"])
	result = sign_in(user, challenge.get("redirect_to")) if user else _refused("invalid")
	if not result.get("ok"):
		return _link_refused()
	frappe.local.response["type"] = "redirect"
	frappe.local.response["location"] = result["redirect_to"]


def _link_refused():
	frappe.respond_as_web_page(
		_("Not Permitted"),
		_("This sign-in link is invalid or has expired. Ask for a new code."),
		http_status_code=403,
		indicator_color="red",
	)
