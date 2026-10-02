//// Neoffice — added file (no upstream equivalent in version-15): copy of frappe develop
//// 12050baed8 (2026-05-20 "fix(pdf): inject update_page_no.js so non-print_designer headers
//// can clone"), brought in by 515e83888c (2026-05-27) so browser.py can inject it into the
//// header/footer pages (repeating letterhead + page numbers on every page).
//// Neoffice-only change: the .only-if-more-pages toggle in update_page_no() — elements with
//// that class (our "continued on next page" footer note) are shown on every page except
//// the last one. And front pages (2026-10-02): a header or footer that carries
//// data-front-pages="N" numbers the document from the page after the N first ones (a cover
//// page sent ahead of a quotation or an invoice is not a page of it), and hides its
//// .not-on-front-pages elements on those N pages. Everything else is verbatim upstream.
// Injected into header/footer pages so clone_and_update is available
// when browser.py calls header_page.evaluate("clone_and_update(...)").
// Matches print_designer/print_designer/page/print_designer/update_page_no.js

const replaceText = (parentEL, className, text) => {
	const elements = parentEL.getElementsByClassName(className);
	for (let j = 0; j < elements.length; j++) {
		elements[j].textContent = text;
	}
};

const update_page_no = (clone, i, no_of_pages, print_designer) => {
	const dateObj = new Date();
	if (print_designer) {
		replaceText(clone, "page_info_page", i);
		replaceText(clone, "page_info_topage", no_of_pages);
		replaceText(clone, "page_info_date", dateObj.toLocaleDateString());
		replaceText(clone, "page_info_isodate", dateObj.toISOString());
		replaceText(clone, "page_info_time", dateObj.toLocaleTimeString());
	} else {
		//// Neoffice — front pages, see the file header.
		const front_el = clone.matches && clone.matches("[data-front-pages]")
			? clone
			: clone.querySelector && clone.querySelector("[data-front-pages]");
		const front = front_el ? parseInt(front_el.getAttribute("data-front-pages"), 10) || 0 : 0;
		const on_front = i <= front;
		const hidden_on_front = clone.getElementsByClassName("not-on-front-pages");
		for (let k = 0; k < hidden_on_front.length; k++) {
			hidden_on_front[k].style.visibility = on_front ? "hidden" : "visible";
		}
		replaceText(clone, "page", on_front ? "" : i - front);
		replaceText(clone, "topage", no_of_pages - front);
		replaceText(clone, "date", dateObj.toLocaleDateString());
		replaceText(clone, "isodate", dateObj.toISOString());
		replaceText(clone, "time", dateObj.toLocaleTimeString());
		// Elements flagged .only-if-more-pages (e.g. a "continued on next page"
		// footer note) are shown on every page except the last one.
		const moreEls = clone.getElementsByClassName("only-if-more-pages");
		for (let k = 0; k < moreEls.length; k++) {
			//// Neoffice — and never on a front page: a cover page continues nothing.
			moreEls[k].style.visibility = i < no_of_pages && !on_front ? "visible" : "hidden";
		}
	}
};

const toggle_visibility = (clone, id, visibility) => {
	const element = clone.querySelector(id);
	if (element) {
		element.style.display = visibility;
	}
};

const add_wrapper = (clone, wrapper) => {
	wrapper = wrapper.cloneNode(true);
	wrapper.appendChild(clone);
	return wrapper;
};

const extract_elements = (template, type) => {
	const extracted = {
		even: template.querySelector(`#evenPage${type}`).cloneNode(true),
		odd: template.querySelector(`#oddPage${type}`).cloneNode(true),
		last: template.querySelector(`#lastPage${type}`).cloneNode(true),
	};

	extracted.even.style.display = "block";
	extracted.odd.style.display = "block";
	extracted.last.style.display = "block";

	template.querySelector(`#evenPage${type}`).remove();
	template.querySelector(`#oddPage${type}`).remove();
	template.querySelector(`#lastPage${type}`).remove();

	template.querySelector(`#firstPage${type}`).style.display = "none";
	extracted.even = add_wrapper(extracted.even, template);
	extracted.odd = add_wrapper(extracted.odd, template);
	extracted.last = add_wrapper(extracted.last, template);
	template.querySelector(`#firstPage${type}`).style.display = "block";

	return extracted;
};

const clone_and_update = (
	selector,
	no_of_pages,
	print_designer,
	type = null,
	is_dynamic = true
) => {
	const template = document.querySelector(selector);
	if (!template) return;
	let elements;
	if (print_designer) {
		elements = extract_elements(template, type);
	}
	const fragment = document.createDocumentFragment();
	for (let i = 2; i <= (is_dynamic ? no_of_pages : 4); i++) {
		let clone;
		if (print_designer) {
			if (i == (is_dynamic ? no_of_pages : 4)) {
				clone = elements.last?.cloneNode(true);
			} else if (i % 2 == 0) {
				clone = elements.even?.cloneNode(true);
			} else {
				clone = elements.odd?.cloneNode(true);
			}
		} else {
			clone = template.cloneNode(true);
		}
		if (is_dynamic) {
			update_page_no(clone, i, no_of_pages, print_designer);
		}
		fragment.appendChild(clone);
	}
	template.parentElement.appendChild(fragment);
	if (is_dynamic) {
		update_page_no(template, 1, no_of_pages, print_designer);
	}
};
