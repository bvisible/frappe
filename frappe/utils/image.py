# Copyright (c) 2015, Frappe Technologies Pvt. Ltd. and Contributors
# License: MIT. See LICENSE
import io
import os

from PIL import Image, ImageOps  # //// Neoffice — ImageOps: strip_exif_data turns a photo upright (#1540)

import frappe


def resize_images(path, maxdim=700):
	size = (maxdim, maxdim)
	for basepath, folders, files in os.walk(path):  # noqa: B007
		for fname in files:
			extn = fname.rsplit(".", 1)[1]
			if extn in ("jpg", "jpeg", "png", "gif"):
				im = Image.open(os.path.join(basepath, fname))
				if im.size[0] > size[0] or im.size[1] > size[1]:
					im.thumbnail(size, Image.Resampling.LANCZOS)
					im.save(os.path.join(basepath, fname))

					print(f"resized {os.path.join(basepath, fname)}")


def strip_exif_data(content, content_type):
	"""Strips EXIF from image files which support it.

	Works by creating a new Image object which ignores exif by
	default and then extracts the binary data back into content.

	Returns:
	        Bytes: Stripped image content
	"""

	original_image = Image.open(io.BytesIO(content))
	output = io.BytesIO()
	# //// Neoffice — the orientation the EXIF carries is applied BEFORE the EXIF is dropped (#1540). The tag
	# //// is the only thing that says a phone photo taken upright is stored sideways: copied without it, the
	# //// picture came back lying down (and the OCR reads a lying page badly). A picture with no orientation
	# //// tag comes back as it was.
	original_image = ImageOps.exif_transpose(original_image)
	# ref: https://stackoverflow.com/a/48248432
	if content_type == "image/jpeg" and original_image.mode in ("RGBA", "P"):
		original_image = original_image.convert("RGB")

	# //// Neoffice — the pixels are copied as raw bytes (a 24 megapixel photo: 70 MB). Upstream built a list of
	# //// every pixel (`putdata(list(getdata()))`): more than 1 GB for the same photo, enough to kill a worker.
	new_image = Image.frombytes(original_image.mode, original_image.size, original_image.tobytes())
	new_image.save(output, format=content_type.split("/")[1])

	content = output.getvalue()

	return content


def optimize_image(content, content_type, max_width=1024, max_height=768, optimize=True, quality=85):
	if content_type == "image/svg+xml":
		return content

	try:
		image = Image.open(io.BytesIO(content))
		exif = image.getexif()
		# //// Neoffice — restored 2026-09-03. Upstream d0eabcd4f6 ("fix: preserve exif data in optimized
		# //// image", backport of #27341) added `exif = image.getexif()` here; our merge 0b9b53c7ea
		# //// (2024-09-23) kept upstream's `exif=exif` in image.save() but lost this assignment, so
		# //// optimize_image() raised NameError on every call, swallowed by the broad except below
		# //// into "Failed to optimize image" — image optimisation was silently dead fleet-wide.
		# //// Found by the //// marking campaign (tracker #205).
		width, height = image.size
		max_height = max(min(max_height, height * 0.8), 200)
		max_width = max(min(max_width, width * 0.8), 200)
		image_format = content_type.split("/")[1]
		# //// Neoffice — added (4c842a98fc "First change v15", finalised in the merge 0b9b53c7ea):
		# //// force PNG output whenever the source image carries transparency (a palette image with a
		# //// transparency index, or an RGBA image with a non-opaque alpha channel). Upstream keeps the
		# //// uploaded content-type, so a transparent PNG re-encoded as JPEG came back with a black
		# //// background.
		# //// added
		if image.info.get("transparency", None) is not None:
			image_format = "png"
		if image.mode == "P":
			transparent = image.info.get("transparency", -1)
			for _, index in image.getcolors():
				if index == transparent:
					image_format = "png"
		elif image.mode == "RGBA":
			extrema = image.getextrema()
			if extrema[3][0] < 255:
				image_format = "png"
		# ////
		size = max_width, max_height
		image.thumbnail(size, Image.Resampling.LANCZOS)

		output = io.BytesIO()
		image.save(
			output,
			format=image_format,
			optimize=optimize,
			quality=quality,
			save_all=True if image_format == "gif" else None,
			exif=exif,
		)
		optimized_content = output.getvalue()
		return optimized_content if len(optimized_content) < len(content) else content
	except Exception as e:
		frappe.msgprint(frappe._("Failed to optimize image: {0}").format(str(e)))
		return content
