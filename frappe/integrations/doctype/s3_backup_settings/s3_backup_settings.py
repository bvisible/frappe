# Copyright (c) 2017, Frappe Technologies and contributors
# License: MIT. See LICENSE
import os
import os.path

import boto3
from botocore.exceptions import ClientError
from rq.timeouts import JobTimeoutException
#//// Neoffice — added with the multipart upload (c4d6f4d84f, 2026-07-16). TO REVIEW: dead at
#//// module level — upload_file_to_s3() re-imports TransferConfig inside the function.
from boto3.s3.transfer import TransferConfig

import frappe
from frappe import _
from frappe.integrations.offsite_backup_utils import (
	generate_files_backup,
	get_latest_backup_file,
	send_email,
	validate_file_size,
)
from frappe.model.document import Document
from frappe.utils import cint
from frappe.utils.background_jobs import enqueue

#//// Neoffice — imports for the Neoffice additions below: shutil/time for the backup
#//// relocation and pruning (68d7f3a760, 342e22a3bb), subprocess for the rclone transport in
#//// upload_file_to_s3().
import shutil
import time
import subprocess

class S3BackupSettings(Document):
	# begin: auto-generated types
	# This code is auto-generated. Do not modify anything in this block.

	from typing import TYPE_CHECKING

	if TYPE_CHECKING:
		from frappe.types import DF

		access_key_id: DF.Data
		backup_files: DF.Check
		backup_path: DF.Data | None
		bucket: DF.Data
		enabled: DF.Check
		endpoint_url: DF.Data | None
		frequency: DF.Literal["Daily", "Weekly", "Monthly", "None"]
		notify_email: DF.Data
		secret_access_key: DF.Password
		send_email_for_successful_backup: DF.Check

	# end: auto-generated types

	def validate(self):
		if not self.enabled:
			return

		if not self.endpoint_url:
			self.endpoint_url = "https://s3.amazonaws.com"

		if self.backup_path and self.backup_path[-1] != "/":
			self.backup_path += "/"

		conn = boto3.client(
			"s3",
			aws_access_key_id=self.access_key_id,
			aws_secret_access_key=self.get_password("secret_access_key"),
			endpoint_url=self.endpoint_url,
		)

		try:
			# Head_bucket returns a 200 OK if the bucket exists and have access to it.
			# Requires ListBucket permission
			conn.head_bucket(Bucket=self.bucket)
		except ClientError as e:
			error_code = e.response["Error"]["Code"]
			bucket_name = frappe.bold(self.bucket)
			if error_code == "403":
				msg = _("Do not have permission to access bucket {0}.").format(bucket_name)
			elif error_code == "404":
				msg = _("Bucket {0} not found.").format(bucket_name)
			else:
				msg = e.args[0]

			frappe.throw(msg)


@frappe.whitelist()
def take_backup():
	"""Enqueue longjob for taking backup to s3"""
	enqueue(
		"frappe.integrations.doctype.s3_backup_settings.s3_backup_settings.take_backups_s3",
		queue="long",
		timeout=1500,
	)
	frappe.msgprint(_("Queued for backup. It may take a few minutes to an hour."))


def take_backups_daily():
	take_backups_if("Daily")


def take_backups_weekly():
	take_backups_if("Weekly")


def take_backups_monthly():
	take_backups_if("Monthly")


def take_backups_if(freq):
	if cint(frappe.db.get_single_value("S3 Backup Settings", "enabled")):
		if frappe.db.get_single_value("S3 Backup Settings", "frequency") == freq:
			take_backups_s3()


#//// Neoffice — upstream: `take_backups_s3(retry_count=0)`. The three flags (68d7f3a760,
#//// 2025-07-03 "Update s3_backup_settings.py", empty message) mark a backup taken by the
#//// setup wizard, by hand or for a demo; they are forwarded to backup_to_s3() which renames
#//// and files it apart from the nightly ones. This endpoint is whitelisted, so the flags are
#//// caller-supplied strings — see how they are compared further down.
@frappe.whitelist()
def take_backups_s3(retry_count=0, wizard=False, manual=False, demo=False):
	try:
		validate_file_size()
		backup_to_s3(wizard, manual, demo)
		send_email(True, "Amazon S3", "S3 Backup Settings", "notify_email")
	except JobTimeoutException:
		if retry_count < 2:
			args = {"retry_count": retry_count + 1}
			enqueue(
				"frappe.integrations.doctype.s3_backup_settings.s3_backup_settings.take_backups_s3",
				queue="long",
				timeout=1500,
				**args,
			)
		else:
			notify()
	#//// Neoffice — upstream: a bare `except Exception:` that calls notify() and loses the
	#//// traceback. Ours logs it to Error Log first (68d7f3a760), because the notification mail is
	#//// the only other trace and it does not always reach anyone.
	except Exception as e:
		frappe.log_error("Backup failed", f"Error: {str(e)}, Traceback: {frappe.get_traceback()}")
		notify()


#//// Neoffice — upstream: `def notify():` always recomputing frappe.get_traceback(). The
#//// optional argument (68d7f3a760) lets a caller pass the message it already has.
def notify(error_message=None):
	if not error_message:
		error_message = frappe.get_traceback()
	send_email(False, "Amazon S3", "S3 Backup Settings", "notify_email", error_message)


#//// Neoffice — added helpers, no upstream equivalent: get_file_or_folder_age / remove_file /
#//// delete_backups (342e22a3bb, 2025-06-26 "feat(s3_backup): add backup file cleanup
#//// functionality"), called at the end of backup_to_s3(). They prune backups older than 3
#//// days from /mnt/neoffice/private/backups so the data volume does not fill up between
#//// nightly runs. TO REVIEW: the path and the retention are hard-coded here rather than read
#//// from S3 Backup Settings. The signature change on backup_to_s3() just below
#//// (wizard/manual/demo/force_no_files) is 68d7f3a760.
def get_file_or_folder_age(path):
	"""
	Get the age of a file or folder in seconds since epoch
	"""
	# Get the time of last modification of the file or folder
	# os.stat(path).st_mtime returns the time of last modification
	return os.stat(path).st_mtime


def remove_file(path):
	"""
	Remove a file safely
	"""
	try:
		if os.path.isfile(path):
			os.remove(path)
	except OSError as e:
		frappe.log_error("Error deleting file", f"Error removing {path}: {e}")


def delete_backups():
	"""
	Delete backup files older than specified days
	"""
	# Specify the path
	path = "/mnt/neoffice/private/backups"
	# Specify the days
	days = 3
	# Converting days to seconds
	# time.time() returns current time in seconds
	seconds = time.time() - (days * 24 * 60 * 60 - 3600)  # days - 1 hour
	
	# Checking whether the file is present in path or not
	if os.path.exists(path):
		# Iterating over each and every folder and file in the path
		for root_folder, folders, files in os.walk(path):
			# Checking the current directory files
			for file in files:
				# File path
				file_path = os.path.join(root_folder, file)
				
				# Comparing the days
				if seconds >= get_file_or_folder_age(file_path):
					# Invoking the remove_file function
					remove_file(file_path)
	else:
		# If the path is not a directory
		# Comparing with the days
		if seconds >= get_file_or_folder_age(path):
			# Invoking the file
			remove_file(path)


def backup_to_s3(wizard=False, manual=False, demo=False, force_no_files=False):
	from frappe.utils import get_backups_path
	from frappe.utils.backups import new_backup

	doc = frappe.get_single("S3 Backup Settings")
	bucket = doc.bucket
	#//// Neoffice — upstream: `backup_files = cint(doc.backup_files)`. The force_no_files
	#//// parameter added to backup_to_s3() lets a caller take a database-only backup (68d7f3a760).
	backup_files = cint(doc.backup_files) if not force_no_files else 0

	#//// Neoffice — added boto3 client options use_ssl / verify (68d7f3a760). TO REVIEW: both are
	#//// already the boto3 defaults, and the comment blames "aws-chunked", which these two flags
	#//// do not control — the real fix for oversized uploads is the TransferConfig in
	#//// upload_file_to_s3() below.
	# Minimal configuration without options that could cause aws-chunked
	conn = boto3.client(
		"s3",
		aws_access_key_id=doc.access_key_id,
		aws_secret_access_key=doc.get_password("secret_access_key"),
		endpoint_url=doc.endpoint_url or "https://s3.amazonaws.com",
		use_ssl=True,
		verify=True
	)

	#//// Neoffice — added (68d7f3a760): files_filename / private_files are pre-set to None because
	#//// the new error paths below can leave them unassigned, where upstream's straight-line flow
	#//// always binds them.
	# Initialize variables
	files_filename = None
	private_files = None
	
	if frappe.flags.create_new_backup:
		#//// Neoffice ▼▼▼ — upstream calls new_backup(ignore_files=False, ...) bare. 68d7f3a760 wraps
		#//// it: if the files tarball blows up, log and retry with ignore_files=True so the database
		#//// backup still reaches S3 instead of the whole job dying. ▲▲▲ ends at the `else:` of this
		#//// try/except.
		try:
			backup = new_backup(
				ignore_files=False,
				backup_path_db=None,
				backup_path_files=None,
				backup_path_private_files=None,
				force=True,
			)
			db_filename = os.path.join(get_backups_path(), os.path.basename(backup.backup_path_db))
			site_config = os.path.join(get_backups_path(), os.path.basename(backup.backup_path_conf))
			if backup_files:
				files_filename = os.path.join(get_backups_path(), os.path.basename(backup.backup_path_files))
				private_files = os.path.join(
					get_backups_path(), os.path.basename(backup.backup_path_private_files)
				)
		except Exception as e:
			frappe.log_error("Backup creation failed", f"Error: {str(e)}")
			# Try without files if error comes from there
			if backup_files:
				backup = new_backup(
					ignore_files=True,
					backup_path_db=None,
					backup_path_files=None,
					backup_path_private_files=None,
					force=True,
				)
				db_filename = os.path.join(get_backups_path(), os.path.basename(backup.backup_path_db))
				site_config = os.path.join(get_backups_path(), os.path.basename(backup.backup_path_conf))
				backup_files = 0  # Disable file upload
			else:
				raise
	else:
		if backup_files:
			db_filename, site_config, files_filename, private_files = get_latest_backup_file(
				with_files=backup_files
			)

			if not files_filename or not private_files:
				#//// Neoffice — added fallback (68d7f3a760): upstream lets a failure in generate_files_backup()
				#//// abort the whole run. Ours logs it and continues with the database only, so a broken files
				#//// tarball never costs the DB backup. TO REVIEW: the run is still reported as a success.
				try:
					generate_files_backup()
					db_filename, site_config, files_filename, private_files = get_latest_backup_file(
						with_files=backup_files
					)
				except Exception as e:
					frappe.log_error("Failed to generate files backup", f"Error: {str(e)}")
					# Continue without files
					db_filename, site_config = get_latest_backup_file(with_files=False)
					files_filename = None
					private_files = None
					backup_files = 0

		else:
			db_filename, site_config = get_latest_backup_file()

	#//// Neoffice — added (68d7f3a760): upstream goes straight from get_latest_backup_file() to
	#//// building the folder name and uploading, so a missing dump surfaced as an obscure
	#//// exception inside boto3. Also note the line below: upstream prefixes the folder with
	#//// `path` (S3 Backup Settings.backup_path); ours drops it — the per-site prefix now comes
	#//// from upload_file_to_s3() instead. The `path = doc.backup_path or ""` read was removed
	#//// with it, so that field is no longer honoured.
	# Check that base files exist
	if not os.path.exists(db_filename):
		error_msg = f"Database backup file not found: {db_filename}"
		frappe.log_error("Critical error", error_msg)
		frappe.throw(error_msg)
		
	if not os.path.exists(site_config):
		error_msg = f"Site config backup file not found: {site_config}"
		frappe.log_error("Critical error", error_msg)
		frappe.throw(error_msg)

	folder = os.path.basename(db_filename)[:15] + "/"
	# for adding datetime to folder name

	#//// Neoffice — added block, no upstream equivalent (68d7f3a760, 2025-07-03 "Update
	#//// s3_backup_settings.py", empty message): a backup taken by the setup wizard, by hand or
	#//// for a demo is renamed with a _wizard/_manual/_demo suffix and moved under
	#//// /mnt/neoffice/private/<kind>_backup/ so it is not swept by delete_backups() with the
	#//// nightly ones. Reached through the wizard/manual/demo flags added to take_backups_s3().
	#//// TO REVIEW: hard-coded /mnt/neoffice paths (the Neoffice data volume — see the
	#//// matching allowance in utils/file_manager.py), and the flags are compared against both 1
	#//// and "1" because they arrive from an HTTP call as strings.
	# Handle wizard/manual/demo backups
	base_file_path = os.path.join(get_backups_path(), os.path.basename(db_filename)[:15])
	if wizard == 1 or wizard == '1':
		add_word = '_wizard'
		new_base_file_path = '/mnt/neoffice/private/wizard_backup/'
	elif manual == 1 or manual == '1':
		add_word = '_manual'
		new_base_file_path = '/mnt/neoffice/private/manual_backup/'
	elif demo == 1 or demo == '1':
		add_word = '_demo'
		new_base_file_path = '/mnt/neoffice/private/demo_backup/'
	if wizard == 1 or wizard == '1' or manual == 1 or manual == '1' or demo == 1 or demo == '1':
		try:
			os.rename(db_filename, db_filename.replace('database.sql.gz', 'database'+add_word+'.sql.gz'))
		except OSError as error:
			frappe.log_error(f"Error renaming file: {error}")
		try:
			os.rename(site_config, site_config.replace('site_config_backup', 'site_config_backup'+add_word))
		except OSError as error:
			frappe.log_error(f"Error renaming file: {error}")
		if backup_files:
			try:
				os.rename(files_filename, files_filename.replace('files.tar', 'files'+add_word+'.tar'))
			except OSError as error:
				frappe.log_error(f"Error renaming file: {error}")
			try:
				os.rename(private_files, private_files.replace('files.tar', 'files'+add_word+'.tar'))
			except OSError as error:
				frappe.log_error(f"Error renaming file: {error}")
		base_file_path = new_base_file_path
		if not os.path.exists(base_file_path):
			try:
				os.makedirs(base_file_path, exist_ok=True)
				# Ensure correct permissions
				os.chmod(base_file_path, 0o755)
			except Exception as e:
				frappe.log_error("Failed to create backup directory", f"Path: {base_file_path}, Error: {str(e)}")
				raise
		old_db_filename = db_filename.replace('database.sql.gz', 'database'+add_word+'.sql.gz')
		old_site_config = site_config.replace('site_config_backup', 'site_config_backup'+add_word)
		db_filename = base_file_path + old_db_filename[old_db_filename.rfind('/')+1:]
		site_config = base_file_path + old_site_config[old_site_config.rfind('/')+1:]
		
		# Check that files exist before moving them
		if os.path.exists(old_db_filename):
			shutil.move(old_db_filename, db_filename)
		else:
			frappe.log_error("Database backup file not found", f"Missing: {old_db_filename}")
			
		if os.path.exists(old_site_config):
			shutil.move(old_site_config, site_config)
		else:
			frappe.log_error("Site config backup file not found", f"Missing: {old_site_config}")
			
		if backup_files:
			old_files_filename = files_filename.replace('files.tar', 'files'+add_word+'.tar')
			old_private_files = private_files.replace('files.tar', 'files'+add_word+'.tar')
			files_filename = base_file_path + old_files_filename[old_files_filename.rfind('/')+1:]
			private_files = base_file_path + old_private_files[old_private_files.rfind('/')+1:]
			
			if os.path.exists(old_files_filename):
				shutil.move(old_files_filename, files_filename)
			else:
				frappe.log_error("Public files backup not found", f"Missing: {old_files_filename}")
				files_filename = None
				
			if os.path.exists(old_private_files):
				shutil.move(old_private_files, private_files)
			else:
				frappe.log_error("Private files backup not found", f"Missing: {old_private_files}")
				private_files = None

	upload_file_to_s3(db_filename, folder, conn, bucket)
	upload_file_to_s3(site_config, folder, conn, bucket)

	if backup_files:
		#//// Neoffice — upstream tests only `if private_files:` / `if files_filename:`. The existence
		#//// check was added with the wizard/manual/demo relocation above (68d7f3a760), which can set
		#//// either name to None when the moved tarball is missing.
		if private_files and os.path.exists(private_files):
			upload_file_to_s3(private_files, folder, conn, bucket)

		if files_filename and os.path.exists(files_filename):
			upload_file_to_s3(files_filename, folder, conn, bucket)

	#//// Neoffice — added call (342e22a3bb, 2025-06-26 "feat(s3_backup): add backup file cleanup
	#//// functionality"): prune local backups older than 3 days once they are on S3. Upstream
	#//// leaves local retention to bench.
	delete_backups()


def upload_file_to_s3(filename, folder, conn, bucket):
	#//// Neoffice ▼▼▼ — upload_file_to_s3() is entirely ours; upstream is three lines
	#//// (`destpath = os.path.join(...)`, a print, `conn.upload_file(...)`). Ours prefixes the key
	#//// with "<domain> - <default company>/" so one bucket holds the whole fleet, then tries
	#//// three transports in order: rclone if the binary exists, a plain signed PUT for files
	#//// under 50 MB, and finally boto3 upload_file with a TransferConfig that switches to
	#//// multipart above 100 MB and streams from disk (c4d6f4d84f, 2026-07-16 "fix(s3-backup):
	#//// restore boto3 multipart upload, drop the silent >1GB skip" — the reverted 6af70ff99f /
	#//// b3990939fd pair had brought back a `if file_size_gb > 1: return` that dropped any tarball
	#//// over 1 GB while still reporting the backup as done; it had already cost blowbackshop both
	#//// files.tar and private-files.tar, and neoservice its files.tar). The rest is 68d7f3a760
	#//// (2025-07-03 "Update s3_backup_settings.py", empty message).
	#//// TO REVIEW at the merge: every failure path here calls frappe.log_error and RETURNS, so a
	#//// completely failed upload still lets take_backups_s3() send the "backup succeeded" mail;
	#//// the rclone branch writes the S3 secret to a temp file; and this file also lost its final
	#//// newline in the same pass. ▲▲▲ block runs to the end of the file.
	# Get url instance and add domain and change destpath
	domain = frappe.utils.get_url()
	company_folder = domain.replace('https://', '') + " - " + frappe.db.get_single_value('Global Defaults', 'default_company')
	destpath = company_folder + "/" + os.path.join(folder, os.path.basename(filename))
	
	try:
		# Check that file exists and get its size
		if not os.path.exists(filename):
			frappe.log_error("File not found for upload", f"Missing: {filename}")
			return
			
		file_size = os.path.getsize(filename)
		file_size_mb = file_size / (1024 * 1024)
		
		print(f"Uploading file: {filename} (Size: {file_size_mb:.2f} MB)")
		
		# Method 1: Try with rclone if available
		try:
			# Check if rclone is installed
			subprocess.run(['rclone', 'version'], capture_output=True, check=True)
			
			# Get S3 credentials
			doc = frappe.get_single("S3 Backup Settings")
			
			# Create temporary rclone config
			rclone_config = f"""[neoffice-s3]
type = s3
provider = Other
access_key_id = {doc.access_key_id}
secret_access_key = {doc.get_password("secret_access_key")}
endpoint = {doc.endpoint_url or "https://s3.amazonaws.com"}
acl = private
"""
			
			# Write config to temporary file
			import tempfile
			with tempfile.NamedTemporaryFile(mode='w', suffix='.conf', delete=False) as f:
				f.write(rclone_config)
				config_file = f.name
			
			try:
				# Upload with rclone
				# Create temporary directory for destination structure
				import tempfile
				with tempfile.TemporaryDirectory() as temp_dir:
					# Create complete structure with company name
					full_dest_path = os.path.join(temp_dir, company_folder, folder.rstrip('/'))
					os.makedirs(full_dest_path, exist_ok=True)
					
					# Copy file with correct name
					dest_file = os.path.join(full_dest_path, os.path.basename(filename))
					shutil.copy2(filename, dest_file)
					
					cmd = [
						'rclone', 'copy',
						'--config', config_file,
						'--no-check-certificate',  # If SSL issues
						'--s3-upload-cutoff', '5G',  # Avoid multipart for files < 5GB
						'--s3-chunk-size', '5G',  # Large chunks to avoid aws-chunked
						temp_dir,
						f'neoffice-s3:{bucket}/'
					]
					
					result = subprocess.run(cmd, capture_output=True, text=True, check=True)
					return
			finally:
				# Delete temporary config file
				os.unlink(config_file)
				
		except (subprocess.CalledProcessError, FileNotFoundError):
			pass
		
		# Method 2: For small files, use REST API directly
		if file_size_mb < 50:  # Only for files < 50MB
			try:
				import requests
				from datetime import datetime
				import hashlib
				import hmac
				
				# Prepare headers for AWS signature
				doc = frappe.get_single("S3 Backup Settings")
				endpoint = doc.endpoint_url or "https://s3.amazonaws.com"
				
				# Read file
				with open(filename, 'rb') as f:
					file_content = f.read()
				
				# Create URL
				url = f"{endpoint}/{bucket}/{destpath}"
				
				# Basic headers
				headers = {
					'Content-Type': 'application/octet-stream',
					'Content-Length': str(len(file_content))
				}
				
				# Direct upload with requests
				response = requests.put(
					url,
					data=file_content,
					headers=headers,
					auth=(doc.access_key_id, doc.get_password("secret_access_key"))
				)
				
				if response.status_code in [200, 201]:
					return
				else:
					frappe.log_error("REST upload failed", f"Status: {response.status_code}, Response: {response.text}")
					
			except Exception as e:
				frappe.log_error("Direct REST method failed", f"Error: {str(e)}")
		
		# Method 3: boto3 managed upload with automatic multipart for large objects.
		# A single put_object fails with EntityTooLarge on big files (the multi-hundred-MB
		# files.tar and multi-GB private-files.tar); upload_file transparently switches to
		# multipart above the threshold and streams from disk instead of reading the whole
		# file into memory.
		try:
			from boto3.s3.transfer import TransferConfig

			transfer_config = TransferConfig(
				multipart_threshold=100 * 1024 * 1024,  # switch to multipart above 100 MB
				multipart_chunksize=100 * 1024 * 1024,  # 100 MB parts
				use_threads=True,
			)
			conn.upload_file(filename, bucket, destpath, Config=transfer_config)
			return

		except Exception as e:
			frappe.log_error("Boto3 multipart upload failed", f"File: {filename}, Error: {str(e)}")

		# If everything fails
		frappe.log_error("All upload methods failed", f"Unable to upload {filename}. Consider using a different S3 service or contacting support.")

	except Exception as e:
		frappe.log_error("Upload error", f"File: {filename}, Error: {str(e)}")
		print("Error uploading: %s" % (e))