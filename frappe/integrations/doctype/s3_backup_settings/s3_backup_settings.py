# Copyright (c) 2017, Frappe Technologies and contributors
# License: MIT. See LICENSE
import os
import os.path

import boto3
from botocore.exceptions import ClientError
from rq.timeouts import JobTimeoutException

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
import zipfile


class S3BackupSettings(Document):
	def validate(self):
		if not self.enabled:
			return

		if not self.endpoint_url:
			self.endpoint_url = "https://s3.amazonaws.com"

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


@frappe.whitelist()
def take_backups_s3(retry_count=0):
	try:
		validate_file_size()
		backup_to_s3()
		send_email(True, "Amazon S3", "S3 Backup Settings", "notify_email")
	except JobTimeoutException:
		if retry_count < 2:
			args = {"retry_count": retry_count + 1}
			enqueue(
				"frappe.integrations.doctype.s3_backup_settings.s3_backup_settings.take_backups_s3",
				queue="long",
				timeout=1500,
				**args
			)
		else:
			notify()
	except Exception:
		notify()


def notify():
	error_message = frappe.get_traceback()
	send_email(False, "Amazon S3", "S3 Backup Settings", "notify_email", error_message)


def backup_to_s3():
	from neoffice_theme.events import delete_backups
	def zip_directory(folder_path, zip_path):
		with zipfile.ZipFile(zip_path, mode='w') as zipf:
			len_dir_path = len(folder_path)
			for root, _, files in os.walk(folder_path):
				for file in files:
					file_path = os.path.join(root, file)
					zipf.write(file_path, file_path[len_dir_path:])
					
	def IsPathValid(path, ignoreDir, ignoreExt):
		splited = None
		if os.path.isfile(path):
			if ignoreExt:
				_, ext = os.path.splitext(path)
				if ext in ignoreExt:
					return False

			splited = os.path.dirname(path).split('\\/')
		else:
			if not ignoreDir:
				return True
			splited = path.split('\\/')

		for s in splited:
			if s in ignoreDir:  # You can also use set.intersection or [x for],
				return False

		return True

	def zipDirHelper(path, rootDir, zf, ignoreDir = [], ignoreExt = []):
		# zf is zipfile handle
		if os.path.isfile(path):
			if IsPathValid(path, ignoreDir, ignoreExt):
				relative = os.path.relpath(path, rootDir)
				zf.write(path, relative)
			return

		ls = os.listdir(path)
		for subFileOrDir in ls:
			if not IsPathValid(subFileOrDir, ignoreDir, ignoreExt):
				continue

			joinedPath = os.path.join(path, subFileOrDir)
			zipDirHelper(joinedPath, rootDir, zf, ignoreDir, ignoreExt)

	def ZipDir(path, zf, ignoreDir = [], ignoreExt = []):
		rootDir = path if os.path.isdir(path) else os.path.dirname(path)
		zipDirHelper(path, rootDir, zf, ignoreDir, ignoreExt)
		pass

	from frappe.utils import get_backups_path
	from frappe.utils.backups import new_backup

	doc = frappe.get_single("S3 Backup Settings")
	bucket = doc.bucket
	backup_files = cint(doc.backup_files)

	conn = boto3.client(
		"s3",
		aws_access_key_id=doc.access_key_id,
		aws_secret_access_key=doc.get_password("secret_access_key"),
		endpoint_url=doc.endpoint_url or "https://s3.amazonaws.com",
	)

	if frappe.flags.create_new_backup:
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
	else:
		if backup_files:
			db_filename, site_config, files_filename, private_files = get_latest_backup_file(
				with_files=backup_files
			)

			if not files_filename or not private_files:
				generate_files_backup()
				db_filename, site_config, files_filename, private_files = get_latest_backup_file(
					with_files=backup_files
				)

		else:
			db_filename, site_config = get_latest_backup_file()

	folder = os.path.basename(db_filename)[:15] + "/"
	# for adding datetime to folder name
	
	base_file_path = os.path.join(get_backups_path(), os.path.basename(db_filename)[:15])
	theZipFile_cloud = zipfile.ZipFile(base_file_path + '_cloud.zip', 'w')
	ZipDir('/mnt/neoffice/data', theZipFile_cloud, ignoreDir=[], ignoreExt=[".log"])
	zip_directory('/mnt/neoffice/data', base_file_path + '_cloud.zip')
	upload_file_to_s3(base_file_path + '_cloud.zip', folder, conn, bucket)
	#os.remove(base_file_path + '_cloud.zip')

	upload_file_to_s3(db_filename, folder, conn, bucket)
	upload_file_to_s3(site_config, folder, conn, bucket)

	if backup_files:
		if private_files:
			upload_file_to_s3(private_files, folder, conn, bucket)

		if files_filename:
			upload_file_to_s3(files_filename, folder, conn, bucket)

	delete_backups()


def upload_file_to_s3(filename, folder, conn, bucket):
	domain = frappe.db.get_value("Neoffice Woocommerce Settings", "Neoffice Woocommerce Settings", "woocommerce_server_url")
	frappe.log_error("parsed json", domain)
	destpath = domain.replace('https://', '').replace('/web', '') + " - " + frappe.db.get_single_value('Global Defaults', 'default_company') + "/" + os.path.join(folder, os.path.basename(filename))
	try:
		print("Uploading file:", filename)
		conn.upload_file(filename, bucket, destpath)  # Requires PutObject permission

	except Exception as e:
		frappe.log_error()
		print("Error uploading: %s" % (e))
