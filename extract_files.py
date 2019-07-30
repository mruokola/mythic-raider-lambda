import binwalk
import boto3
import botocore
from datetime import datetime
import io
import json
import libarchive
import os
import re
import sys
import tempfile
from urllib.parse import urlparse
import time

env_files_bucket = os.environ["FILES_BUCKET"].strip()
env_initrd_bucket = os.environ["INITRD_BUCKET"].strip()
env_initrd_key = os.environ["INITRD_KEY"].strip()
env_files = os.environ["FILES"].strip()
env_files = env_files.split(",")
s3 = boto3.client("s3")


with tempfile.TemporaryFile() as temp_file:
    # read the s3 file and write it to disk
    obj = s3.get_object(Bucket=env_initrd_bucket, Key=env_initrd_key)
    for chunk in obj["Body"].iter_chunks():
        temp_file.write(chunk)
    # reset buffer position/offset
    temp_file.seek(0)
    # scan the file and target the xz archive magic specifically
    # https://github.com/ReFirmLabs/binwalk/blob/798ac5/src/binwalk/magic/compressed#L98-L99
    scan = binwalk.scan(
        temp_file,
        quiet=True,
        signature=True,
        include=r"^xz compressed data$"
    )
    # assume scan was successful
    scan = scan[0]  # TODO unsafe
    # get the first result of the scan
    rootfs = scan.results[0]  # TODO unsafe
    # determine the size of the file
    # since rootfs.size is usually 0 and the archive is at the end of the file,
    # this can be deducted by subtracting the archive's offset from the file size
    size = rootfs.size or rootfs.file.size - rootfs.offset  # basically just the rest of the file most of the time
    # set buffer position/offset to rootfs offset
    temp_file.seek(rootfs.offset)
    files_read = 0
    with libarchive.stream_reader(temp_file, format_name="cpio") as archive:
        for entry in archive:
            if entry.pathname in env_files:
                with tempfile.TemporaryFile() as mini_file:
                    # write the file to disk
                    for block in entry.get_blocks():
                        mini_file.write(block)
                    # reset buffer position
                    mini_file.seek(0)
                    # put to s3
                    file_key = os.path.basename(entry.pathname) + \
                        str(int(time.time()))
                    resp = s3.put_object(
                        Bucket=env_files_bucket,
                        Key=file_key,
                        Body=mini_file
                    )
                    print("Stored file '{}/{}' at version {}".format(
                        env_files_bucket, file_key, resp["VersionId"]
                    ))
                files_read += 1
                if files_read == len(env_files):
                    break
