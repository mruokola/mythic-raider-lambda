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

env_releases_bucket = os.environ["RELEASES_BUCKET"].strip()
env_releases_key = os.environ["RELEASES_KEY"].strip()
env_initrd = os.environ["INITRD"].strip()
env_initrd_bucket = os.environ["INITRD_BUCKET"].strip()
s3_nosign = boto3.client(
    "s3", config=botocore.config.Config(signature_version=botocore.UNSIGNED)
)
s3 = boto3.client("s3")
initrd_key = "initrd" + str(int(time.time()))


def get_releases(bucket, key):
    obj = s3_nosign.get_object(Bucket=bucket, Key=key)
    val = None
    with io.BytesIO() as buf:
        for chunk in obj["Body"].iter_chunks():
            buf.write(chunk)
        val = buf.getvalue()
    return json.loads(val) if val else None


def get_latest_release(releases):
    _release = None
    _date = None
    for release in releases:
        release_date = datetime.strptime(release["date"], "%Y-%m-%d")
        if not _date or _date < release_date:
            _date = release_date
            _release = release
    return _release


releases = get_releases(env_releases_bucket, env_releases_key)
if not releases:
    print("unable to get releases")
    sys.exit(1)

release = get_latest_release(releases)
if not release:
    print("unable to get latest release")
    sys.exit(1)

print("Release '{}' url '{}'".format(release["name"], release["url"]))

url = urlparse(release["url"])
if url.netloc != "s3.amazonaws.com":
    print("unsupported url netloc")
    sys.exit(1)

m = re.match(r"^/([^/]+)?/(.+)$", url.path)
if not m:
    print("unable to find bucket and key from url path")
    sys.exit(1)

bucket = m.group(1)
key = m.group(2)
print("Reading '{}' from bucket '{}'".format(key, bucket))
obj = s3_nosign.get_object(Bucket=bucket, Key=key)
print("Size: {} MB".format(obj["ContentLength"] / 1024 / 1024))

print("Opening")
with tempfile.TemporaryFile() as temp_file:
    # read the s3 file and write it into a temp file
    with libarchive.stream_reader(obj["Body"], format_name="zip") as archive:
        for entry in archive:
            if entry.pathname == env_initrd:
                print("Extracting '{}' into temp file '{}'".format(
                        entry.pathname, temp_file.name))
                for block in entry.get_blocks():
                    temp_file.write(block)
                break
    # reset buffer read/write position
    temp_file.seek(0)
    # put to bucket
    resp = s3.put_object(
        Bucket=env_initrd_bucket,
        Key=initrd_key,
        Body=temp_file
    )
    print("Stored file '{}/{}' at version {}".format(
        env_initrd_bucket, initrd_key, resp["VersionId"]
    ))
