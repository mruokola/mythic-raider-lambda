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

releases_bucket = os.environ["RELEASES_BUCKET"].strip()
releases_key = os.environ["RELEASES_KEY"].strip()
s3 = boto3.client(
    "s3", config=botocore.config.Config(signature_version=botocore.UNSIGNED)
)


def get_releases(bucket, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
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


releases = get_releases(releases_bucket, releases_key)
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
obj = s3.get_object(Bucket=bucket, Key=key)
print("Size: {} MB".format(obj["ContentLength"] / 1024 / 1024))

temp_filename = None
with tempfile.NamedTemporaryFile(delete=False) as fil:
    temp_filename = fil.name
    print("Reading into temp file '{}'".format(temp_filename))
    for chunk in obj["Body"].iter_chunks():
        fil.write(chunk)

print("Opening")
with open(temp_filename, "rb") as fil:
    archive_reader = libarchive.stream_reader(fil, format_name="zip")
    with archive_reader as archive:
        print(archive)
        for entry in archive:
            print(entry)

print("Done, deleting temp file")
os.remove(temp_filename)
