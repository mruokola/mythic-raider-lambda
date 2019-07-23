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

env_releases_bucket = os.environ["RELEASES_BUCKET"].strip()
env_releases_key = os.environ["RELEASES_KEY"].strip()
env_initrd = os.environ["INITRD"].strip()
env_files = os.environ["FILES"].strip()
env_files = env_files.split(",")
env_modulus = os.environ["MODULUS"].strip()
env_modulus2 = os.environ["MODULUS2"].strip()
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
obj = s3.get_object(Bucket=bucket, Key=key)
print("Size: {} MB".format(obj["ContentLength"] / 1024 / 1024))

temp_filename = None
with tempfile.NamedTemporaryFile(delete=False) as fil:
    temp_filename = fil.name
    print("Reading into temp file '{}'".format(temp_filename))
    for chunk in obj["Body"].iter_chunks():
        fil.write(chunk)

print("Opening")
temp_filename2 = None
with open(temp_filename, "rb") as fil:
    archive_reader = libarchive.stream_reader(fil, format_name="zip")
    with archive_reader as archive:
        for entry in archive:
            if entry.pathname == env_initrd:
                with tempfile.NamedTemporaryFile(delete=False) as fil:
                    print("Extracting '{}' into temp file '{}'".format(entry.pathname, fil.name))
                    temp_filename2 = fil.name
                    for block in entry.get_blocks():
                        fil.write(block)

# scan the file and target the xz archive magic specifically
# https://github.com/ReFirmLabs/binwalk/blob/798ac5/src/binwalk/magic/compressed#L98-L99
scan = binwalk.scan(
    temp_filename2,
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

files_bufs = []
# open the file for reading
with open(temp_filename2, "rb") as fh:
    # seek to rootfs offset
    fh.seek(rootfs.offset)
    with libarchive.stream_reader(fh, format_name="cpio") as archive:
        for entry in archive:
            if len(files_bufs) == len(env_files):
                break
            if entry.pathname in env_files:
                buf = io.BytesIO()
                for block in entry.get_blocks():
                    buf.write(block)
                buf.seek(0)
                files_bufs += [(entry.pathname, buf)]


print("Done, deleting temp file")
os.remove(temp_filename)
os.remove(temp_filename2)




from contextlib import contextmanager


@contextmanager
def get_zip_entry(stream, entry_name):
    import libarchive
    with libarchive.stream_reader(stream, format_name="zip") as archive:
        for entry in archive:
            if entry.pathname == entry_name:
                yield entry


@contextmanager
def get_archive_entries(stream, entry_names=[], format_name="zip"):
    import libarchive
    with libarchive.stream_reader(stream, format_name=format_name) as archive:
        for entry in archive:
            if entry_names and entry.pathname in entry_names:
                yield entry
            elif not entry_names:
                yield entry


def magic_scan(buffer, includes=[]):
    import binwalk.core.settings
    import binwalk.core.magic
    settings = binwalk.core.settings.Settings()
    magic = binwalk.core.magic.Magic(include=includes)
    for f in settings.system.magic + settings.user.magic:
        magic.load(f)
    while True:
        data = buffer.read(1024 * 8)
        if not data:
            return None
        scan = magic.scan(data.decode("latin1"), len(data))
        if scan:
            return scan[0]
    return None


import time
st = time.time()
temp_filename = "file.zip"
initrd_temp = None
with open(temp_filename, "rb") as zip_buf:
    with get_archive_entries(zip_buf, ["bzroot"], "zip") as entry:
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False) as temp_buf:
            initrd_temp = temp_buf.name
            for block in entry.get_blocks():
                temp_buf.write(block)

scan = binwalk.scan(initrd_temp, quiet=True, signature=True, include=[r"^xz compressed data$"])
scan = scan[0].results[0]
rootfs_temp = None
with open(initrd_temp, "rb") as buf:
    buf.seek(scan.offset)
    with tempfile.NamedTemporaryFile(delete=False) as temp_buf:
        rootfs_temp = temp_buf.name
        while True:
            data = buf.read(1024 * 8)
            if not data:
                break
            temp_buf.write(data)

os.remove(initrd_temp)
buffers = []
with open(rootfs_temp, "rb") as buf:
    with libarchive.stream_reader(buf, format_name="cpio") as archive:
        for entry in archive:
            if len(buffers) == len(env_files):
                break
            if entry.pathname in env_files:
                buffer = io.BytesIO()
                for block in entry.get_blocks():
                    buffer.write(block)
                buffer.seek(0)
                buffers += [(entry.pathname, buffer)]

os.remove(rootfs_temp)
import os
import time
for filename, buffer in buffers:
    modulus = bytearray.fromhex(env_modulus)
    modulus2 = bytearray.fromhex(env_modulus2)
    data = bytearray(buffer.read())
    offset = data.find(modulus)
    data = data[:offset] + modulus2 + data[offset + 256:]
    filename = os.path.basename(filename)
    with open("{}_patched_{}".format(filename, int(time.time())), "wb") as buf:
        buf.write(buffer)
