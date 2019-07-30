from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding
from cryptography.hazmat.primitives.serialization import PrivateFormat
from cryptography.hazmat.primitives.serialization import PublicFormat
from cryptography.hazmat.primitives.serialization import NoEncryption
import boto3
import os
import time


exponent = 0x10001
size = 2048
env_patch_info_bucket = os.environ["PATCH_INFO_BUCKET"].strip()
s3 = boto3.client("s3")


print("Generating private key")
private_key = rsa.generate_private_key(
    public_exponent=exponent,
    key_size=size,
    backend=default_backend()
)
private_bytes = private_key.private_bytes(
    encoding=Encoding.PEM,
    format=PrivateFormat.PKCS8,
    encryption_algorithm=NoEncryption()
)
print(private_bytes)
public_key = private_key.public_key()
public_bytes = public_key.public_bytes(
    encoding=Encoding.PEM,
    format=PublicFormat.SubjectPublicKeyInfo
)
print(public_bytes)
modulus = public_key.public_numbers().n
modulus_bytes = bytearray.fromhex("{:0512x}".format(modulus))

t = str(int(time.time()))
resp = s3.put_object(
    Bucket=env_patch_info_bucket,
    Key="private_key-{}.pem".format(t),
    Body=private_bytes
)
print("Stored file '{}/{}' at version {}".format(
    env_patch_info_bucket, "private_key-{}.pem".format(t), resp["VersionId"]
))
resp = s3.put_object(
    Bucket=env_patch_info_bucket,
    Key="public_key-{}.pem".format(t),
    Body=public_bytes
)
print("Stored file '{}/{}' at version {}".format(
    env_patch_info_bucket, "public_key-{}.pem".format(t), resp["VersionId"]
))
resp = s3.put_object(
    Bucket=env_patch_info_bucket,
    Key="modulus-{}.bin".format(t),
    Body=modulus_bytes
)
print("Stored file '{}/{}' at version {}".format(
    env_patch_info_bucket, "modulus-{}.bin".format(t), resp["VersionId"]
))
