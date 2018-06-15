import csv, os, io, contextlib
import shutil
from urllib.parse import urljoin

import logging
from botocore.client import Config
from botocore.exceptions import ClientError
import boto3
import json
import datetime
from contextlib import contextmanager
from tempfile import mkdtemp

logging.getLogger('botocore').setLevel(logging.INFO)


@contextmanager
def temp_dir(*args, **kwargs):
    dir = mkdtemp(*args, **kwargs)
    try:
        yield dir
    except Exception:
        if os.path.exists(dir):
            os.rmdir(dir)
        raise


@contextmanager
def temp_file(*args, **kwargs):
    with temp_dir(*args, **kwargs) as dir:
        file = os.path.join(dir, "temp")
        try:
            yield file
        except Exception:
            if os.path.exists(file):
                os.unlink(file)
            raise


# def download(s3, object_name, file_name):
#     res = s3.get_object(Bucket=bucket, Key=object_name)
#     with open(file_name, "wb") as f:
#         f.write(res["Body"].read())
#     return bool(res)
#
#
# @contextlib.contextmanager
# def temp_download(s3, object_name):
#     with temp_file() as file_name:
#         download(s3, bucket, object_name, file_name)
#         yield file_name


# @contextlib.contextmanager
# def csv_writer(s3, object_name, public_bucket=False):
#     with temp_file() as filename:
#         with open(filename, "w") as f:
#             yield csv.writer(f)
#         write(s3, bucket, object_name, file_name=filename, public_bucket=public_bucket)


class FileBasedStorage:

    def __init__(self):
        self.base_path = '/var/datapackages/budgetkey-files/'

    def get_path(self, object_name):
        path = os.path.join(self.base_path, object_name)
        return path

    def exists(self, object_name, min_size=None):
        path = self.get_path(object_name)
        if os.path.exists(path):
            if os.path.isfile(path):
                s = os.lstat(path).st_size
                if min_size is not None:
                    return s > min_size
                else:
                    return True
        return False

    @staticmethod
    def get_read_object_data(data):
        return io.BytesIO(data)

    @staticmethod
    def get_write_object_data(data):
        if isinstance(data, str):
            data = data.encode()
        return io.BytesIO(data)

    def write(self, object_name, data=None, file_name=None):
        path = self.get_path(object_name)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if file_name is not None and data is None:
            shutil.copy(file_name, path)
        elif data is not None and file_name is None:
            with open(path, 'w') as o:
                o.write(data)
        else:
            raise AttributeError()
        return path

    def delete(self, object_name):
        if self.exists(object_name):
            self.internal_delete(object_name)

    def internal_delete(self, object_name):
        path = self.get_path(object_name)
        os.unlink(path)


class ObjectStorage(FileBasedStorage):

    def __init__(self):
        super(ObjectStorage, self).__init__()
        self.bucket_name = 'budgetkey-files'
        config = Config(signature_version=os.environ["S3_SIGNATURE_VERSION"]) if os.environ.get("S3_SIGNATURE_VERSION") else None
        self.s3 = False
        self.s3_endpoint_url = os.environ.get("S3_ENDPOINT_URL", 'https://s3.amazonaws.com')
        if os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY"):
            self.s3 = boto3.client('s3',
                                   endpoint_url=self.s3_endpoint_url,
                                   aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
                                   aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
                                   config=config,
                                   region_name=os.environ.get("S3_REGION"))

    def urlfor(self, object_name):
        return os.path.join(self.s3_endpoint_url, self.bucket_name, object_name)

    def exists(self, object_name, min_size=None):
        if not self.s3:
            return super(ObjectStorage, self).exists(object_name, min_size)
        try:
            res = self.s3.head_object(Bucket=self.bucket_name, Key=object_name)
        except Exception:
            res = False
        if res and min_size:
            return res['ContentLength'] > min_size
        else:
            return bool(res)

    def write(self, object_name, data=None, file_name=None, create_bucket=True, public_bucket=False):
        if not self.s3:
            return super(ObjectStorage, self).write(object_name, data, file_name)
        try:
            if file_name is not None and data is None:
                self.s3.put_object(Body=open(file_name, 'rb'), Bucket=self.bucket_name, Key=object_name, ACL='public-read')
            elif data is not None and file_name is None:
                self.s3.put_object(Body=self.get_write_object_data(data), Bucket=self.bucket_name, Key=object_name, ACL='public-read')
            else:
                raise AttributeError()
            return self.urlfor(object_name)
        except ClientError as e:
            logging.exception('Error WRITING')
            if create_bucket:
                self.s3.create_bucket(Bucket=self.bucket_name)
                if public_bucket:
                    try:
                        self.s3.put_bucket_policy(Bucket=self.bucket_name, Policy=json.dumps({
                            "Version": str(datetime.datetime.now()).replace(" ", "-"),
                            "Statement": [{"Sid": "AddPerm", "Effect": "Allow", "Principal": "*",
                                           "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::{}/*".format(self.bucket_name)]}]}))
                    except ClientError as e2:
                        logging.exception('Failed to put bucket policy', exc_info=e2)
                        pass
                return self.write(object_name, data=data, file_name=file_name, create_bucket=False, public_bucket=public_bucket)
            else:
                raise

    def internal_delete(self, object_name):
        self.s3.delete_object(Bucket=self.bucket_name, Key=object_name)

#
# def read(s3, bucket, object_name):
#     res = s3.get_object(Bucket=bucket, Key=object_name)
#     return res["Body"].read()
#
#
object_storage = ObjectStorage()