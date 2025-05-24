import httpx
from fsspec import AbstractFileSystem


def bootstrap_project(client: httpx.Client) -> None:
    payload = {"accept-terms-of-use": True}

    r = client.post("/management/v1/bootstrap", json=payload)
    r.raise_for_status()


def create_bucket(fs: AbstractFileSystem, bucket_name: str) -> None:
    try:
        return fs.mkdir(bucket_name)
    except FileExistsError:
        return None


def create_warehouse(client: httpx.Client, storage_bucket: str = "warehouse") -> None:
    payload = {
        "warehouse-name": "lakehouse",
        "project-id": "00000000-0000-0000-0000-000000000000",
        "storage-profile": {
            "type": "s3",
            "bucket": storage_bucket,
            "assume-role-arn": None,
            "endpoint": "http://minio:9000",
            "region": "us-east-1",
            "path-style-access": True,
            "flavor": "minio",
            "sts-enabled": True,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "minio",
            "aws-secret-access-key": "minio1234",
        },
    }
    r = client.post("/management/v1/warehouse", json=payload)
    r.raise_for_status()
