import os
import logging
import uuid
from typing import Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

try:
    import boto3
    from botocore.config import Config
    _S3_AVAILABLE = True
except ImportError:
    logger.warning("boto3 not available. Install with 'pip install boto3'")
    _S3_AVAILABLE = False


class S3Service:
    """S3/DigitalOcean Spaces client for uploading source files."""

    MIME_TYPES = {
        ".csv": "text/csv",
        ".json": "application/json",
        ".parquet": "application/vnd.apache.parquet",
    }

    def __init__(
        self,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        bucket_name: Optional[str] = None,
        region: Optional[str] = None
    ):
        self.access_key = access_key or os.getenv("S3_ACCESS_KEY")
        self.secret_key = secret_key or os.getenv("S3_SECRET_KEY")
        self.endpoint_url = endpoint_url or os.getenv("S3_ENDPOINT_URL", "https://atl1.digitaloceanspaces.com")
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET_NAME", "optisql-uploads")
        self.region = region or os.getenv("S3_REGION", "us-east-1")
        self._client = None

    @property
    def is_available(self) -> bool:
        return _S3_AVAILABLE and bool(self.access_key and self.secret_key)

    def _get_client(self):
        if not _S3_AVAILABLE:
            raise RuntimeError("boto3 not available. Install with 'pip install boto3'")

        if not self.access_key or not self.secret_key:
            raise RuntimeError("S3 credentials not configured. Set S3_ACCESS_KEY and S3_SECRET_KEY")

        if self._client is None:
            self._client = boto3.client(
                's3',
                endpoint_url=self.endpoint_url,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                region_name=self.region,
                config=Config(signature_version='s3v4')
            )
            logger.info(f"Connected to S3 at {self.endpoint_url}")

        return self._client

    def get_mime_type(self, filename: str) -> str:
        ext = os.path.splitext(filename)[1].lower()
        return self.MIME_TYPES.get(ext, "application/octet-stream")

    def upload_file(self, file_content: bytes, filename: str, content_type: Optional[str] = None) -> str:
        """
        Upload file to S3 with public-read ACL.

        Returns the public S3 URI.
        """
        if not self.is_available:
            raise RuntimeError("S3 not available or not configured")

        client = self._get_client()

        key = filename

        mime_type = content_type or self.get_mime_type(filename)

        logger.info(f"Uploading {filename} ({len(file_content)} bytes) to s3://{self.bucket_name}/{key}")

        client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=file_content,
            ContentType=mime_type,
            ACL='public-read'
        )

        # Return S3 URI format for backend compatibility
        s3_uri = f"s3://{self.bucket_name}/{key}"

        logger.info(f"Uploaded to {s3_uri}")
        return s3_uri

    async def upload_file_async(self, file_content: bytes, filename: str, content_type: Optional[str] = None) -> str:
        """Async wrapper for upload_file."""
        return self.upload_file(file_content, filename, content_type)

    def download_file(self, key: str) -> bytes:
        """
        Download file from S3 by key.

        Args:
            key: The S3 object key (path within bucket)

        Returns:
            File contents as bytes
        """
        if not self.is_available:
            raise RuntimeError("S3 not available or not configured")

        client = self._get_client()

        logger.info(f"Downloading s3://{self.bucket_name}/{key}")

        response = client.get_object(Bucket=self.bucket_name, Key=key)
        content = response['Body'].read()

        logger.info(f"Downloaded {len(content)} bytes from {key}")
        return content

    async def download_file_async(self, key: str) -> bytes:
        """Async wrapper for download_file."""
        return self.download_file(key)


_s3_service: Optional[S3Service] = None


def get_s3_service() -> S3Service:
    global _s3_service
    if _s3_service is None:
        _s3_service = S3Service()
    return _s3_service
