import base64
import uuid
from dataclasses import dataclass
from typing import Optional


def generate_uuid():
    return str(uuid.uuid4())


@dataclass
class ConvertRequest:
    data: bytes
    async_mode: bool
    extension: str
    webhook_url: Optional[str]
    webhook_headers: Optional[dict]

    def __post_init__(self):
        self.uuid = generate_uuid()
        if isinstance(self.data, str):
            # base64

            self.data = base64.b64decode(self.data)
        self.extension = self.extension.replace(".", "")


@dataclass
class ConvertResponse:
    data: bytes
    request: ConvertRequest
