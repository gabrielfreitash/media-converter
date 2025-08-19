# filepath: /home/gabriel/media-converter/converter.py
import base64
import io
import os
import pickle
import sys
import uuid as py_uuid
from typing import Optional

from PIL import Image, ImageColor, ImageOps
from pydub import AudioSegment

from models import ConvertRequest, ConvertResponse
from redis_client import REQUESTS_CHANNEL, RESPONSES_CHANNEL, redis_client

# Configuration via environment variables
TARGET_WIDTH = int(os.getenv("IMAGE_TARGET_WIDTH", "1024"))
TARGET_HEIGHT = int(os.getenv("IMAGE_TARGET_HEIGHT", "1024"))
IMAGE_BG_COLOR = os.getenv("IMAGE_BG_COLOR", "#FFFFFF")
# Parse color to RGB tuple for compatibility with type-checkers and Pillow
IMAGE_BG_COLOR_RGB = ImageColor.getrgb(IMAGE_BG_COLOR)
AUDIO_BITRATE = os.getenv("AUDIO_BITRATE", "192k")
LOCK_TTL_SECONDS = int(os.getenv("LOCK_TTL_SECONDS", "600"))  # 10 minutes

INSTANCE_ID = str(py_uuid.uuid4())

# Lua script for safe lock release (delete only if value matches)
UNLOCK_LUA = (
    "if redis.call('get', KEYS[1]) == ARGV[1] then "
    "return redis.call('del', KEYS[1]) else return 0 end"
)

# Select a robust resampling filter compatible across Pillow versions (with safe fallbacks)
try:
    RESAMPLE_FILTER: int = getattr(getattr(Image, "Resampling", Image), "LANCZOS")
except Exception:
    RESAMPLE_FILTER = getattr(Image, "LANCZOS", getattr(Image, "BICUBIC", 3))


def b64_decode(data_b64: str) -> bytes:
    return base64.b64decode(data_b64.encode("utf-8"))


def acquire_lock(uid: str) -> bool:
    key = f"converter:lock:{uid}"
    # SET key value NX EX ttl
    return bool(redis_client.set(key, INSTANCE_ID, nx=True, ex=LOCK_TTL_SECONDS))


def release_lock(uid: str) -> None:
    key = f"converter:lock:{uid}"
    try:
        redis_client.eval(UNLOCK_LUA, 1, key, INSTANCE_ID)
    except Exception:
        # Best-effort unlock
        pass


def is_image_extension(ext: str) -> bool:
    ext = ext.lower().lstrip(".")
    return ext in {
        "jpg",
        "jpeg",
        "png",
        "bmp",
        "gif",
        "tif",
        "tiff",
        "webp",
    }


def is_audio_extension(ext: str) -> bool:
    ext = ext.lower().lstrip(".")
    return ext in {
        "mp3",
        "wav",
        "ogg",
        "flac",
        "aac",
        "m4a",
        "wma",
        "aiff",
        "oga",
        "opus",
        "amr",
        "mp4",
        "3gp",
    }


def convert_image_to_jpg_resized(raw: bytes) -> bytes:
    with Image.open(io.BytesIO(raw)) as im:
        im = ImageOps.exif_transpose(im)
        # Use RGBA to preserve transparency if present, then resize
        im = im.convert("RGBA")
        target_size = (TARGET_WIDTH, TARGET_HEIGHT)
        # Resize while keeping aspect ratio, then pad to target size
        resized = ImageOps.contain(im, target_size, method=RESAMPLE_FILTER)
        # Create an RGBA canvas filled with the desired background color (opaque)
        bg_color_rgba = IMAGE_BG_COLOR_RGB + (255,)
        canvas = Image.new("RGBA", target_size, bg_color_rgba)
        x = (target_size[0] - resized.width) // 2
        y = (target_size[1] - resized.height) // 2
        # Paste using the resized image as mask to honor its alpha channel
        canvas.paste(resized, (x, y), resized)
        # Convert to RGB before saving as JPEG (JPEG has no alpha)
        final = canvas.convert("RGB")
        out = io.BytesIO()
        final.save(out, format="JPEG", quality=85, optimize=True, progressive=True)
        return out.getvalue()


def convert_audio_to_mp3(raw: bytes, source_ext: Optional[str]) -> bytes:
    buf = io.BytesIO(raw)
    # Let ffmpeg auto-detect; pass format if extension known
    fmt = None
    if source_ext and is_audio_extension(source_ext):
        fmt = source_ext.lower().lstrip(".")
    seg = AudioSegment.from_file(buf, format=fmt)
    out_buf = io.BytesIO()
    seg.export(out_buf, format="mp3", bitrate=AUDIO_BITRATE)
    return out_buf.getvalue()


def publish_response_obj(resp: ConvertResponse) -> None:
    redis_client.publish(RESPONSES_CHANNEL, pickle.dumps(resp))


def process_message(msg_data: bytes) -> None:
    try:
        request: ConvertRequest = pickle.loads(msg_data)
    except Exception:
        # Ignore malformed messages since the server expects pickled dataclasses
        return

    uid = getattr(request, "uuid", None) or str(py_uuid.uuid4())

    if not acquire_lock(uid):
        # Another worker got it; ignore
        return

    try:
        raw_data = request.data
        if isinstance(raw_data, str):
            # Accept base64 string payloads transparently
            raw = b64_decode(raw_data)
        elif isinstance(raw_data, (bytes, bytearray)):
            raw = bytes(raw_data)
        else:
            raise ValueError("unsupported data type; expected bytes or base64 string")

        ext = (request.extension or "").lower().lstrip(".")

        # Decide conversion by extension (with fallback autodetect)
        if is_audio_extension(ext):
            out_bytes = convert_audio_to_mp3(raw, ext)
        elif is_image_extension(ext):
            out_bytes = convert_image_to_jpg_resized(raw)
        else:
            try:
                out_bytes = convert_image_to_jpg_resized(raw)
            except Exception:
                out_bytes = convert_audio_to_mp3(raw, None)

        response = ConvertResponse(data=out_bytes, request=request)
        publish_response_obj(response)

    except Exception:
        # On error, publish an empty response to unblock the caller
        try:
            publish_response_obj(ConvertResponse(data=b"", request=request))
        except Exception:
            pass
    finally:
        release_lock(uid)


def main() -> None:
    pubsub = redis_client.pubsub()
    pubsub.subscribe(REQUESTS_CHANNEL)
    print(
        f"converter worker started. instance={INSTANCE_ID} listening on {REQUESTS_CHANNEL}",
        flush=True,
    )
    try:
        for message in pubsub.listen():
            if not message or message.get("type") != "message":
                continue
            data = message.get("data")
            if isinstance(data, (bytes, bytearray)):
                process_message(data)
            elif isinstance(data, str):
                process_message(data.encode("utf-8"))
    except KeyboardInterrupt:
        print("converter worker stopping...", flush=True)
    finally:
        try:
            pubsub.close()
        except Exception:
            pass


if __name__ == "__main__":
    # Ensure dependencies give clear errors if missing at runtime
    try:
        main()
    except Exception as e:
        print(f"fatal error: {e}", file=sys.stderr, flush=True)
        sys.exit(1)
