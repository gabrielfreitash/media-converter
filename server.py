import asyncio
import os
import pickle
from functools import wraps

import flask

import redis_client
from models import ConvertRequest, ConvertResponse

app = flask.Flask(__name__)

auth_token = os.getenv("AUTH_TOKEN")


def require_auth(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        token = flask.request.headers.get("Authorization")
        if not auth_token:
            raise ValueError("AUTH_TOKEN environment variable is not set.")
        if not token:
            return flask.abort(401)
        if token == f"Bearer {auth_token}" or token == auth_token:
            return func(*args, **kwargs)
        return flask.abort(401)

    return wrapper


def require_auth_async(func):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        token = flask.request.headers.get("Authorization")
        if not auth_token:
            raise ValueError("AUTH_TOKEN environment variable is not set.")
        if not token:
            return flask.abort(401)
        if token == f"Bearer {auth_token}" or token == auth_token:
            return await func(*args, **kwargs)
        return flask.abort(401)

    return wrapper


@app.route("/hc")
def index():
    return "ok"


@app.route("/convert")
@require_auth_async
async def convert():
    request = ConvertRequest(**flask.request.json)
    redis_client.redis_client.publish(
        redis_client.REQUESTS_CHANNEL,
        pickle.dumps(request),
    )

    if request.async_mode:
        return "ok"

    subscriber = redis_client.redis_client.pubsub()
    subscriber.subscribe(redis_client.RESPONSES_CHANNEL)
    while True:
        message = subscriber.get_message()
        if message:
            # Ignore non-message control frames (e.g., subscribe confirmations) which often carry integers
            if message.get("type") != "message":
                continue

            data = message.get("data")
            if data is None:
                continue

            # Only attempt to unpickle when we have bytes-like data
            if isinstance(data, (bytes, bytearray)):
                response: ConvertResponse = pickle.loads(data)
            else:
                # Some clients may send strings; try to decode to bytes as a fallback
                if isinstance(data, str):
                    try:
                        response: ConvertResponse = pickle.loads(data.encode())
                    except Exception:
                        # Not a pickled payload we can handle
                        continue
                else:
                    # Unsupported data type (e.g., int), skip
                    continue

            if response.request.uuid == request.uuid:
                return response.data
        await asyncio.sleep(0.1)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5095, debug=True)
