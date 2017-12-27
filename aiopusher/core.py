import asyncio
import websockets
import json
import traceback
from inspect import iscoroutinefunction
from collections import defaultdict

class PusherException(Exception):
    def __init__(self, code, message):
        super().__init__("{} (code: {})".format(message, code))

async def call_function_or_coroutine(f, *args, **kwargs):
    if iscoroutinefunction(f):
        return await f(*args, **kwargs)
    return f(*args, **kwargs)

class Client:

    VERSION = "0.0.1"
    CLIENT_ID = "python_aiopusher"
    PUSHER_PROTOCOL = 7

    def __init__(self, key, secure=False, port=None, host=None):
        self.key = key
        self._url = self.build_url(key, secure, port, host)
        self.callbacks = defaultdict(lambda: defaultdict(lambda: []))
        self.always_call = []
        self.error_handlers = []

    @classmethod
    def build_url(cls, key, secure, port, host):
        if not host:
            host = "ws.pusherapp.com"
        if secure:
            protocol = "wss"
            port = 443
        else:
            protocol = "ws"
            port = 80
        url = ""
        url += "{protocol}://{host}".format(**locals())
        if port:
            url += ":{}".format(port)
        url += "/app/{}".format(key)
        url += "?client={}".format(cls.CLIENT_ID)
        url += "&version={}".format(cls.VERSION)
        url += "&protocol={}".format(cls.PUSHER_PROTOCOL)
        return url

    async def connect(self):
        self._sock = await websockets.connect(self._url)
        msg = await self._recv_event()
        if msg["event"] != "pusher:connection_established":
            raise Exception("Connection failed:\n{}".format(pformat(msg)))
        self._socket_id = msg["data"]["socket_id"]

    async def subscribe(self, channel, auth=None):
        data = {"channel": channel}
        if auth:
            raise NotImplementedError("auth not implemented")
        await self._send_event("pusher:subscribe", data)

    async def unsubscribe(self, channel):
        data = {"channel": channel}
        await self._send_event("pusher:unsubscribe", data)

    async def _send_event(self, event, data, channel=None):
        msg = {"event": event, "data": data}
        if channel:
            msg["channel"] = channel
        msg = json.dumps(msg)
        await self._sock.send(msg)

    async def _recv_event(self):
        msg = await self._sock.recv()
        return self._parse_msg(msg)

    async def recv_forever(self):
        while True:
            try:
                await self.handle_one()
            except Exception as err:
                if not self.error_handlers:
                    traceback.print_exc()
                else:
                    for f in self.error_handlers:
                        await call_function_or_coroutine(f, err)
                # Connection is closed, no reason to continue ...
                if isinstance(err, websockets.exceptions.ConnectionClosed):
                    break

    async def handle_one(self):
        msg = await self._recv_event()
        channel = msg.get("channel")
        event = msg["event"]
        # callbacks that will be fired always
        for f in self.always_call:
            await call_function_or_coroutine(f, msg)
        # callbacks that will be fired with all events
        for f in self.callbacks[channel][None]:
            await call_function_or_coroutine(f, msg)
        for f in self.callbacks[channel][event]:
            await call_function_or_coroutine(f, msg["data"])

    async def close(self):
        await self._sock.close()

    @staticmethod
    def _parse_msg(msg):
        msg = json.loads(msg)
        event = msg["event"]
        data = msg["data"]
        # For some reason sometimes data is returned as a str representing json
        # and needs to be converted.
        if isinstance(data, str):
            data = json.loads(data)
        if event == "pusher:error":
            raise PusherException(**data)
        channel = msg.get("channel")
        return dict(event=event, data=data, channel=channel)
