import asyncio
import os
import argparse
import zmq
import zmq.asyncio
import json
import logging
import time
import traceback
import re
import sys
import csv
from zmapi.codes import error
import aiohttp
import aiopusher
from asyncio import ensure_future as create_task
from inspect import isfunction
from pprint import pprint, pformat
from time import time, gmtime

################################## CONSTANTS ##################################

CAPABILITIES = sorted([
    "GET_SNAPSHOT",
    "GET_TICKER_INFO_PRICE_TICK_SIZE",
    "SUBSCRIBE",
])

################################ GLOBAL STATE #################################

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()

# placeholder for Logger
L = None

###############################################################################

def split_message(msg_parts):
    separator_idx = None
    for i, part in enumerate(msg_parts):
        if not part:
            separator_idx = i
            break
    if not separator_idx:
        raise ValueError("ident separator not found")
    ident = msg_parts[:separator_idx]
    msg = msg_parts[separator_idx+1]
    return ident, msg

def ident_to_str(ident):
    return "/".join([x.decode("latin-1").replace("/", "\/") for x in ident])

def get_last_ep(sock):
    return sock.getsockopt_string(zmq.LAST_ENDPOINT)

###############################################################################

class CodecException(Exception):
    pass

class DecodingException(Exception):
    pass

class InvalidArguments(Exception):
    pass

class CommandNotImplemented(Exception):
    pass

class ControllerBase:

    _commands = {}

    def __init__(self, ctx, addr):
        self._sock = ctx.socket(zmq.ROUTER)
        self._sock.bind(addr)

    async def _send_error(self, ident, msg_id, ecode, msg=None):
        msg = error.gen_error(ecode, msg)
        msg["msg_id"] = msg_id
        await self._send_reply(ident, msg)

    async def _send_result(self, ident, msg_id, content):
        msg = dict(msg_id=msg_id, content=content, result="ok")
        await self._send_reply(ident, msg)
       
    async def _send_reply(self, ident, res):
        msg = " " + json.dumps(res)
        msg = msg.encode()
        await self._sock.send_multipart(ident + [b"", msg])

    async def run(self):
        while True:
            msg_parts = await self._sock.recv_multipart()
            # pprint(msg_parts)
            try:
                ident, msg = split_message(msg_parts)
            except ValueError as err:
                L.error(str(err))
                continue
            if len(msg) == 0:
                # handle ping message
                await self._sock.send_multipart(msg_parts)
                continue
            create_task(self._handle_one_1(ident, msg))

    async def _handle_one_1(self, ident, msg):
        try:
            msg = json.loads(msg.decode())
            msg_id = msg.get("msg_id")
            res = await self._handle_one_2(ident, msg)
        except InvalidArguments as e:
            L.exception("invalid arguments on message:")
            await self._send_error(ident, msg_id, error.ARGS, str(e))
        except CommandNotImplemented as e:
            L.exception("command not implemented:")
            await self._send_error(ident, msg_id, error.NOTIMPL, str(e))
        except Exception as e:
            L.exception("general exception handling message:", str(e))
            await self._send_error(ident, msg_id, error.GENERIC, str(e))
        else:
            if res is not None:
                await self._send_result(ident, msg_id, res)


    async def _handle_one_2(self, ident, msg):
        cmd = msg["command"]
        L.debug("{}: {}".format(ident_to_str(ident), cmd))
        f = self._commands.get(cmd)
        if not f:
            raise CommandNotImplemented(cmd)
        return await f(self, ident, msg)

    @staticmethod
    def handler(cmd=None):
        def decorator(f):
            nonlocal cmd
            if not cmd:
                cmd = f.__name__
            ControllerBase._commands[cmd] = f
            return f
        return decorator

class Controller(ControllerBase):

    def __init__(self, ctx, addr):
        super().__init__(ctx, addr)
        self._cache = {}

    async def _fetch_cached(self, url, **kwargs):
        session = kwargs.pop("session", None)
        expiration_secs = kwargs.pop("expiration_secs", 86400)
        holder = self._cache.get(url)
        data = None
        if holder is not None:
            elapsed = time() - holder["timestamp"]
            if elapsed < expiration_secs:
                data = holder["data"]
        if data is None:
            timestamp = time()
            data = await self._do_fetch(session, url)
            holder = dict(data=data, timestamp=timestamp)
            self._cache[url] = holder
        return data

    async def _do_fetch(self, session, url):
        close_session = False
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True
        data = None
        async with session.get(url) as r:
            if r.status < 200 or r.status >= 300:
                raise Exception("GET {}: status {}".format(url, r.status))
            data = await r.read()
        if close_session:
            session.close()
        return data

    @ControllerBase.handler()
    async def get_status(self, ident, msg):
        return [{"name": "bitstamp-md"}]

    @ControllerBase.handler()
    async def list_directory(self, ident, msg):
        url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
        data = await self._fetch_cached(url)
        data = json.loads(data.decode())
        res = [x["url_symbol"].upper() for x in data]
        res = sorted(res)
        res = [dict(name=x, dir=False) for x in res]
        return res

    @ControllerBase.handler()
    async def get_ticker_info(self, ident, msg):
        ticker = msg["content"]["ticker"]
        if "ticker_id" in ticker:
            ticker_id = ticker["ticker_id"]
        else:
            ticker_id = ticker["symbol"].lower()
        url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
        data = await self._fetch_cached(url)
        data = json.loads(data.decode())
        sel = [x for x in data if x["url_symbol"] == ticker_id]
        if not sel:
            raise Exception("ticker not found")
        assert len(sel) == 1, sel
        d = sel[0]
        res = {}
        res["description"] = d["description"]
        # is this strictly the same thing as tradable?
        if d["trading"] == "Enabled":
            res["tradable"] = True
        else:
            res["tradable"] = False
        res["price_tick_size"] = 10 ** -d["counter_decimals"]
        res["float_price"] = True
        res["float_volume"] = True
        res["ticker_id"] = ticker_id
        return res
                

    @ControllerBase.handler()
    async def subscribe(self, ident, msg):
        await g.pub.subscribe(msg)
        return {}

    @ControllerBase.handler()
    async def get_publisher(self, ident, msg):
        ep = get_last_ep(g.pub._sock)
        transport = ep[:ep.find(":")]
        res = {}
        res["transport"] = transport
        if transport == "tcp":
            res["port"] = int(ep.split(":")[-1])
        elif transport == "ipc":
            if ep[6] == "/":  #absolute path
                res["path"] = ep[6:]
            else:  # relative path
                res["path"] = os.path.join(os.environ["PWD"], ep[6:])
        else:
            raise NotImplementedError("transport not implemented")
        return res

    @ControllerBase.handler()
    async def get_snapshot(self, ident, msg):
        res = {}
        content = msg["content"]
        ticker_id = content["ticker_id"]
        url = "https://www.bitstamp.net/api/v2/ticker/{}/".format(ticker_id)
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as r:
                if r.status < 200 or r.status >= 300:
                    raise Exception("GET ticker: {}".format(r.status))
                data = await r.read()
                data = json.loads(data.decode())
                res["ask_price"] = float(data.pop("ask"))
                res["bid_price"] = float(data.pop("bid"))
                res["last_price"] = float(data.pop("last"))
                res["timestamp"] = float(data.pop("timestamp"))
                if content.get("daily_data"):
                    res["daily"] = daily = {}
                    daily.update({k: float(v) for k, v in data.items()})
                order_book_levels = content.get("order_book_levels", 0)
                if order_book_levels > 0:
                    ob_data = await self.get_order_book_snapshot(
                            order_book_levels,
                            ticker_id,
                            session)
                    timestamp2 = ob_data.pop("timestamp")
                    res["timestamp"] = max(res["timestamp"], timestamp2)
                    res["order_book"] = ob_data
        return res

    async def get_order_book_snapshot(self, levels, ticker_id, session):
        res = {}
        url = "https://www.bitstamp.net/api/v2/order_book/{}/"
        url = url.format(ticker_id)
        async with session.get(url) as r:
            if r.status < 200 or r.status >= 300:
                raise Exception("GET order_book: {}".format(r.status))
            data = await r.read()
            data = json.loads(data.decode())
            res["timestamp"] = float(data["timestamp"])
            bids = [[float(p), float(s)] for p, s in data["bids"]]
            bids = sorted(bids)[::-1]
            bids = bids[:levels]
            res["bids"] = bids
            asks = [[float(p), float(s)] for p, s in data["asks"]]
            asks = sorted(asks)
            asks = asks[:levels]
            res["asks"] = asks
        return res

    @ControllerBase.handler()
    async def list_capabilities(self, ident, msg):
        return CAPABILITIES


###############################################################################

class Publisher:
    
    def __init__(self, ctx, addr):
        self._ctx = ctx
        self._sock = ctx.socket(zmq.PUB)
        if addr:
            self._sock.bind(addr)
        else:
            self._sock.bind_to_random_port("tcp://*")
        self._pusher = aiopusher.Client("de504dc5763aeef9ff52", secure=True)
        self._pusher.always_call.append(self._data_received)
        self._pusher.error_handlers.append(self._error_received)
        self._channel_to_tid = {}
        self._internal_pub = ctx.socket(zmq.PUB)
        self._internal_pub.bind("inproc://pusher_internal")

    async def _error_received(self, err):
        L.warning("{}: {}".format(type(err).__name__, err))

    async def _data_received(self, msg):
        channel = msg["channel"]
        ticker_id = self._channel_to_tid.get(channel)
        if not ticker_id:
            return
        data = msg["data"]
        event = msg["event"]
        # not required at the moment ...
        # if event.startswith("pusher_internal"):
        #     await self._internal_pub.send_string(json.dumps(msg))
        #     return
        if not data:
            return
        if channel.startswith("live_trades"):
            await self._handle_trade(ticker_id, data)
        elif channel.startswith("diff_order_book"):
            await self._handle_order_book(ticker_id, data)

    async def _handle_trade(self, ticker_id, data):
        price = data["price"]
        size = data["amount"]
        timestamp = float(data["timestamp"])
        data = dict(price=price, size=size, timestamp=timestamp)
        data = " " + json.dumps(data)
        data = [ticker_id.encode() + b"\x02", data.encode()]
        await self._sock.send_multipart(data)

    async def _handle_order_book(self, ticker_id, data):
        timestamp = float(data["timestamp"])
        asks = [[float(price), float(size)] for price, size in data["asks"]]
        bids = [[float(price), float(size)] for price, size in data["bids"]]
        data = dict(timestamp=timestamp, bids=bids, asks=asks)
        data = " " + json.dumps(data)
        data = [ticker_id.encode() + b"\x01", data.encode()]
        await self._sock.send_multipart(data)
        # timestamp = float(data["timestamp"])
        # levels = {price_str: float(size) for price_str, size in data["bids"]}
        # for price_str, size in data["asks"]:
        #     size = float(size)
        #     old_size = levels.get(price_str, 0)
        #     if old_size == 0:
        #         levels[price_str] = -size
        # l = [[float(price_str), levels[price_str]]
        #      for price_str in sorted(levels)]
        # data = dict(timestamp=timestamp, order_book=l)
        # data = " " + json.dumps(data)
        # data = [ticker_id.encode() + b"\x01", data.encode()]
        # await self._sock.send_multipart(data)

    #async def _error_received(self, err):
    #    traceback.print_exception(type(err), err, err.__traceback__)

    async def run(self):
        L.info("connecting to pusher ...")
        await self._pusher.connect()
        L.info("connected to pusher")
        await self._pusher.recv_forever()

    @staticmethod
    def tid_to_postfix(ticker_id):
        if ticker_id == "btcusd":
            return ""
        return "_{}".format(ticker_id)

    async def _subscribe_trades(self, ticker_id):
        channel = "live_trades{}".format(self.tid_to_postfix(ticker_id))
        self._channel_to_tid[channel] = ticker_id
        await self._pusher.subscribe(channel)
        # pusher_internal:subscription_succeeded fires even when
        # the channel does not even exist. Not useful to wait for this
        # confirmation.

        # sock = self._ctx.socket(zmq.SUB)
        # sock.connect("inproc://pusher_internal")
        # sock.subscribe(b"")
        # tic = time()
        # while True:
        #     print("trying")
        #     await asyncio.sleep(0.1)
        #     try:
        #         data = await sock.recv_string(zmq.NOBLOCK)
        #     except zmq.error.Again:
        #         print("again")
        #         pass
        #     else:
        #         data = json.loads(data)
        #         evt_ok = "pusher_internal:subscription_succeeded"
        #         if data["channel"] == channel and data["event"] == evt_ok:
        #             L.info("subscribed to trades on {}".format(ticker_id))
        #             break
        #     toc = time() - tic
        #     if toc > 5:
        #         raise Exception("timed out when subscribing to trades on {}"
        #                         .format(ticker_id))

    async def _unsubscribe_trades(self, ticker_id):
        channel = "live_trades{}".format(self.tid_to_postfix(ticker_id))
        await self._pusher.unsubscribe(channel)
        data = [ticker_id.encode() + b"\x02", b""]
        await self._sock.send_multipart(data)

    async def _subscribe_order_book(self, ticker_id, levels):
        channel = "diff_order_book{}".format(self.tid_to_postfix(ticker_id))
        self._channel_to_tid[channel] = ticker_id
        await self._pusher.subscribe(channel)

    async def _unsubscribe_order_book(self, ticker_id):
        channel = "diff_order_book{}".format(self.tid_to_postfix(ticker_id))
        await self._pusher.unsubscribe(channel)
        data = [ticker_id.encode() + b"\x01", b""]
        await self._sock.send_multipart(data)


    async def subscribe(self, msg):
        content = msg["content"]
        ticker_id = content["ticker_id"]
        order_book_levels = content.get("order_book_levels", 0)
        order_book_speed = content.get("order_book_speed", 10)
        trades_speed = content.get("trades_speed", 10)
        if trades_speed > 0:
            await self._subscribe_trades(ticker_id)
        else:
            await self._unsubscribe_trades(ticker_id)
        if order_book_levels > 0 and order_book_speed > 0:
            await self._subscribe_order_book(ticker_id, order_book_levels)
        else:
            await self._unsubscribe_order_book(ticker_id)


###############################################################################


def build_logger(args):
    logging.root.setLevel(args.log_level)
    logger = logging.getLogger(__name__)
    logger.propagate = False
    logger.handlers.clear()
    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s"
    datefmt = "%H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    # convert datetime to utc
    formatter.converter = gmtime
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def parse_args():
    parser = argparse.ArgumentParser(description="bitstamp md connector")
    parser.add_argument("ctl_addr", help="address to bind to for ctl socket")
    parser.add_argument("pub_addr", help="address to bind to for pub socket")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args

def main():
    global L
    args = parse_args()
    L = build_logger(args)
    g.ctl = Controller(g.ctx, args.ctl_addr)
    g.pub = Publisher(g.ctx, args.pub_addr)
    L.debug("starting event loop ...")
    tasks = [
        create_task(g.ctl.run()),
        create_task(g.pub.run()),
    ]
    g.loop.run_until_complete(asyncio.gather(*tasks))
    g.ctx.destroy()

if __name__ == "__main__":
    main()

