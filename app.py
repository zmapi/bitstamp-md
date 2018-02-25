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
from datetime import datetime
from zmapi.zmq.utils import *
from zmapi.connector.controller import RESTController, ControllerBase
from zmapi.logging import setup_root_logger, disable_logger
from collections import defaultdict

################################## CONSTANTS ##################################

CAPABILITIES = sorted([
    "GET_SNAPSHOT",
    "GET_TICKER_FIELDS",
    "GET_TICKER_INFO_PRICE_TICK_SIZE",
    "SUBSCRIBE",
    "LIST_DIRECTORY",
    "PUB_ORDER_BOOK_INCREMENTAL",
])

TICKER_FIELDS = [
    {"field": "symbol",
     "type": "str",
     "label": "Symbol",
     "description": "The symbol of the ticker"}
]

MISC_DETAILS = {
    "timestamp_granularity": 1,
}

MODULE_NAME = "bitstamp-md"

################################ GLOBAL STATE #################################

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.status = "ok"

# placeholder for Logger
L = logging.root

###############################################################################

class Controller(RESTController):

    def __init__(self, ctx, addr):
        super().__init__("MD", ctx, addr)
        # max 600 requests in any rolling 10 minute period
        self._add_throttler(r".*", 600, 10 * 60)

    def _process_fetched_data(self, data, url):
        return json.loads(data.decode())

    @ControllerBase.handler()
    async def get_status(self, ident, msg):
        status = {
            "name": MODULE_NAME,
            "connector_name": "bitstamp",
            "status": g.status,
            "uptime": (datetime.utcnow() - g.startup_time).total_seconds(),
        }
        return [status]

    @ControllerBase.handler()
    async def list_directory(self, ident, msg):
        url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
        data = await self._http_get_cached(url, 86400)
        res = [x["url_symbol"] for x in data]
        res = sorted(res)
        res = [dict(name=x, ticker_id=x) for x in res]
        return res

    @ControllerBase.handler()
    async def get_ticker_info(self, ident, msg):
        content = msg["content"]
        ticker = content.get("ticker")
        ticker_id = ticker.get("ticker_id", ticker.get("symbol", "").lower())
        url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
        data = await self._http_get_cached(url, 86400)
        if ticker_id:
            data = [x for x in data if x["url_symbol"] == ticker_id]
            if not data:
                raise Exception("ticker not found")
            assert len(data) == 1, len(data)
        res = []
        for t in data:
            d = {}
            d["description"] = t["description"]
            # is this strictly the same thing as tradable?
            if t["trading"] == "Enabled":
                d["tradable"] = True
            else:
                d["tradable"] = False
            d["price_tick_size"] = 10 ** -t["counter_decimals"]
            d["float_price"] = True
            d["float_volume"] = True
            d["ticker_id"] = t["url_symbol"]
            res.append(d)
        return res

    @ControllerBase.handler()
    async def get_ticker_fields(self, ident, msg):
        return TICKER_FIELDS

    @ControllerBase.handler()
    async def get_misc_details(self, ident, msg):
        return MISC_DETAILS

    @ControllerBase.handler()
    async def modify_subscription(self, ident, msg):
        content = msg["content"]
        ticker_id = content["ticker_id"]
        ob_levels = content["order_book_levels"]
        ob_speed = content["order_book_speed"]
        trades_speed = content["trades_speed"]
        emit_quotes = content["emit_quotes"]
        res = {}
        if trades_speed > 0:
            res["trades"] = await g.pub.subscribe_trades(ticker_id)
        else:
            res["trades"] = await g.pub.unsubscribe_trades(ticker_id)
        if (ob_levels > 0 and ob_speed > 0) or emit_quotes:
            res["order_book"] = await g.pub.subscribe_order_book(ticker_id)
        else:
            res["order_book"] = await g.pub.unsubscribe_order_book(ticker_id)
        return res

    async def get_order_book_snapshot(self, levels, ticker_id, session):
        res = {}
        url = "https://www.bitstamp.net/api/v2/order_book/{}/"
        url = url.format(ticker_id)
        data = await self._http_get(url, session=session)
        res["timestamp"] = float(data["timestamp"])
        res["bids"] = []
        if "bids" in data:
            bids = [{"price": float(p), "size": float(s)}
                    for p, s in data["bids"]]
            bids = sorted(bids, key=lambda x: x["price"])[::-1][:levels]
            res["bids"] = bids
        res["asks"] = []
        if "asks" in data:
            asks = [{"price": float(p), "size": float(s)}
                    for p, s in data["asks"]]
            asks = sorted(asks, key=lambda x: x["price"])[:levels]
            res["asks"] = asks
        return res

    @ControllerBase.handler()
    async def get_snapshot(self, ident, msg):
        res = {}
        content = msg["content"]
        ticker_id = content["ticker_id"]
        url = "https://www.bitstamp.net/api/v2/ticker/{}/".format(ticker_id)
        async with aiohttp.ClientSession() as session:
            data = await self._http_get(url, session=session)
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
                # Get bbo data from order book to avoid problem with
                # data synchronization. This way it's possible to get size
                # data as well.
                if ob_data["bids"]:
                    best_lvl = ob_data["bids"][0]
                    res["bid_price"] = best_lvl["price"]
                    res["bid_size"] = best_lvl["size"]
                if ob_data["asks"]:
                    best_lvl = ob_data["asks"][0]
                    res["ask_price"] = best_lvl["price"]
                    res["ask_size"] = best_lvl["size"]
        return res

    @ControllerBase.handler()
    async def list_capabilities(self, ident, msg):
        return CAPABILITIES


###############################################################################

class Publisher:

    @staticmethod
    def create_subscription_definition():
        return dict(trades=False, ob=False)
    
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
        self._subscriptions = defaultdict(self.create_subscription_definition)

    async def _error_received(self, err):
        L.warning("{}: {}".format(type(err).__name__, err))

    async def _data_received(self, msg):
        channel = msg["channel"]
        ticker_id = self._channel_to_tid.get(channel)
        if not ticker_id:
            return
        data = msg["data"]
        event = msg["event"]
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
        asks = [{"price": float(price), "size": float(size)}
                for price, size in data["asks"]]
        bids = [{"price": float(price), "size": float(size)}
                for price, size in data["bids"]]
        data = dict(timestamp=timestamp, bids=bids, asks=asks)
        data = " " + json.dumps(data)
        data = [ticker_id.encode() + b"\x01", data.encode()]
        await self._sock.send_multipart(data)

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

    async def subscribe_trades(self, ticker_id):
        sub_def = self._subscriptions[ticker_id]
        if sub_def["trades"]:
            return "no change"
        channel = "live_trades{}".format(self.tid_to_postfix(ticker_id))
        self._channel_to_tid[channel] = ticker_id
        await self._pusher.subscribe(channel)
        # pusher_internal:subscription_succeeded fires even when
        # the channel does not even exist. Not useful to wait for this
        # confirmation.
        sub_def["trades"] = True
        return "subscribed"

    async def unsubscribe_trades(self, ticker_id):
        sub_def = self._subscriptions[ticker_id]
        if not sub_def["trades"]:
            return "no change"
        channel = "live_trades{}".format(self.tid_to_postfix(ticker_id))
        await self._pusher.unsubscribe(channel)
        data = [ticker_id.encode() + b"\x02", b""]
        await self._sock.send_multipart(data)
        sub_def["trades"] = False
        return "unsubscribed"

    async def subscribe_order_book(self, ticker_id):
        sub_def = self._subscriptions[ticker_id]
        if sub_def["ob"]:
            return "no change"
        channel = "diff_order_book{}".format(self.tid_to_postfix(ticker_id))
        self._channel_to_tid[channel] = ticker_id
        # pusher_internal:subscription_succeeded fires even when
        # the channel does not even exist. Not useful to wait for this
        # confirmation.
        await self._pusher.subscribe(channel)
        sub_def["ob"] = True
        return "subscribed"

    async def unsubscribe_order_book(self, ticker_id):
        sub_def = self._subscriptions[ticker_id]
        if not sub_def["ob"]:
            return "no change"
        channel = "diff_order_book{}".format(self.tid_to_postfix(ticker_id))
        await self._pusher.unsubscribe(channel)
        data = [ticker_id.encode() + b"\x01", b""]
        await self._sock.send_multipart(data)
        sub_def["ob"] = False
        return "unsubscribed"




###############################################################################

def parse_args():
    parser = argparse.ArgumentParser(description="bitstamp md connector")
    parser.add_argument("ctl_addr", help="address to bind to for ctl socket")
    parser.add_argument("pub_addr", help="address to bind to for pub socket")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    parser.add_argument("--log-websockets", action="store_true",
                        help="add websockets logger")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args

def setup_logging(args):
    if not args.log_websockets:
        disable_logger("websockets")
    setup_root_logger(args.log_level)

def main():
    args = parse_args()
    setup_logging(args)
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

