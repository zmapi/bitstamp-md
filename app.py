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
import struct
import aiohttp
import aiopusher
from numbers import Number
from zmapi import fix
from asyncio import ensure_future as create_task
from inspect import isfunction
from pprint import pprint, pformat
from time import time, gmtime
from datetime import datetime
from zmapi.zmq.utils import *
from zmapi.utils import random_str, delayed, get_timestamp
from zmapi.controller import RESTConnectorCTL, ConnectorCTL
from zmapi.logging import setup_root_logger, disable_logger
from collections import defaultdict
from uuid import uuid4


################################## CONSTANTS ##################################


CAPABILITIES = sorted([
    fix.ZMCap.UnsyncMDSnapshot,
    fix.ZMCap.GetTickerFields,
    fix.ZMCap.MDSubscribe,
    fix.ZMCap.ListDirectory,
    fix.ZMCap.PubOrderBookIncremental,
])

TICKER_FIELDS = [
#    {"field": "symbol",
#     "type": "str",
#     "label": "Symbol",
#     "description": "The symbol of the ticker"}
]


MISC_DETAILS = {
    "timestamp_granularity": 1,
}


MODULE_NAME = "bitstamp-md"
ENDPOINT_NAME = "bitstamp"


################################ GLOBAL STATE #################################


class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.status = "ok"
g.session_id = str(uuid4())
g.seq_no = 0

g.startup_event = asyncio.Event()

# placeholder for Logger
L = logging.root


###############################################################################


class MyController(RESTConnectorCTL):


    def __init__(self, sock_dn, ctx):
        super().__init__(sock_dn, ctx)
        # max 600 requests in any rolling 10 minute period
        self._add_throttler(r".*", 600, 10 * 60)


    def _process_fetched_data(self, data, url):
        return json.loads(data.decode())


    async def ZMGetStatus(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMGetStatusResponse
        d = {}
        d["module_name"] = MODULE_NAME
        d["endpoint_name"] = ENDPOINT_NAME
        d["session_id"] = g.session_id
        res["Body"] = [d]
        return res


    async def ZMListDirectory(self, ident, msg_raw, msg):
        url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
        data = await self._http_get_cached(url, 86400)
        data = sorted(data, key=lambda x: x["url_symbol"])
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListDirectoryResponse
        res["Body"] = body = {}
        group = []
        for x in data:
            d = {}
            d["ZMNodeName"] = x["url_symbol"]
            d["ZMInstrumentID"] = x["url_symbol"]
            d["Text"] = x["description"]
            group.append(d)
        body["ZMNoDirEntries"] = group
        return res


        # data = [x["url_symbol"] for x in data]
        # data = sorted(data)
        # res = {}
        # res["Header"] = header = {}
        # header["MsgType"] = fix.MsgType.ZMListDirectoryResponse
        # res["Body"] = body = {}
        # body["ZMDirEntries"] = [dict(ZMNodeName=x, ZMInstrumentID=x, Text=x)
        #                         for x in data]
        # return res


    async def SecurityListRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        instrument_id = body.get("ZMInstrumentID")
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.SecurityList
        res["Body"] = body = {}
        body["NoRelatedSym"] = group = []
        url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
        data = await self._http_get_cached(url, 86400)
        if instrument_id:
            print(instrument_id)
            data = [x for x in data if x["url_symbol"] == instrument_id]
            assert len(data) == 1, len(data)
            if not data:
                body["SecurityRequestResult"] = \
                        fix.SecurityRequestResult.NoInstrumentsFound
                return res
        for t in data:
            d = {}
            d["SecurityDesc"] = t["description"]
            d["MinPriceIncrement"] = 10 ** -t["counter_decimals"]
            d["ZMInstrumentID"] = t["url_symbol"]
            group.append(d)
        return res


    async def ZMGetInstrumentFields(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMGetInstrumentFieldsResponse
        res["Body"] = TICKER_FIELDS
        return res


    # # not yet specified in ZMAPI docs
    # async def get_misc_details(self, ident, msg_raw, msg):
    #     return MISC_DETAILS


    async def _get_snapshot(self, ins_id, sub_def):
        
        res = {}

        res["ZMNoMDFullEntries"] = group = []
        ticks = sub_def["NoMDEntryTypes"]
        market_depth = sub_def.get("MarketDepth", 0)

        get_ob = True
        if market_depth == 1 and "*" not in ticks \
                and "0" not in ticks and "1" not in ticks:
            get_ob = False

        get_quotes = False
        if "*" in ticks or \
               "7" in ticks or \
               "8" in ticks or \
               "2" in ticks or \
               "4" in ticks or \
               "B" in ticks or \
               "9" in ticks:
            get_quotes = True

        async with aiohttp.ClientSession() as session:

            res["LastUpdateTime"] = 0

            if get_quotes:
                url = "https://www.bitstamp.net/api/v2/ticker/{}/"
                url = url.format(ins_id)
                data = await self._http_get(url, session=session)
                if "*" in ticks or "7" in ticks:
                    d = {}
                    d["MDEntryType"] = "7"
                    d["MDEntryPx"] = float(data["high"])
                    group.append(d)
                if "*" in ticks or "8" in ticks:
                    d = {}
                    d["MDEntryType"] = "8"
                    d["MDEntryPx"] = float(data["low"])
                    group.append(d)
                if "*" in ticks or "4" in ticks:
                    d = {}
                    d["MDEntryType"] = "4"
                    d["MDEntryPx"] = float(data["open"])
                    group.append(d)
                if "*" in ticks or "B" in ticks:
                    d = {}
                    d["MDEntryType"] = "B"
                    d["MDEntrySize"] = float(data["volume"])
                    group.append(d)
                if "*" in ticks or "9" in ticks:
                    d = {}
                    d["MDEntryType"] = "9"
                    d["MDEntryPx"] = float(data["vwap"])
                    group.append(d)
                ts = datetime.utcfromtimestamp(float(data["timestamp"]))
                ts = int(ts.timestamp()) * 1000000000
                res["LastUpdateTime"] = ts

            if get_ob:
                url = "https://www.bitstamp.net/api/v2/order_book/{}/"
                url = url.format(ins_id)
                data = await self._http_get(url, session=session)
                ts = datetime.utcfromtimestamp(float(data["timestamp"]))
                ts = int(ts.timestamp()) * 1000000000
                res["LastUpdateTime"] = max(res["LastUpdateTime"], ts)
                if market_depth == 0:
                    market_depth = sys.maxsize
                bids = data.get("bids")
                bids = [(float(x[0]), float(x[1])) for x in bids]
                bids = sorted(bids)[::-1]
                asks = data.get("asks")
                asks = [(float(x[0]), float(x[1])) for x in asks]
                asks = sorted(asks)
                for i, (price, size) in enumerate(bids):
                    if i == market_depth:
                        break
                    d = {}
                    d["MDEntryType"]= "0"
                    d["MDEntryPx"] = price
                    d["MDEntrySize"] = size
                    d["MDPriceLevel"] = i + 1
                    group.append(d)
                for i, (price, size) in enumerate(asks):
                    if i == market_depth:
                        break
                    d = {}
                    d["MDEntryType"]= "1"
                    d["MDEntryPx"] = price
                    d["MDEntrySize"] = size
                    d["MDPriceLevel"] = i + 1
                    group.append(d)

        return res

                    
    #     res["bids"] = []
    #     if "bids" in data:
    #         bids = [{"price": float(p), "size": float(s)}
    #                 for p, s in data["bids"]]
    #         bids = sorted(bids, key=lambda x: x["price"])[::-1][:levels]
    #         res["bids"] = bids
    #     res["asks"] = []
    #     if "asks" in data:
    #         asks = [{"price": float(p), "size": float(s)}
    #                 for p, s in data["asks"]]
    #         asks = sorted(asks, key=lambda x: x["price"])[:levels]
    #         res["asks"] = asks




    async def MarketDataRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        srt = body["SubscriptionRequestType"]
        if srt not in "012":
            raise MarketDataRequestRejectException(
                    fix.UnsupportedSubscriptionRequestType, srt)
        ins_id = body["ZMInstrumentID"]
        ticks = body.get("NoMDEntryTypes")
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMMarketDataRequestResponse
        res["Body"] = body = {}
        if srt == "0":
            snap = await self._get_snapshot(ins_id, msg["Body"])
            body["ZMSnapshots"] = [snap]
        elif srt == "1":
            if "*" in ticks or "2" in ticks:
                tres = await g.pub.subscribe_trades(ins_id)
            else:
                tres = await g.pub.unsubscribe_trades(ins_id)
            bres = await g.pub.subscribe_order_book(ins_id)
            body["Text"] = "Trades: {} | Book: {}".format(tres, bres)
        elif srt == "2":
            tres = await g.pub.unsubscribe_trades(ins_id)
            bres = await g.pub.unsubscribe_order_book(ins_id)
            body["Text"] = "Trades: {} | Book: {}".format(tres, bres)
        return res


    # async def get_order_book_snapshot(self, levels, instrument_id, session):
    #     res = {}
    #     url = "https://www.bitstamp.net/api/v2/order_book/{}/"
    #     url = url.format(instrument_id)
    #     data = await self._http_get(url, session=session)
    #     res["timestamp"] = float(data["timestamp"])
    #     res["bids"] = []
    #     if "bids" in data:
    #         bids = [{"price": float(p), "size": float(s)}
    #                 for p, s in data["bids"]]
    #         bids = sorted(bids, key=lambda x: x["price"])[::-1][:levels]
    #         res["bids"] = bids
    #     res["asks"] = []
    #     if "asks" in data:
    #         asks = [{"price": float(p), "size": float(s)}
    #                 for p, s in data["asks"]]
    #         asks = sorted(asks, key=lambda x: x["price"])[:levels]
    #         res["asks"] = asks
    #     return res


    # async def get_snapshot(self, ident, msg_raw, msg):
    #     res = {}
    #     content = msg["content"]
    #     order_book_levels = content.get("order_book_levels", 0)
    #     daily_data = content.get("daily_data", False)
    #     quotes = content.get("quotes", True)
    #     instrument_id = content["ticker_id"]
    #     url = "https://www.bitstamp.net/api/v2/ticker/{}/".format(instrument_id)
    #     async with aiohttp.ClientSession() as session:
    #         if quotes:
    #             data = await self._http_get(url, session=session)
    #             res["quotes"] = q = {}
    #             q["ask_price"] = float(data.pop("ask"))
    #             q["bid_price"] = float(data.pop("bid"))
    #             q["last_price"] = float(data.pop("last"))
    #             q["timestamp"] = float(data.pop("timestamp"))
    #         if daily_data:
    #             res["daily"] = daily = {}
    #             daily.update({k: float(v) for k, v in data.items()})
    #         if order_book_levels > 0:
    #             ob_data = await self.get_order_book_snapshot(
    #                     order_book_levels,
    #                     instrument_id,
    #                     session)
    #             ob_data["id"] = "default"
    #             res["order_books"] = [ob_data]
    #             # Get bbo data from order book to avoid problem with
    #             # data synchronization. This way it's possible to get size
    #             # data as well.
    #             # if ob_data["bids"]:
    #             #     best_lvl = ob_data["bids"][0]
    #             #     res["bid_price"] = best_lvl["price"]
    #             #     res["bid_size"] = best_lvl["size"]
    #             # if ob_data["asks"]:
    #             #     best_lvl = ob_data["asks"][0]
    #             #     res["ask_price"] = best_lvl["price"]
    #             #     res["ask_size"] = best_lvl["size"]
    #     return res


    async def ZMListCapabilities(self, ident, msg_raw, msg):
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListCapabilitiesResponse
        res["Body"] = body = {}
        body["ZMNoCaps"] = CAPABILITIES
        return res


###############################################################################


class Publisher:


    @staticmethod
    def create_subscription_definition():
        return {
            "trades": False,
            "ob": False,
        }
        return dict(trades=False, ob=False)
    

    def __init__(self, sock):
        self._sock = sock
        self._pusher = None
        self._channel_to_tid = {}
        self._subscriptions = defaultdict(self.create_subscription_definition)


    async def resubscribe(self):
        for ins_id, d in self._subscriptions.items():
            L.debug("pusher resubscribing to {} ...".format(ins_id))
            if d["trades"]:
                d["trades"] = False
                await self.subscribe_trades(ins_id)
            if d["ob"]:
                d["ob"] = False
                await self.subscribe_order_book(ins_id)


    async def _send_disconnected(self):
        seq_no = g.seq_no
        g.seq_no += 1
        msg = {}
        msg["Header"] = header = {}
        header["ZMSendingTime"] = get_timestamp()
        header["MsgSeqNum"] = seq_no
        msg["Body"] = body = {}
        body["UserStatus"] = fix.UserStatus.Disconnected
        msg_bytes = (" " + json.dumps(msg)).encode()
        await self._sock.send_multipart([
            fix.MsgType.UserNotification.encode(),
            msg_bytes
        ])

    
    async def _restart(self):
        if self._pusher:
            await self._pusher.close()
        await self._send_disconnected()
        self._pusher = aiopusher.Client("de504dc5763aeef9ff52", secure=True)
        self._pusher.always_call.append(self._data_received)
        self._pusher.error_handlers.append(self._error_received)
        L.info("connecting to pusher ...")
        while True:
            try:
                await self._pusher.connect()
            except Exception:
                L.exception("error connecting to pusher:")
                L.info("connecting to pusher ...")
                continue
            break
        L.info("connected to pusher")
        g.startup_event.set()
        create_task(self.resubscribe())
        await self._pusher.recv_forever()


    async def _error_received(self, err):
        L.warning("{}: {}".format(type(err).__name__, err))
        L.warning("restarting pusher ...")
        g.seq_no = 0
        await self._restart()


    async def _data_received(self, msg):
        channel = msg["channel"]
        instrument_id = self._channel_to_tid.get(channel)
        if not instrument_id:
            return
        data = msg["data"]
        event = msg["event"]
        if not data:
            return
        if channel.startswith("live_trades"):
            await self._handle_trade(instrument_id, data)
        elif channel.startswith("diff_order_book"):
            await self._handle_order_book(instrument_id, data)


    async def _handle_trade(self, instrument_id, data):
        seq_no = g.seq_no
        g.seq_no += 1
        d = {}
        d["Header"] = header = {}
        header["MsgSeqNum"] = seq_no
        ts = datetime.utcfromtimestamp(float(data["timestamp"])).timestamp()
        ts = int(ts) * 1000000000
        header["TransactTime"] = ts
        header["ZMSendingTime"] = int(datetime.utcnow().timestamp() * 1e9)
        d["Body"] = body = {}
        body["MDIncGrp"] = group = []
        c = {}
        c["MDUpdateAction"] = "0"
        c["MDEntryType"] = "2"
        c["MDEntryPx"] = float(data["price"])
        c["MDEntrySize"] = float(data["amount"])
        c["ZMTickerID"] = g.ctl.insid_to_tid[instrument_id]
        group.append(c)
        data = " " + json.dumps(d)
        data = [b"X", data.encode()]
        await self._sock.send_multipart(data)


    async def _handle_order_book(self, instrument_id, data):

        tid = g.ctl.insid_to_tid[instrument_id]
        seq_no = g.seq_no
        g.seq_no += 1
        d = {}
        d["Header"] = header = {}
        header["MsgSeqNum"] = seq_no
        ts = datetime.utcfromtimestamp(float(data["timestamp"])).timestamp()
        ts = int(ts) * 1000000000
        header["TransactTime"] = ts
        header["ZMSendingTime"] = int(datetime.utcnow().timestamp() * 1e9)

        d["Body"] = body = {}
        body["ZMNoMDIncEntries"] = group = []

        for price, size in data["bids"]:
            c = {}
            c["MDUpdateAction"] = "5"
            c["MDEntryType"] = "0"
            c["MDEntryPx"] = float(price)
            c["MDEntrySize"] = float(size)
            c["ZMTickerID"] = tid
            group.append(c)

        for price, size in data["asks"]:
            c = {}
            c["MDUpdateAction"] = "5"
            c["MDEntryType"] = "1"
            c["MDEntryPx"] = float(price)
            c["MDEntrySize"] = float(size)
            c["ZMTickerID"] = tid
            group.append(c)

        data = " " + json.dumps(d)
        data = [b"X", data.encode()]
        await self._sock.send_multipart(data)


    async def run(self):
        await self._restart()


    @staticmethod
    def tid_to_postfix(instrument_id):
        if instrument_id == "btcusd":
            return ""
        return "_{}".format(instrument_id)


    async def subscribe_trades(self, instrument_id):
        sub_def = self._subscriptions[instrument_id]
        if sub_def["trades"]:
            return "no change"
        channel = "live_trades{}".format(self.tid_to_postfix(instrument_id))
        self._channel_to_tid[channel] = instrument_id
        await self._pusher.subscribe(channel)
        # pusher_internal:subscription_succeeded fires even when
        # the channel does not even exist. Not useful to wait for this
        # confirmation.
        sub_def["trades"] = True
        return "subscribed"


    async def unsubscribe_trades(self, instrument_id):
        sub_def = self._subscriptions[instrument_id]
        if not sub_def["trades"]:
            return "no change"
        channel = "live_trades{}".format(self.tid_to_postfix(instrument_id))
        await self._pusher.unsubscribe(channel)
        # data = [instrument_id.encode() + b"\x02", b""]
        # await self._sock.send_multipart(data)
        sub_def["trades"] = False
        return "unsubscribed"


    async def subscribe_order_book(self, instrument_id):
        sub_def = self._subscriptions[instrument_id]
        if sub_def["ob"]:
            return "no change"
        channel = "diff_order_book{}".format(self.tid_to_postfix(instrument_id))
        self._channel_to_tid[channel] = instrument_id
        # pusher_internal:subscription_succeeded fires even when
        # the channel does not even exist. Not useful to wait for this
        # confirmation.
        await self._pusher.subscribe(channel)
        sub_def["ob"] = True
        return "subscribed"


    async def unsubscribe_order_book(self, instrument_id):
        sub_def = self._subscriptions[instrument_id]
        if not sub_def["ob"]:
            return "no change"
        channel = "diff_order_book{}".format(self.tid_to_postfix(instrument_id))
        await self._pusher.unsubscribe(channel)
        # data = [instrument_id.encode() + b"\x01", b""]
        # await self._sock.send_multipart(data)
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


def init_zmq_sockets(args):
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr)
    g.sock_pub = g.ctx.socket(zmq.PUB)
    g.sock_pub.bind(args.pub_addr)


def main():
    args = parse_args()
    setup_logging(args)
    init_zmq_sockets(args)
    g.ctl = MyController(g.sock_ctl, g.ctx)
    g.pub = Publisher(g.sock_pub)
    L.debug("starting event loop ...")
    tasks = [
        delayed(g.ctl.run, g.startup_event),
        g.pub.run(),
    ]
    g.loop.run_until_complete(asyncio.gather(*tasks))
    g.ctx.destroy()


if __name__ == "__main__":
    main()

