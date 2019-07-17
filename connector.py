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
from base64 import b64encode, b64decode


################################## CONSTANTS ##################################


CAPABILITIES = sorted([
    fix.ZMCap.ListDirectoryOOBSnapshot,
    fix.ZMCap.SecurityDefinitionOOBSnapshot,
    fix.ZMCap.MDSubscribe,
    fix.ZMCap.MDMBP,
    fix.ZMCap.MDMBPIncremental,
    fix.ZMCap.MDMBPExplicitDelete,
    fix.ZMCap.MDMBO,
    fix.ZMCap.MDMBPPlusMBO,
    fix.ZMCap.MDSaneMBO,
])


################################ GLOBAL STATE #################################


class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.status = "ok"

g.startup_event = asyncio.Event()

# placeholder for Logger
L = logging.root


################################### HELPERS ###################################


def bitstamp_ts_to_zmapi_ts(bitstamp_ts):
    res = datetime.fromtimestamp(float(bitstamp_ts) / 1e6).timestamp()
    return int(res * 1e9)


###############################################################################


class MyController(RESTConnectorCTL):


    def __init__(self, sock_dn, ctx):
        super().__init__(sock_dn, ctx, "bitstamp", caps=CAPABILITIES)
        # max 600 requests in any rolling 10 minute period
        self._add_throttler(r".*", 600, 10 * 60)


    def _process_fetched_data(self, data, url):
        return json.loads(data.decode())


    async def ZMListCommonInstruments(self, ident, msg_raw, msg):
        url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
        data = await self._http_get_cached(url, 86400)
        data = sorted(data, key=lambda x: x["url_symbol"])
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMListCommonInstrumentsResponse
        res["Body"] = body = {}
        r = {x["url_symbol"]: x["url_symbol"] for x in data}
        r = json.dumps(r)
        r = b64encode(r.encode()).decode()
        body["ZMCommonInstruments"] = r
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


    # async def SecurityListRequest(self, ident, msg_raw, msg):
    #     body = msg["Body"]
    #     instrument_id = body.get("ZMInstrumentID")
    #     res = {}
    #     res["Header"] = header = {}
    #     header["MsgType"] = fix.MsgType.SecurityList
    #     res["Body"] = body = {}
    #     body["NoRelatedSym"] = group = []
    #     url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
    #     data = await self._http_get_cached(url, 86400)
    #     if instrument_id:
    #         data = [x for x in data if x["url_symbol"] == instrument_id]
    #         assert len(data) == 1, len(data)
    #     for t in data:
    #         d = {}
    #         d["SecurityDesc"] = t["description"]
    #         d["MinPriceIncrement"] = 10 ** -t["counter_decimals"]
    #         d["ZMInstrumentID"] = t["url_symbol"]
    #         group.append(d)
    #     return res


    async def SecurityDefinitionRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        ins_id = body["ZMInstrumentID"]
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMSecurityDefinitionRequestResponse
        res["Body"] = body = {}
        url = "https://www.bitstamp.net/api/v2/trading-pairs-info/"
        data = await self._http_get_cached(url, 86400)
        data = [x for x in data if x["url_symbol"] == ins_id]
        assert len(data) == 1
        data = data[0]
        body["SecurityDesc"] = data["description"]
        body["MinPriceIncrement"] = 10 ** -data["counter_decimals"]
        body["ZMInstrumentID"] = ins_id
        return res


    # async def ZMGetInstrumentFields(self, ident, msg_raw, msg):
    #     res = {}
    #     res["Header"] = header = {}
    #     header["MsgType"] = fix.MsgType.ZMGetInstrumentFieldsResponse
    #     res["Body"] = TICKER_FIELDS
    #     return res


    # # not yet specified in ZMAPI docs
    # async def get_misc_details(self, ident, msg_raw, msg):
    #     return MISC_DETAILS


    # async def _get_snapshot(self, ins_id, sub_def):
    #     
    #     res = {}

    #     res["ZMNoMDFullEntries"] = group = []
    #     ticks = sub_def["NoMDEntryTypes"]
    #     market_depth = sub_def.get("MarketDepth", 0)

    #     get_ob = True
    #     if market_depth == 1 and "*" not in ticks \
    #             and "0" not in ticks and "1" not in ticks:
    #         get_ob = False

    #     get_quotes = False
    #     if "*" in ticks or \
    #            "7" in ticks or \
    #            "8" in ticks or \
    #            "2" in ticks or \
    #            "4" in ticks or \
    #            "B" in ticks or \
    #            "9" in ticks:
    #         get_quotes = True

    #     async with aiohttp.ClientSession() as session:

    #         res["LastUpdateTime"] = 0

    #         if get_quotes:
    #             url = "https://www.bitstamp.net/api/v2/ticker/{}/"
    #             url = url.format(ins_id)
    #             data = await self._http_get(url, session=session)
    #             if "*" in ticks or "7" in ticks:
    #                 d = {}
    #                 d["MDEntryType"] = "7"
    #                 d["MDEntryPx"] = float(data["high"])
    #                 group.append(d)
    #             if "*" in ticks or "8" in ticks:
    #                 d = {}
    #                 d["MDEntryType"] = "8"
    #                 d["MDEntryPx"] = float(data["low"])
    #                 group.append(d)
    #             if "*" in ticks or "4" in ticks:
    #                 d = {}
    #                 d["MDEntryType"] = "4"
    #                 d["MDEntryPx"] = float(data["open"])
    #                 group.append(d)
    #             if "*" in ticks or "B" in ticks:
    #                 d = {}
    #                 d["MDEntryType"] = "B"
    #                 d["MDEntrySize"] = float(data["volume"])
    #                 group.append(d)
    #             if "*" in ticks or "9" in ticks:
    #                 d = {}
    #                 d["MDEntryType"] = "9"
    #                 d["MDEntryPx"] = float(data["vwap"])
    #                 group.append(d)
    #             ts = datetime.utcfromtimestamp(float(data["timestamp"]))
    #             ts = int(ts.timestamp()) * 1000000000
    #             res["LastUpdateTime"] = ts

    #         if get_ob:
    #             url = "https://www.bitstamp.net/api/v2/order_book/{}/"
    #             url = url.format(ins_id)
    #             data = await self._http_get(url, session=session)
    #             ts = datetime.utcfromtimestamp(float(data["timestamp"]))
    #             ts = int(ts.timestamp()) * 1000000000
    #             res["LastUpdateTime"] = max(res["LastUpdateTime"], ts)
    #             if market_depth == 0:
    #                 market_depth = sys.maxsize
    #             bids = data.get("bids")
    #             bids = [(float(x[0]), float(x[1])) for x in bids]
    #             bids = sorted(bids)[::-1]
    #             asks = data.get("asks")
    #             asks = [(float(x[0]), float(x[1])) for x in asks]
    #             asks = sorted(asks)
    #             for i, (price, size) in enumerate(bids):
    #                 if i == market_depth:
    #                     break
    #                 d = {}
    #                 d["MDEntryType"]= "0"
    #                 d["MDEntryPx"] = price
    #                 d["MDEntrySize"] = size
    #                 d["MDPriceLevel"] = i + 1
    #                 group.append(d)
    #             for i, (price, size) in enumerate(asks):
    #                 if i == market_depth:
    #                     break
    #                 d = {}
    #                 d["MDEntryType"]= "1"
    #                 d["MDEntryPx"] = price
    #                 d["MDEntrySize"] = size
    #                 d["MDPriceLevel"] = i + 1
    #                 group.append(d)

    #     return res

                    
    async def MarketDataRequest(self, ident, msg_raw, msg):
        body = msg["Body"]
        srt = body["SubscriptionRequestType"]
        ins_id = body["ZMInstrumentID"]
        res = {}
        res["Header"] = header = {}
        header["MsgType"] = fix.MsgType.ZMMarketDataRequestResponse
        res["Body"] = body = {}
        if srt == fix.SubscriptionRequestType.SnapshotAndUpdates:
            body["Text"] = await g.pub.subscribe(ins_id, msg["Body"])
            # if "*" in ticks or "2" in ticks:
            #     tres = await g.pub.subscribe_trades(ins_id)
            # else:
            #     tres = await g.pub.unsubscribe_trades(ins_id)
            # bres = await g.pub.subscribe_order_book(ins_id)
            # body["Text"] = "Trades: {} | Book: {}".format(tres, bres)
        elif srt == fix.SubscriptionRequestType.Unsubscribe:
            body["Text"] = g.pub.unsubscribe(ins_id)
            # tres = await g.pub.unsubscribe_trades(ins_id)
            # bres = await g.pub.unsubscribe_order_book(ins_id)
            # body["Text"] = "Trades: {} | Book: {}".format(tres, bres)
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



###############################################################################


class Publisher:


    INTERNAL_PUB_ADDR = "inproc://publisher-internal-pub"


    def __init__(self, sock):
        self._sock = sock
        self._pusher = None
        self._channel_to_ins_id = {}
        def create_subscription_definition():
            return {
                "trades": False,
                "mbp": False,
                "mbo": False,
                "lock": asyncio.Lock(),
            }
        self._subscriptions = defaultdict(create_subscription_definition)
        self._internal_pub = g.ctx.socket(zmq.PUB)
        self._internal_pub.bind(self.INTERNAL_PUB_ADDR)


    async def resubscribe(self):
        # TODO: write this
        return
        # for ins_id, d in self._subscriptions.items():
        #     L.debug("pusher resubscribing to {} ...".format(ins_id))
        #     if d["trades"]:
        #         d["trades"] = False
        #         await self.subscribe_trades(ins_id)
        #     if d["ob"]:
        #         d["ob"] = False
        #         await self.subscribe_order_book(ins_id)


    # async def subscribe_trades(self, instrument_id):
    #     sub_def = self._subscriptions[instrument_id]
    #     if sub_def["trades"]:
    #         return "no change"
    #     channel = "live_trades{}".format(self.tid_to_postfix(instrument_id))
    #     self._channel_to_tid[channel] = instrument_id
    #     await self._pusher.subscribe(channel)
    #     # pusher_internal:subscription_succeeded fires even when
    #     # the channel does not even exist. Not useful to wait for this
    #     # confirmation.
    #     sub_def["trades"] = True
    #     return "subscribed"


    # async def unsubscribe_trades(self, instrument_id):
    #     sub_def = self._subscriptions[instrument_id]
    #     if not sub_def["trades"]:
    #         return "no change"
    #     channel = "live_trades{}".format(self.tid_to_postfix(instrument_id))
    #     await self._pusher.unsubscribe(channel)
    #     # data = [instrument_id.encode() + b"\x02", b""]
    #     # await self._sock.send_multipart(data)
    #     sub_def["trades"] = False
    #     return "unsubscribed"


    async def _subscribe_channel(self, ins_id, channel):
        self._channel_to_ins_id[channel] = ins_id
        await self._pusher.subscribe(channel)
        # pusher_internal:subscription_succeeded fires even when
        # the channel does not even exist. Not useful to wait for this
        # confirmation.


    async def _listen_internal(self, channel):
        sock_sub = g.ctx.socket(zmq.SUB)
        sock_sub.connect(self.INTERNAL_PUB_ADDR)
        sock_sub.subscribe(channel + "\0")
        poller = zmq.asyncio.Poller()
        poller.register(sock_sub)
        L.debug(f"listening channel: {channel}")
        r = await poller.poll(5000)
        if not r:
            raise Exception("timed out waiting for native snapshot")
        _, data = await sock_sub.recv_multipart()
        L.debug(f"listening finished on channel: {channel}")
        sock_sub.close()
        return json.loads(data.decode())


    async def _subscribe_trades(self, ins_id):
        sd = self._subscriptions[ins_id]
        channel = "live_trades{}".format(
                self.insid_to_postfix(ins_id))
        await self._subscribe_channel(ins_id, channel)
        sd["trades"] = True


    async def _unsubscribe_trades(self, ins_id):
        sd = self._subscriptions[ins_id]
        channel = "live_trades{}".format(
                self.insid_to_postfix(ins_id))
        await self._pusher.unsubscribe(channel)
        sd["trades"] = False


    async def _subscribe_mbp(self, ins_id):
        sd = self._subscriptions[ins_id]
        channel = "diff_order_book{}".format(
                self.insid_to_postfix(ins_id))
        create_task(self._subscribe_channel(ins_id, channel))
        channel = "order_book{}".format(
                self.insid_to_postfix(ins_id))
        create_task(self._subscribe_channel(ins_id, channel))
        await self._listen_internal(channel)
        await self._pusher.unsubscribe(channel)
        sd["mbp"] = True


    async def _unsubscribe_mbp(self, ins_id):
        sd = self._subscriptions[ins_id]
        channel = "diff_order_book{}".format(
                self.insid_to_postfix(ins_id))
        await self._pusher.unsubscribe(channel)
        sd["mbp"] = False


    async def _subscribe_mbo(self, ins_id):
        sd = self._subscriptions[ins_id]
        channel = "live_orders{}".format(
                self.insid_to_postfix(ins_id))
        await self._subscribe_channel(ins_id, channel)
        channel = "detail_order_book{}".format(
                self.insid_to_postfix(ins_id))
        await self._subscribe_channel(ins_id, channel)
        await self._listen_internal(channel)
        await self._pusher.unsubscribe(channel)
        sd["mbo"] = True


    async def _unsubscribe_mbo(self, ins_id):
        sd = self._subscriptions[ins_id]
        channel = "live_orders{}".format(
                self.insid_to_postfix(ins_id))
        await self._pusher.unsubscribe(channel)
        sd["mbo"] = False


    async def subscribe(self, ins_id, body):
        sd = self._subscriptions[ins_id]
        ticks = body["NoMDEntryTypes"]
        if "*" in ticks:
            ticks += fix.MDEntryType.Trade
            ticks += fix.MDEntryType.Bid
            ticks += fix.MDEntryType.Offer
        mbp_book = body["ZMMBPBook"]
        mbo_book = body["ZMMBOBook"]
        res = []
        async with sd["lock"]:
            if not sd["trades"] and fix.MDEntryType.Trade in ticks:
                await self._subscribe_trades(ins_id)
                res.append("subscribed trades")
            elif sd["trades"] and fix.MDEntryType.Trade not in ticks:
                await self._unsubscribe_trades(ins_id)
                res.append("unsubscribed trades")
            if fix.MDEntryType.Bid in ticks or fix.MDEntryType.Offer in ticks:
                if not sd["mbp"] and mbp_book:
                    await self._subscribe_mbp(ins_id)
                    res.append("subscribed mbp")
                elif sd["mbp"] and not mbp_book:
                    await self._unsubscribe_mbp(ins_id)
                    res.append("unsubscribed mbp")
                if not sd["mbo"] and mbo_book:
                    await self._subscribe_mbo(ins_id)
                    res.append("subscribed mbo")
                elif sd["mbo"] and not mbo_book:
                    await self._unsubscribe_mbo(ins_id)
                    res.append("unsubscribed mbo")
            else:
                await self._unsubscribe_mbp(ins_id)
                res.append("unsubscribed mbp")
                await self._unsubscribe_mbo(ins_id)
                res.append("unsubscribed mbo")
        return ", ".join(res)


    async def unsubscribe(self, ins_id):
        body = {}
        body["NoMDEntryTypes"] = ""
        return await self.subscribe(ins_id, body)

    # async def _send_disconnected(self):
    #     seq_no = g.seq_no
    #     g.seq_no += 1
    #     msg = {}
    #     msg["Header"] = header = {}
    #     header["ZMSendingTime"] = get_timestamp()
    #     header["MsgSeqNum"] = seq_no
    #     msg["Body"] = body = {}
    #     body["UserStatus"] = fix.UserStatus.Disconnected
    #     msg_bytes = (" " + json.dumps(msg)).encode()
    #     await self._sock.send_multipart([
    #         fix.MsgType.UserNotification.encode(),
    #         msg_bytes
    #     ])

    
    async def _restart(self):
        if self._pusher:
            await self._pusher.close()
        # await self._send_disconnected()
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
        pprint(msg)
        channel = msg["channel"]
        ins_id = self._channel_to_ins_id.get(channel)
        if not ins_id:
            return
        data = msg["data"]
        event = msg["event"]
        if not data:
            return
        if channel.startswith("live_trades"):
            await self._handle_trade(ins_id, data)
        elif channel.startswith("order_book"):
            await self._handle_mbp_snapshot(ins_id, data)
            await self._internal_pub.send_multipart(
                    [channel.encode() + b"\00", json.dumps(data).encode()])
        elif channel.startswith("diff_order_book"):
            await self._handle_mbp_delta(ins_id, data)
        elif channel.startswith("detail_order_book"):
            await self._handle_mbo_snapshot(ins_id, data)
            await self._internal_pub.send_multipart(
                    [channel.encode() + b"\00", json.dumps(data).encode()])
        elif channel.startswith("live_orders"):
            await self._handle_mbo_delta(ins_id, event, data)


    async def _handle_trade(self, ins_id, data):
        msg = {}
        msg["Header"] = header = {}
        header["MsgType"] = fix.MsgType.MarketDataIncrementalRefresh
        header["ZMSendingTime"] = get_timestamp()
        msg["Body"] = body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDUpdateAction"] = "0"
        d["MDEntryType"] = "2"
        d["MDEntryPx"] = float(data["price"])
        d["MDEntrySize"] = float(data["amount"])
        ts = bitstamp_ts_to_zmapi_ts(data["microtimestamp"])
        d["TransactTime"] = ts
        d["ZMInstrumentID"] = ins_id
        group.append(d)
        await self._sock.send_string(" " + json.dumps(msg))


    async def _handle_mbp_snapshot(self, ins_id, data):
        msg = {}
        msg["Header"] = header = {}
        header["MsgType"] = fix.MsgType.MarketDataIncrementalRefresh
        ts = bitstamp_ts_to_zmapi_ts(data["microtimestamp"])
        header["ZMSendingTime"] = get_timestamp()
        msg["Body"] = body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDEntryType"] = fix.MDEntryType.EmptyBook
        d["ZMInstrumentID"] = ins_id
        d["MDBookType"] = fix.MDBookType.PriceDepth
        group.append(d)
        for price, size in data.get("bids", []):
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.New
            d["MDEntryType"] = fix.MDEntryType.Bid
            d["MDEntryPx"] = float(price)
            d["MDEntrySize"] = float(size)
            d["TransactTime"] = ts
            group.append(d)
        for price, size in data.get("asks", []):
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.New
            d["MDEntryType"] = fix.MDEntryType.Offer
            d["MDEntryPx"] = float(price)
            d["MDEntrySize"] = float(size)
            d["TransactTime"] = ts
            group.append(d)
        group[0]["ZMInstrumentID"] = ins_id
        await self._sock.send_string(" " + json.dumps(msg))


    async def _handle_mbp_delta(self, ins_id, data):
        msg = {}
        msg["Header"] = header = {}
        header["MsgType"] = fix.MsgType.MarketDataIncrementalRefresh
        ts = bitstamp_ts_to_zmapi_ts(data["microtimestamp"])
        header["ZMSendingTime"] = get_timestamp()
        msg["Body"] = body = {}
        body["NoMDEntries"] = group = []
        for price, size in data.get("bids", []):
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
            d["MDEntryType"] = fix.MDEntryType.Bid
            d["MDEntryPx"] = float(price)
            d["MDEntrySize"] = float(size)
            d["TransactTime"] = ts
            group.append(d)
        for price, size in data.get("asks", []):
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.Overlay
            d["MDEntryType"] = fix.MDEntryType.Offer
            d["MDEntryPx"] = float(price)
            d["MDEntrySize"] = float(size)
            d["TransactTime"] = ts
            group.append(d)
        group[0]["ZMInstrumentID"] = ins_id
        await self._sock.send_string(" " + json.dumps(msg))


    async def _handle_mbo_snapshot(self, ins_id, data):
        msg = {}
        msg["Header"] = header = {}
        header["MsgType"] = fix.MsgType.MarketDataIncrementalRefresh
        ts = bitstamp_ts_to_zmapi_ts(data["microtimestamp"])
        header["ZMSendingTime"] = get_timestamp()
        msg["Body"] = body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["MDEntryType"] = fix.MDEntryType.EmptyBook
        d["ZMInstrumentID"] = ins_id
        d["MDBookType"] = fix.MDBookType.OrderDepth
        group.append(d)
        for price, size, order_id in data.get("bids", []):
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.New
            d["MDEntryType"] = fix.MDEntryType.Bid
            d["MDEntryPx"] = float(price)
            d["MDEntrySize"] = float(size)
            d["OrderID"] = order_id
            d["TransactTime"] = ts
            group.append(d)
        for price, size, order_id in data.get("asks", []):
            d = {}
            d["MDUpdateAction"] = fix.MDUpdateAction.New
            d["MDEntryType"] = fix.MDEntryType.Offer
            d["MDEntryPx"] = float(price)
            d["MDEntrySize"] = float(size)
            d["OrderID"] = order_id
            d["TransactTime"] = ts
            group.append(d)
        group[0]["ZMInstrumentID"] = ins_id
        await self._sock.send_string(" " + json.dumps(msg))


    ORD_EVENT_TO_UA = {
        "order_created": fix.MDUpdateAction.New,
        "order_changed": fix.MDUpdateAction.Change,
        "order_deleted": fix.MDUpdateAction.Delete,
    }


    async def _handle_mbo_delta(self, ins_id, event, data):
        msg = {}
        msg["Header"] = header = {}
        header["MsgType"] = fix.MsgType.MarketDataIncrementalRefresh
        ts = bitstamp_ts_to_zmapi_ts(data["microtimestamp"])
        header["ZMSendingTime"] = get_timestamp()
        msg["Body"] = body = {}
        body["NoMDEntries"] = group = []
        d = {}
        d["ZMInstrumentID"] = ins_id
        d["MDUpdateAction"] = self.ORD_EVENT_TO_UA[event]
        d["MDEntryPx"] = data["price"]
        d["MDEntrySize"] = data["amount"]
        d["MDEntryType"] = str(data["order_type"])
        d["OrderID"] = str(data["id"])
        d["TransactTime"] = ts
        group.append(d)
        await self._sock.send_string(" " + json.dumps(msg))


    async def run(self):
        await self._restart()


    @staticmethod
    def insid_to_postfix(ins_id):
        if ins_id == "btcusd":
            return ""
        return "_{}".format(ins_id)




    # async def subscribe_order_book(self, instrument_id):
    #     sub_def = self._subscriptions[instrument_id]
    #     if sub_def["ob"]:
    #         return "no change"
    #     channel = "diff_order_book{}".format(self.tid_to_postfix(instrument_id))
    #     self._channel_to_tid[channel] = instrument_id
    #     # pusher_internal:subscription_succeeded fires even when
    #     # the channel does not even exist. Not useful to wait for this
    #     # confirmation.
    #     await self._pusher.subscribe(channel)
    #     sub_def["ob"] = True
    #     return "subscribed"


    # async def unsubscribe_order_book(self, instrument_id):
    #     sub_def = self._subscriptions[instrument_id]
    #     if not sub_def["ob"]:
    #         return "no change"
    #     channel = "diff_order_book{}".format(self.tid_to_postfix(instrument_id))
    #     await self._pusher.unsubscribe(channel)
    #     # data = [instrument_id.encode() + b"\x01", b""]
    #     # await self._sock.send_multipart(data)
    #     sub_def["ob"] = False
    #     return "unsubscribed"


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

