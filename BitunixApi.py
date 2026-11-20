import secrets
import base64
import time
import json
import hashlib
import asyncio
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError, Timeout
from urllib.parse import urlencode
from typing import Dict, Any
import traceback
from logger import Logger
#import ccxt


class BitunixApi:
    
    def __init__(self, api_key, secret_key, settings, logger):
        self.api_key = api_key
        self.secret_key = secret_key
        self.logger = logger
        self.settings = settings
        self._precision_cache = {}
        self._open_session()
        
        self.pending_positions_URL="https://fapi.bitunix.com/api/v1/futures/position/get_pending_positions"
        self.account_Url="https://fapi.bitunix.com/api/v1/futures/account"
        self.ticker_Url='https://fapi.bitunix.com/api/v1/futures/market/tickers'
        self.ticker_pair_Url='https://fapi.bitunix.com/api/v1/futures/market/trading_pairs'
        self.kline_Url='https://fapi.bitunix.com/api/v1/futures/market/kline'
        self.depth_Url='https://fapi.bitunix.com/api/v1/futures/market/depth'
        self.placeOrder_Url="https://fapi.bitunix.com/api/v1/futures/trade/place_order"
        self.flashClose_Url="https://fapi.bitunix.com/api/v1/futures/trade/flash_close_position"
        self.pending_order_url="https://fapi.bitunix.com/api/v1/futures/trade/get_pending_orders"
        self.cancelOrder_Url="https://fapi.bitunix.com/api/v1/futures/trade/cancel_orders"
        self.Trade_history_Url="https://fapi.bitunix.com/api/v1/futures/trade/get_history_trades"
        self.position_history_Url="https://fapi.bitunix.com/api/v1/futures/position/get_history_positions"
 
    def _open_session(self):
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'BitunixAPI/1.0'
        })

    def _reset_session(self):
        try:
            self.session.close()
        except Exception:
            pass
        self._open_session()

    async def _get_response(self, url, **kwargs):
        """GET with a fresh connection when Bitunix drops a keep-alive socket."""
        kwargs.setdefault("timeout", 20)
        last_error = None
        for attempt in range(3):
            try:
                response = self.session.get(url, **kwargs)
                response.raise_for_status()
                return response
            except (ConnectionError, Timeout, ChunkedEncodingError) as error:
                last_error = error
                self._reset_session()
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                raise
        raise last_error

    async def update_settings(self, settings):
        self.settings = settings
       
    async def create_timestamp(self):
        return str(int(time.time()*1000))

    async def is_near_high_of_day(self, current_value, high_of_day, threshold_percentage=5):
        if not high_of_day:
            return False
        difference_percentage = ((high_of_day - current_value) / high_of_day) * 100
        return difference_percentage <= threshold_percentage

    async def is_near_low_of_day(self, current_value, low_of_day, threshold_percentage=5):
        if not low_of_day:
            return False
        difference_percentage = ((current_value - low_of_day) / low_of_day) * 100
        return difference_percentage <= threshold_percentage


    async def _generate_nonce(self) -> str:
        random_bytes = secrets.token_bytes(32)
        return base64.b64encode(random_bytes).decode('utf-8')


    async def sign_request(self, nonce: str, timestamp: str, 
                     query_params: str = None, 
                     body: str = None) -> str:
        query_string = query_params if query_params else ""
        body_string = body if body else ""
        message = f"{nonce}{timestamp}{self.api_key}{query_string}{body_string}"

        # First SHA256 encryption
        digest = hashlib.sha256(message.encode()).hexdigest()

        # Second SHA256 encryption
        sign = hashlib.sha256((digest + self.secret_key).encode()).hexdigest()

        return sign
        
    async def _get(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
            response = await self._get_response(endpoint, params=params)
            return response.json()    

    async def _get_authenticated(self, endpoint: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        last_error = None
        for attempt in range(3):
            timestamp = await self.create_timestamp()
            nonce = await self._generate_nonce()
            headers = {
                "api-key": self.api_key,
                "timestamp": timestamp,
                "nonce": nonce,
            }
            query_string = urlencode(sorted(params.items())).replace('=','').replace('&','')  if params else ""
            signature = await self.sign_request(nonce, timestamp, query_params=query_string)
            headers["sign"] = signature
            try:
                response = self.session.get(endpoint, params=params, headers=headers, timeout=20)
                response.raise_for_status()
                return response.json()
            except (ConnectionError, Timeout, ChunkedEncodingError) as error:
                last_error = error
                self._reset_session()
                if attempt < 2:
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                raise
        raise last_error

    async def _post_authenticated(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        timestamp = await self.create_timestamp()
        nonce = await self._generate_nonce()
        
        headers = {
            "api-key": self.api_key,
            "timestamp": timestamp,
            "nonce": nonce,
            "Content-Type": "application/json"
        }
        
        body_string = json.dumps(data, separators=(',', ':'))
        signature = await self.sign_request(nonce, timestamp, body=body_string)
        headers["sign"] = signature

        response = self.session.post(endpoint, data=body_string, headers=headers)
        self.logger.info(f"Response: {body_string} {response.json()}")
        response.raise_for_status()
        return response.json()

    def _quantize(self, value, precision, rounding=ROUND_DOWN):
        quant = Decimal(1).scaleb(-int(precision))
        return format(Decimal(str(value)).quantize(quant, rounding=rounding), "f")

    async def _symbol_precision(self, ticker):
        cached = self._precision_cache.get(ticker)
        if cached:
            return cached
        pdata = await self.GetTickersPair(ticker)
        if pdata:
            precision = (int(pdata[0].get("basePrecision", 0)), int(pdata[0].get("quotePrecision", 8)))
        else:
            precision = (0, 8)
        self._precision_cache[ticker] = precision
        return precision

    async def PlaceOrder(self, ticker, qty, price, side, positionId=0, tradeSide="OPEN",reduceOnly=False, takeProfitPrice=0, stopLossPrice=0):
        datajs = None
        base_precision, quote_precision = await self._symbol_precision(ticker)
        qty = self._quantize(qty, base_precision, ROUND_DOWN)
        price = self._quantize(price, quote_precision, ROUND_HALF_UP)
        if Decimal(qty) <= 0:
            self.logger.info(f"Order qty for {ticker} rounded to 0 at base precision {base_precision}")
            return None
        if takeProfitPrice:
            takeProfitPrice = self._quantize(takeProfitPrice, quote_precision, ROUND_HALF_UP)
        if stopLossPrice:
            stopLossPrice = self._quantize(stopLossPrice, quote_precision, ROUND_HALF_UP)
        if takeProfitPrice == 0 or stopLossPrice == 0:
            data = {
                "side": side,
                "orderType":"LIMIT",
                "qty":  qty,
                "price": price,
                "symbol": ticker,
                "tradeSide":tradeSide,
                "reduceOnly":reduceOnly,
            }
            if positionId:
                data["positionId"] = positionId
            datajs = await self._post_authenticated(self.placeOrder_Url,data)
        elif takeProfitPrice == 0:
            data = {
                "side": side,
                "orderType":"LIMIT",
                "qty":  qty,
                "price": price,
                "symbol": ticker,
                "tradeSide":tradeSide,
                "slPrice": stopLossPrice,
                "slStopType":'MARK_PRICE',
                "slStopType": self.settings.PROFIT_LOSS_PRICE_TYPE,
                "slOrderType": self.settings.PROFIT_LOSS_ORDER_TYPE,
            }
            if positionId:
                data["positionId"] = positionId
            datajs = await self._post_authenticated(self.placeOrder_Url,data)
        elif stopLossPrice == 0:
            data = {
                "side": side,
                "orderType":"LIMIT",
                "qty":  qty,
                "price": price,
                "symbol": ticker,
                "tradeSide":tradeSide,
                "tpPrice": takeProfitPrice,
                "tpStopType": self.settings.PROFIT_LOSS_PRICE_TYPE,
                "tpOrderType": self.settings.PROFIT_LOSS_ORDER_TYPE,
                "tpOrderPrice":takeProfitPrice,
            }
            if positionId:
                data["positionId"] = positionId
            datajs = await self._post_authenticated(self.placeOrder_Url,data)
        else:
            data = {
                "side": side,
                "orderType":"LIMIT",
                "qty":  qty,
                "price": price,
                "symbol": ticker,
                "tradeSide":tradeSide,
                "tpPrice": takeProfitPrice,
                "tpStopType": self.settings.PROFIT_LOSS_PRICE_TYPE,
                "tpOrderType": self.settings.PROFIT_LOSS_ORDER_TYPE,
                "tpOrderPrice":takeProfitPrice,
                "slPrice": stopLossPrice,
                "slStopType": self.settings.PROFIT_LOSS_PRICE_TYPE,
                "slOrderType": self.settings.PROFIT_LOSS_ORDER_TYPE,
                "slOrderPrice": stopLossPrice,
            }
            if positionId:
                data["positionId"] = positionId
            datajs = await self._post_authenticated(self.placeOrder_Url,data)
            
        return datajs

    async def FlashClose(self, positionId):
        data = {
            "positionId": positionId
        }
        datajs = await self._post_authenticated(self.flashClose_Url,data)
        return datajs

    async def CancelOrder(self, symbol, orderId):
        data = {
            "symbol": symbol,
            "orderList":[{"orderId": orderId}]
        }
        datajs = await self._post_authenticated(self.cancelOrder_Url,data)
        return datajs

    async def GetTradeHistoryData(self):
        tradeHistory=await self._get_authenticated(self.Trade_history_Url)
        if tradeHistory['code']==0:
            return tradeHistory['data']
        else:
            self.logger.info(tradeHistory['msg'])

    async def GetPendingOrderData(self,dictparm={}):
        orders=await self._get_authenticated(self.pending_order_url, dictparm)
        if orders['code']==0:
            return orders['data']
        else:
            self.logger.info(orders['msg'])

    async def GetPendingPositionData(self, dictparm={}):
        positions=await self._get_authenticated(self.pending_positions_URL, dictparm)
        if positions['code']==0:
            return positions['data']
        else:
            self.logger.info(positions['msg'])

    async def GetPositionHistoryData(self, dictparm={}):
        tradeHistory=await self._get_authenticated(self.position_history_Url, dictparm)
        if tradeHistory['code']==0:
            return tradeHistory['data']
        else:
            self.logger.info(tradeHistory['msg'])

    
    async def GetportfolioData(self):
        portfolio=await self._get_authenticated(self.account_Url, params={"marginCoin":"USDT"})
        if portfolio['code']==0:
            return portfolio['data']
        else:
            self.logger.info(portfolio['msg'])
           

    async def GetTickerslastPrice(self, tickersStr):
        try:
            resp = await self._get_response(self.ticker_Url+'?symbols='+tickersStr)
            datajs = resp.json()
            if datajs['code']==0:
                return datajs['data']
        except Exception as e:
            stack = traceback.extract_stack()
            function_name = stack[-2].name
            self.logger.error(f"Function: {function_name}, {e}, {e.args}, {type(e).__name__}")

    async def GetTickersPair(self, tickersStr):
        try:
            resp = await self._get_response(self.ticker_pair_Url+'?symbols='+tickersStr)
            datajs = resp.json()
            if datajs['code']==0:
                return datajs['data']
        except Exception as e:
            stack = traceback.extract_stack()
            function_name = stack[-2].name
            self.logger.error(f"Function: {function_name}, {e}, {e.args}, {type(e).__name__}")

    async def GetTickerList(self, threshold, volume):
        symbols=[]
        try:
            resp = await self._get_response(self.ticker_Url)
            datajs = resp.json()
            for item in datajs["data"]: 
                if await self.is_near_high_of_day(float(item['last']), float(item['high']) ,threshold) and float(item['baseVol']) > volume:
                        symbols.append(item['symbol'])
                if await self.is_near_low_of_day(float(item['last']), float(item['low']) ,threshold) and float(item['baseVol']) > volume:
                        symbols.append(item['symbol'])
                #if float(item['baseVol']) > volume:
                #    symbols.append(item['symbol'])
            return symbols
        except Exception as e:
            stack = traceback.extract_stack()
            function_name = stack[-2].name
            self.logger.info(f"Function: {function_name}, {e}, {e.args}, {type(e).__name__}")
            return symbols
            
    async def GetTickerData(self):
        try:
            url = f'{self.ticker_Url}'
            resp = await self._get_response(url)
            datajs = resp.json()
            if datajs['code']==0:
                return datajs['data']
        except Exception as e:
            stack = traceback.extract_stack()
            function_name = stack[-2].name
            self.logger.info(f"Function: {function_name}, {e}, {e.args}, {type(e).__name__}")           

    async def GetDepthData(self,symbol,limit):
        try:
            url = f'{self.depth_Url}?symbol={symbol}&limit={limit}'
            resp = await self._get_response(url)
            datajs = resp.json()
            if datajs['code']==0:
                return datajs['data']
        except Exception as e:
            stack = traceback.extract_stack()
            function_name = stack[-2].name
            self.logger.info(f"Function: {function_name}, {e}, {e.args}, {type(e).__name__}")           

    async def GetKlineHistory(self, ticker, interval, limit, starttime=None):
        """Latest closed candles, newest first.

        Bitunix counts backward from an exclusive endTime and allows at most
        200 candles per request. startTime is no longer a request parameter,
        so the old starttime argument is ignored.
        """
        wanted = max(int(limit), 1)
        collected = []
        end_time = None
        try:
            while len(collected) < wanted:
                page_limit = min(200, wanted - len(collected))
                params = {
                    "symbol": ticker,
                    "interval": interval,
                    "limit": page_limit,
                }
                if end_time is not None:
                    params["endTime"] = end_time

                datajs = None
                for _ in range(3):
                    resp = await self._get_response(self.kline_Url, params=params, timeout=20)
                    datajs = resp.json()
                    if datajs.get("code") in (10005, 10006):
                        await asyncio.sleep(1)
                        continue
                    break
                # Public market endpoints are limited to 10 requests per second per IP.
                await asyncio.sleep(0.12)

                if not datajs or datajs.get("code") != 0:
                    if datajs:
                        self.logger.info(datajs.get("msg"))
                    break
                batch = datajs.get("data") or []
                if not batch:
                    break
                collected.extend(batch)
                if len(batch) < page_limit:
                    break
                end_time = min(int(item["time"]) for item in batch)

            if not collected:
                return None
            collected.sort(key=lambda item: int(item["time"]), reverse=True)
            return collected[:wanted]
        except Exception as e:
            stack = traceback.extract_stack()
            function_name = stack[-2].name
            self.logger.info(f"Function: {function_name}, {e}, {e.args}, {type(e).__name__}") 

    def __del__(self):
            """Cleanup method to close the session"""
            if hasattr(self, 'session'):
                self.session.close()
