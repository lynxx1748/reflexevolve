import numpy as np
import pandas as pd
import json
import asyncio
from datetime import datetime, timedelta, timezone
from collections import defaultdict
import traceback
import time
import pytz
from concurrent.futures import ProcessPoolExecutor
from BitunixWebSocket import BitunixPrivateWebSocketClient, BitunixPublicWebSocketClient
from AsyncThreadRunner import AsyncThreadRunner
from TickerManager import Tickers, Ticker, Interval
from DataFrameHtmlRenderer import DataFrameHtmlRenderer
from TradeJournal import TradeJournal
from logger import Logger, Colors
colors = Colors()
import gc
import os
from concurrent.futures import ProcessPoolExecutor
import sqlite3
import queue
from collections import deque
import threading


cst = pytz.timezone('US/Central')


def _price_text(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:.8f}".rstrip("0").rstrip(".")


class BitunixSignal:
    def __init__(self, api_key, secret_key, settings, threadManager, notifications, bitunixApi, logger):
        self.api_key = api_key
        self.secret_key = secret_key
        self.settings=settings
        self.threadManager = threadManager
        self.notifications = notifications
        self.bitunixApi = bitunixApi
        self.logger = logger
              
        #Ticker object
        self.tickerObjects = Tickers(self.settings, self.logger)
        
        #these are used for html rendering as well as storing
        self.signaldf= pd.DataFrame()
        self.positiondf= pd.DataFrame()
        self.positiondf2= pd.DataFrame()
        self.portfoliodf=pd.DataFrame()
        self.orderdf=pd.DataFrame()
        self.tickerdf=pd.DataFrame()
        self.inspectdf=pd.DataFrame()
        self.tradesdf=pd.DataFrame() 
        self.positionHistorydf=pd.DataFrame() 
        
        self.portfoliodfrenderer=None
        self.positiondfrenderer=None
        self.orderdfrenderer=None
        self.signaldfrenderer=None
        self.allsignaldfrenderer=None
        self.positionHistorydfrenderer=None
        
        self.portfoliodfStyle=None
        self.positiondfStyle=None   
        self.orderdfStyle=None
        self.signaldfStyle=None
        self.allsignaldfStyle=None
        self.positionHistorydfStyle=None
        
        #postion, order, etc
        self.pendingOrders=None
        self.pendingPositions=None
        self.portfolioData=None
        self.tradeHistoryData=None
        self.positionHistoryData=None
        
        #this contain all data
        self.signaldf_full = pd.DataFrame()
        self.signaldf_filtered = pd.DataFrame()

        #websockets
        self.bitunixPrivateWebSocketClient = BitunixPrivateWebSocketClient(self.api_key, self.secret_key, logger)
        
        if self.settings.USE_PUBLIC_WEBSOCKET:
            self.bitunixPublicDepthWebSocketClient = BitunixPublicWebSocketClient(self.api_key, self.secret_key, "depth", logger)
            self.bitunixPublicTickerWebSocketClient = BitunixPublicWebSocketClient(self.api_key, self.secret_key, "ticker", logger)

        self.tickerList=[]

        self.green="#A5DFDF"
        self.red="#FFB1C1"
        
        self.journal = TradeJournal(self.settings.DATABASE)
        self.profit = self.journal.total_realized_pnl()
        self._history_backfill_done = False
        self.open_block_reason = ""
        self._skip_notes = []
        self._last_skip_notice = ""
        self._halt_notice_until = 0.0
        
        self.lastAutoTradeTime = time.time()
        self.lastTickerDataTime = time.time()
        self._depth_lock = threading.Lock()
        self._next_depth_at = 0.0
        self._loss_state_lock = threading.Lock()
        self._reentry_lock = threading.Lock()
        self._reentry_until = self._read_reentry()
        
        #sqllite database connection
        self.connection = sqlite3.connect(self.settings.DATABASE) 
        
    async def update_settings(self, settings):
        self.settings = settings
        self.tickerObjects.update_settings(settings)
        
    async def load_tickers(self):
        if self.settings.TICKERS=="":
            symbols = await self.bitunixApi.GetTickerList(float(self.settings.THRESHOLD), float(self.settings.MIN_VOLUME)) or []
            self.pendingPositions= await self.bitunixApi.GetPendingPositionData()
            self.pendingOrders= await self.bitunixApi.GetPendingOrderData()
            olist=[]
            plist=[]
            if self.pendingPositions:
                plist = [entry['symbol'] for entry in self.pendingPositions]
            order_list = self.pendingOrders.get('orderList') if isinstance(self.pendingOrders, dict) else None
            if order_list:
                olist = [entry['symbol'] for entry in order_list]
            newlist=olist+plist+list(set(symbols))
            self.tickerList=newlist[:300]
            if "STMXUSDT" in self.tickerList:
                self.tickerList.remove("STMXUSDT")
            if "AMBUSDT" in self.tickerList:
                self.tickerList.remove("AMBUSDT")
            #self.tickerList=['POLUSDT']
        else:
            self.tickerList = self.settings.TICKERS.split(",")
            self.tickerList = [sym.strip().upper() for sym in self.tickerList if sym.strip()]

        [await self.add_ticker_to_tickerObjects(sym) for sym in self.tickerList]
        self.notifications.add_notification(f"{len(self.tickerList)} ticker list loaded") 
        
    async def add_ticker_to_tickerObjects(self, symbol):
        if not self.tickerObjects.get(symbol):
            self.tickerObjects.add(symbol)
            
    async def start_jobs(self):
        #setup renderers
        await asyncio.create_task(self.DefinehtmlRenderers())

        #async thread that runs forever jobs
        self.GetportfolioDataTask = AsyncThreadRunner(self.GetportfolioData, self.logger, interval=int(self.settings.PORTFOLIO_API_INTERVAL))
        self.GetportfolioDataTask.start_thread(thread_name="GetportfolioData")

        self.GetPendingPositionDataTask = AsyncThreadRunner(self.GetPendingPositionData, self.logger, interval=int(self.settings.PENDING_POSITIONS_API_INTERVAL))
        self.GetPendingPositionDataTask.start_thread(thread_name="GetPendingPositionData")

        self.GetPendingOrderDataTask = AsyncThreadRunner(self.GetPendingOrderData, self.logger, interval=int(self.settings.PENDING_ORDERS_API_INTERVAL))
        self.GetPendingOrderDataTask.start_thread(thread_name="GetPendingOrderData")

        self.GetTradeHistoryDataTask = AsyncThreadRunner(self.GetTradeHistoryData, self.logger, interval=int(self.settings.TRADE_HISTORY_API_INTERVAL))
        self.GetTradeHistoryDataTask.start_thread(thread_name="GetTradeHistoryData")
       
        self.GetPositionHistoryDataTask = AsyncThreadRunner(self.GetPositionHistoryData, self.logger, interval=int(self.settings.POSITION_HISTORY_API_INTERVAL))
        self.GetPositionHistoryDataTask.start_thread(thread_name="GetPositionHistoryData")
       
        self.ProcessPrivateDataTask = AsyncThreadRunner(self.bitunixPrivateWebSocketClient.run_websocket, self.logger, 0, self.ProcessPrivateData)
        self.ProcessPrivateDataTask.start_thread(thread_name="ProcessPrivateData")

        if self.settings.USE_PUBLIC_WEBSOCKET:
            self.bitunixPublicDepthWebSocketClient.tickerList = self.tickerList
            self.StoreDepthDataTask = AsyncThreadRunner(self.bitunixPublicDepthWebSocketClient.run_websocket, self.logger, 0, self.StoreDepthData)
            self.StoreDepthDataTask.start_thread(thread_name="StoreDepthData")
            self.depth_que = asyncio.Queue()
            self.ProcessDepthDataTask = AsyncThreadRunner(self.ProcessDepthData, self.logger, interval=0) # run only once
            self.ProcessDepthDataTask.start_thread(thread_name="ProcessDepthData")

            self.bitunixPublicTickerWebSocketClient.tickerList = self.tickerList
            self.StoreTickerDataTask = AsyncThreadRunner(self.bitunixPublicTickerWebSocketClient.run_websocket, self.logger, 0, self.StoreTickerData)
            self.StoreTickerDataTask.start_thread(thread_name="StoreTickerData")
            self.ticker_que = asyncio.Queue()
            self.ProcessTickerDataTask = AsyncThreadRunner(self.ProcessTickerData, self.logger, interval=0) # run only once
            self.ProcessTickerDataTask.start_thread(thread_name="ProcessTickerData")
            

        #normal processes
        self.LoadKlineHistoryTask = AsyncThreadRunner(self.LoadKlineHistory, self.logger, interval=0) # run only once
        self.LoadKlineHistoryTask.start_thread(thread_name="LoadKlineHistory")

        self.AutoTradeProcessTask = AsyncThreadRunner(self.AutoTradeProcess, self.logger, interval=int(self.settings.SIGNAL_CHECK_INTERVAL))
        self.AutoTradeProcessTask.start_thread(thread_name="AutoTradeProcess")

        self.checkTickerAndAutotradeStatusTask = AsyncThreadRunner(self.checkTickerAndAutotradeStatus, self.logger, interval=0)
        self.checkTickerAndAutotradeStatusTask.start_thread(thread_name="checkTickerAndAutotradeStatus")

        if not self.settings.USE_PUBLIC_WEBSOCKET:
            self.GetTickerDataTask = AsyncThreadRunner(self.GetTickerData, self.logger, interval=int(self.settings.TICKER_DATA_API_INTERVAL))
            self.GetTickerDataTask.start_thread(thread_name="GetTickerData")



    ###########################################################################################################
    async def DefinehtmlRenderers(self):
        period = self.settings.OPTION_MOVING_AVERAGE
        #html rendering setup
        self.portfoliodfrenderer = DataFrameHtmlRenderer()
        self.positiondfrenderer = DataFrameHtmlRenderer(hide_columns=["positionId", "lastcolor","bidcolor","askcolor",f"{period}_barcolor"], \
                                                color_column_mapping={"bid": "bidcolor",
                                                                        "last": "lastcolor",
                                                                        "ask": "askcolor",
                                                                    f"{period}_cb": f"{period}_barcolor"
                                                            })
        self.orderdfrenderer = DataFrameHtmlRenderer()
        self.signaldfrenderer = DataFrameHtmlRenderer(hide_columns=["1d_barcolor","1h_barcolor","15m_barcolor","5m_barcolor","1m_barcolor","lastcolor","bidcolor","askcolor"], \
                                                color_column_mapping={"bid": "bidcolor",
                                                                    "last": "lastcolor",
                                                                    "ask": "askcolor",
                                                                    "1d_cb": "1d_barcolor",
                                                                    "1h_cb": "1h_barcolor",
                                                                    "15m_cb": "15m_barcolor",
                                                                    "5m_cb": "5m_barcolor",
                                                                    "1m_cb": "1m_barcolor"
                                                            })

        #html rendering setup
        self.allsignaldfrenderer = DataFrameHtmlRenderer(hide_columns=["1d_barcolor","1h_barcolor","15m_barcolor","5m_barcolor","1m_barcolor","lastcolor","bidcolor","askcolor"], \
                                                color_column_mapping={"bid": "bidcolor",
                                                                    "last": "lastcolor",
                                                                    "ask": "askcolor",
                                                                    "1d_cb": "1d_barcolor",
                                                                    "1h_cb": "1h_barcolor",
                                                                    "15m_cb": "15m_barcolor",
                                                                    "5m_cb": "5m_barcolor",
                                                                    "1m_cb": "1m_barcolor"
                                                            })        
        self.positionHistorydfrenderer = DataFrameHtmlRenderer()
        

    ###########################################################################################################
    #load kline history
    async def LoadKlineHistory(self):
        start = time.time()
        intervals = self.tickerObjects.get_intervalIds()
        for ticker in self.tickerList:
            for intervalId in intervals:
                data = await self.bitunixApi.GetKlineHistory(ticker, intervalId, self.settings.BARS)
                if data is not None:
                   self.tickerObjects.load_kline_history(ticker, intervalId, self.settings.BARS, data)
        if self.settings.VERBOSE_LOGGING:
            self.logger.info(f"kline_history: elapsed time {time.time()-start}")
        self.notifications.add_notification("Kline history loaded")

    #api data       
    async def GetTickerData(self):
        if not self.settings.USE_PUBLIC_WEBSOCKET:
            start=time.time()
            # Get the current time and set the seconds and microseconds to zero
            current_time = datetime.now()
            current_minute = current_time.replace(second=0, microsecond=0)
            ts = int(current_minute.timestamp())*1000

            #api used insted of websocket
            try:
                data = await self.bitunixApi.GetTickerData()
            except Exception as e:
                self.logger.info(f"Function: GetTickerData, {e}, {e.args}, {type(e).__name__}")
                data = None
            self.tickerdf = pd.DataFrame()
            if data:
                
                # Create a DataFrame from the data
                self.tickerdf = pd.DataFrame(data, columns=["symbol", "last"])
                
                #remove not required symbols
                self.tickerdf.loc[~self.tickerdf['symbol'].isin(self.tickerObjects.symbols()), :] = None
                self.tickerdf.dropna(inplace=True)
                
                self.tickerdf['ts']=ts
                self.tickerdf["tickerObj"] = self.tickerdf["symbol"].map(self.tickerObjects.get_tickerDict())
                self.tuples_list = list(zip(self.tickerdf["tickerObj"],  self.tickerdf["last"].astype(float), self.tickerdf["ts"]))
                self.tickerObjects.form_candle(self.tuples_list)

            await self.refresh_watchlist_depth()
            self.lastTickerDataTime = time.time()
            if self.settings.VERBOSE_LOGGING:
                self.logger.info(f"GetTickerData: elapsed time {time.time()-start}")
            if self.settings.BENCHMARK:
                self.connection = sqlite3.connect(self.settings.DATABASE) 
                self.cursor = self.connection.cursor()
                self.cursor.execute("INSERT INTO benchmark (process_name, time) VALUES (?, ?)", ("GetTickerData", time.time()-start))
                self.connection.commit()
    
    async def drain_queue(self, queue):
        items = []
        while not queue.empty():
            items.append(await queue.get())
        return deque(items[::-1][:self.settings.BATCH_PROCESS_SIZE])  # Reverse items    

    #websocket data to update ticker lastprice and other relavent data
    # Function to add data to the last price deque
    async def StoreTickerData(self, message):
        if self.settings.USE_PUBLIC_WEBSOCKET and message:
            await self.ticker_que.put(message)

    # Function to process the last price deque
    async def ProcessTickerData(self):
        while True:
            try:
                latest_data = {}
                reversed_items = await self.drain_queue(self.ticker_que)
                while reversed_items:
                    message = reversed_items.popleft()
                    data = json.loads(message)
                    if data.get('symbol') and data.get('ch') == 'ticker':
                        symbol = data["symbol"]
                        ts = data["ts"]
                        if symbol not in latest_data or ts > latest_data[symbol]['ts']:
                            latest_data[symbol] = {'ts': ts, 'last': float(data['data']['la'])}
                    await asyncio.sleep(0.01)
                # Convert to DataFrame
                self.tickerdf = pd.DataFrame.from_dict(latest_data, orient="index")
                if not self.tickerdf.empty:
                    self.tickerdf["tickerObj"] = self.tickerdf.index.map(self.tickerObjects.get_tickerDict())
                    self.tuples_list = list(zip(self.tickerdf["tickerObj"],  self.tickerdf["last"].astype(float), self.tickerdf["ts"]))
                    self.tickerObjects.form_candle(self.tuples_list)
                    self.lastTickerDataTime = time.time()
            except Exception as e:
                self.logger.info(f"Function: ProcessTickerData, {e}, {e.args}, {type(e).__name__}")
                self.logger.info(traceback.print_exc())
            await asyncio.sleep(0.01)
        self.logger.info(f"ProcessTickerData: exitied out of the loop, exiting app")
        os._exit(1)  # Exit the program 
            

    #websocket data to update bid and ask
    async def StoreDepthData(self, message):
        if self.settings.USE_PUBLIC_WEBSOCKET and message:
            await self.depth_que.put(message)
 
    # Function to process the bid, ask
    async def ProcessDepthData(self):
        while True:
            try:
                latest_data = {}
                reversed_items = await self.drain_queue(self.depth_que)
                while reversed_items:
                    message = reversed_items.popleft()
                    data = json.loads(message)
                    if data.get('symbol') and data.get('ch') == 'depth_book1':
                        symbol = data["symbol"]
                        ts = data["ts"]
                        if symbol not in latest_data or ts > latest_data[symbol]['ts']:
                            latest_data[symbol] = {'ts': ts, 'bid': float(data['data']['b'][0][0]), 'ask': float(data['data']['a'][0][0])}
                    await asyncio.sleep(0.01)
                # Convert to DataFrame
                self.depthdf = pd.DataFrame.from_dict(latest_data, orient="index")
                if not self.depthdf.empty:
                    self.depthdf["tickerObj"] = self.depthdf.index.map(self.tickerObjects.get_tickerDict())
                    self.depthdf.apply(self.apply_depth_data2, axis=1)
            except Exception as e:
                self.logger.info(f"Function: ProcessTickerData, {e}, {e.args}, {type(e).__name__}")
                self.logger.info(traceback.print_exc())
            await asyncio.sleep(0.01)
        self.logger.info(f"ProcessDepthData: exitied out of the loop, exiting app")
        os._exit(1)  # Exit the program 

    def apply_depth_data2(self, row):
        row["tickerObj"].set_ask(row["ask"])
        row["tickerObj"].set_bid(row["bid"])
        return row
        
    # non websocket method
    # this is called to update last price, as the websocket is lagging
    # this is only called for the tickers in the pendingpositions
    # and for first few records in the signaldf    
    async def apply_last_data(self, symbols):
        start=time.time()
        try:
            # Get the current time and set the seconds and microseconds to zero
            current_time = datetime.now()
            current_minute = current_time.replace(second=0, microsecond=0)
            ts = int(current_minute.timestamp())*1000
            
            data= await self.bitunixApi.GetTickerslastPrice(symbols)
            if data is None:
                return
            tickerdf = pd.DataFrame(data, columns=["symbol", "markPrice", "lastPrice", "open", "last", "quoteVol", "baseVol", "high", "low"])
            tickerdf['ts']=ts
            tickerdf["tickerObj"] = tickerdf["symbol"].map(self.tickerObjects.get_tickerDict())
            tuples_list = list(zip(tickerdf["tickerObj"],  tickerdf["last"].astype(float), tickerdf["ts"]))
            self.tickerObjects.form_candle(tuples_list)
            del data, tickerdf, tuples_list
            gc.collect()
        except Exception as e:
            self.logger.info(e)
        if self.settings.VERBOSE_LOGGING:
            self.logger.info(f"apply_last_data: elapsed time {time.time()-start}")
        
    def _pace_depth(self):
        # Depth is 10 requests per second per IP, and several threads call it.
        with self._depth_lock:
            wait = self._next_depth_at - time.time()
            if wait > 0:
                time.sleep(wait)
            self._next_depth_at = time.time() + 0.12

    async def refresh_watchlist_depth(self):
        for symbol in list(self.tickerObjects.symbols()):
            await self.apply_depth_data(symbol)
            self.lastTickerDataTime = time.time()

    async def apply_depth_data(self, ticker):
        self._pace_depth()
        ddata = await self.bitunixApi.GetDepthData(ticker,"1")
        if ddata is not None and ddata.get('bids') and ddata.get('asks'):
            bid = float(ddata['bids'][0][0])
            ask = float(ddata['asks'][0][0])
            tickerObj=self.tickerObjects.get(ticker)
            if tickerObj:
                tickerObj.set_bid(bid)
                tickerObj.set_ask(ask)
                del tickerObj
                gc.collect()
        del ddata
        gc.collect()
                
    ###########################################################################################################
    async def checkTickerAndAutotradeStatus(self):
        while True:
            now = time.time()
            stale = []
            if self.lastAutoTradeTime + 300 < now:
                stale.append("AutoTradeProcess")
            if self.lastTickerDataTime + 300 < now:
                stale.append("GetTickerData")
            if stale:
                self.notifications.add_notification(
                    f"{' and '.join(stale)} has not finished a cycle in 5 minutes"
                )
            await asyncio.sleep(300)

    async def GetportfolioData(self):
        start=time.time()
        try:
            self.portfolioData = await self.bitunixApi.GetportfolioData()
            if self.portfolioData:
                self.portfoliodf=pd.DataFrame(self.portfolioData,index=[0])[["marginCoin","available","margin","crossUnrealizedPNL"]]
                self.portfoliodf = self.portfoliodf.astype({"marginCoin": str, "available": float, "margin": float, "crossUnrealizedPNL": float})
                self.portfoliodf['Value'] =round(self.portfoliodf['margin'] + self.portfoliodf['crossUnrealizedPNL'] + self.portfoliodf['available'],2)
                self.journal.record_equity(
                    self.portfoliodf["available"].iloc[0],
                    self.portfoliodf["margin"].iloc[0],
                    self.portfoliodf["crossUnrealizedPNL"].iloc[0],
                    self.portfoliodf["Value"].iloc[0],
                )
            else:
                self.portfolioData = pd.DataFrame()
            self.portfoliodfStyle= self.portfoliodfrenderer.render_html(self.portfoliodf[["Value", "marginCoin", "available", "margin", "crossUnrealizedPNL"]])
            
        except Exception as e:
            self.logger.info(f"Function: GetportfolioData, {e}, {e.args}, {type(e).__name__}")
        if self.settings.VERBOSE_LOGGING:
            self.logger.info(f"GetportfolioData: elapsed time {time.time()-start}")
                
    async def GetPendingPositionData(self):
        start=time.time()
        try:
            self.pendingPositions = await self.bitunixApi.GetPendingPositionData()
            if self.pendingPositions:
                self.positiondf = pd.DataFrame(self.pendingPositions, columns=["positionId", "symbol", "side", "unrealizedPNL", "realizedPNL",  "qty", "ctime", "avgOpenPrice"])
                self.positiondf['bid']=0.0
                self.positiondf['bidcolor']=""
                self.positiondf['last']=0.0
                self.positiondf['lastcolor']=""
                self.positiondf['ask']=0.0
                self.positiondf['askcolor']=""
                self.positiondf['ctime']=pd.to_datetime(self.positiondf['ctime'].astype(float), unit='ms').dt.tz_localize('UTC').dt.tz_convert(cst).dt.strftime('%Y-%m-%d %H:%M:%S')

                try:
                    self.positiondf = self.positiondf.assign(
                        bid=self.positiondf['symbol'].map(lambda sym: self.tickerObjects.get(sym).get_bid()),
                        bidcolor=self.positiondf['symbol'].map(lambda sym: self.tickerObjects.get(sym).bidcolor),
                        last=self.positiondf['symbol'].map(lambda sym: self.tickerObjects.get(sym).get_last()),
                        lastcolor=self.positiondf['symbol'].map(lambda sym: self.tickerObjects.get(sym).lastcolor),
                        ask=self.positiondf['symbol'].map(lambda sym: self.tickerObjects.get(sym).get_ask()),
                        askcolor=self.positiondf['symbol'].map(lambda sym: self.tickerObjects.get(sym).askcolor)
                    )
                except Exception as e:
                    pass

                self.positiondf['markprice']= (self.positiondf['bid'] + self.positiondf['ask']) / 2
                self.positiondf = self.positiondf.astype({"unrealizedPNL": float, "realizedPNL": float, "markprice": float, "last": float, "qty": float, "avgOpenPrice": float})
                self.positiondf['ROI'] = self.positiondf.apply(
                    lambda row: round((row['unrealizedPNL'] / (row['qty'] * row['avgOpenPrice'])) * 100 * self.settings.LEVERAGE,2), 
                    axis=1
                )
                #print(self.positiondf[['symbol', 'ROI','markprice', 'qty', 'avgOpenPrice']].head())
                
                self.positiondf['charts'] = self.positiondf.apply(self.add_charts_button, axis=1)
                self.positiondf['bitunix'] = self.positiondf.apply(self.add_bitunix_button, axis=1)
                self.positiondf['action'] = self.positiondf.apply(self.add_close_button, axis=1)
                self.positiondf['add'] = self.positiondf.apply(self.add_add_button, axis=1)
                self.positiondf['reduce'] = self.positiondf.apply(self.add_reduce_button, axis=1)

                self.positiondf['bid'] = self.positiondf['bid'].astype('float64')
                self.positiondf['last'] = self.positiondf['last'].astype('float64')
                self.positiondf['ask'] = self.positiondf['ask'].astype('float64')
            else:
                self.positiondf = pd.DataFrame()
                self.positiondfStyle= self.positiondfrenderer.render_html(self.positiondf)    

            #if not self.settings.USE_PUBLIC_WEBSOCKET:                    
            #get bid las ask using api for the symbols in pending psotion
            if not self.positiondf.empty:
                if not self.settings.USE_PUBLIC_WEBSOCKET:
                    await asyncio.create_task(self.apply_last_data(','.join(self.positiondf['symbol'].astype(str).tolist())))                                                
                    await asyncio.gather(
                        *[
                            asyncio.create_task(self.apply_depth_data(row['symbol']))
                            for index, row in self.positiondf.iterrows()
                        ]
                    )                
                    

        except Exception as e:
            self.logger.info(f"Function: GetPendingPositionData, {e}, {e.args}, {type(e).__name__}")
        if self.settings.VERBOSE_LOGGING:
            self.logger.info(f"GetPendingPositionData: elapsed time {time.time()-start}")

    async def GetPendingOrderData(self):
        start=time.time()
        try:
            self.pendingOrders = await self.bitunixApi.GetPendingOrderData()
            if self.pendingOrders and 'orderList' in self.pendingOrders:
                self.orderdf = pd.DataFrame(self.pendingOrders['orderList'], columns=["orderId", "symbol", "qty", "side", "price", "ctime", "status", "reduceOnly"])
                self.orderdf['rtime']=pd.to_datetime(self.orderdf['ctime'].astype(float), unit='ms').dt.tz_localize('UTC').dt.tz_convert(cst).dt.strftime('%Y-%m-%d %H:%M:%S')
                self.orderdf['charts'] = self.orderdf.apply(self.add_charts_button, axis=1)
                self.orderdf['bitunix'] = self.orderdf.apply(self.add_bitunix_button, axis=1)
                self.orderdf['action'] = self.orderdf.apply(self.add_order_close_button, axis=1)
            else:
                self.orderdf = pd.DataFrame()
            self.orderdfStyle= self.orderdfrenderer.render_html(self.orderdf)                
            
        except Exception as e:
            self.logger.info(f"Function: GetPendingOrderData, {e}, {e.args}, {type(e).__name__}")
        if self.settings.VERBOSE_LOGGING:
            self.logger.info(f"GetPendingOrderData: elapsed time {time.time()-start}")

    def _study_snapshot(self, symbol, period):
        names = (
            "trend", "cb", "barcolor", "ema_open", "ema_close", "macd", "bbm",
            "rsi", "bos", "trendline", "adx", "candle_trend",
        )
        frame = self.signaldf_full
        if frame is None or getattr(frame, "empty", True) or symbol not in getattr(frame, "index", []):
            return {}
        record = frame.loc[symbol]
        if isinstance(record, pd.DataFrame):
            record = record.iloc[0]
        snapshot = {}
        for name in names:
            key = f"{period}_{name}"
            try:
                value = record.get(key) if hasattr(record, "get") else record[key]
            except (KeyError, TypeError, ValueError):
                continue
            if hasattr(value, "item"):
                try:
                    value = value.item()
                except Exception:
                    pass
            if value is None:
                continue
            if isinstance(value, float) and value != value:
                continue
            snapshot[name] = value
        return snapshot

    def _store_exchange_page(self, payload, list_name):
        rows = payload.get(list_name) if isinstance(payload, dict) else None
        if not rows:
            return
        try:
            if list_name == "positionList":
                self.journal.upsert_positions(rows)
            elif list_name == "tradeList":
                self.journal.upsert_fills(rows)
        except Exception as error:
            self.logger.info(f"Function: _store_exchange_page, {error}")

    async def _backfill_exchange_history(self):
        skip = 0
        try:
            while skip <= 1000:
                positions = await self.bitunixApi.GetPositionHistoryData({"limit": 100, "skip": skip})
                if not isinstance(positions, dict):
                    return
                rows = positions.get("positionList") or []
                if not rows:
                    break
                self.journal.upsert_positions(rows)
                if len(rows) < 100:
                    break
                skip += 100
                await asyncio.sleep(0.12)
            self._history_backfill_done = True
            self.profit = self.journal.total_realized_pnl()
        except Exception as error:
            self.logger.info(f"Function: _backfill_exchange_history, {error}")

    async def GetPositionHistoryData(self):
        start=time.time()
        try:
            self.positionHistoryData = await self.bitunixApi.GetPositionHistoryData()
            if not self._history_backfill_done:
                await self._backfill_exchange_history()
            self._store_exchange_page(self.positionHistoryData, "positionList")
            self.profit = self.journal.total_realized_pnl()
            if self.positionHistoryData and 'positionList' in self.positionHistoryData:
                self.positionHistorydf = pd.DataFrame(self.positionHistoryData['positionList'], columns=["symbol", "side","realizedPNL", "ctime", "mtime","maxQty", "closePrice","fee", "funding"])
                self.positionHistorydf = self.positionHistorydf.astype({"realizedPNL": float, "maxQty": float, "closePrice": float})
                self.positionHistorydf['ROI'] = self.positionHistorydf.apply(
                    lambda row: round((row['realizedPNL'] / (row['maxQty'] * row['closePrice'])) * 100 * self.settings.LEVERAGE,2), 
                    axis=1
                )
                
                self.positionHistorydf['ctime'] = pd.to_datetime(self.positionHistorydf['ctime'].astype(float), unit='ms').dt.tz_localize('UTC').dt.tz_convert(cst).dt.strftime('%Y-%m-%d %H:%M:%S')
                self.positionHistorydf['mtime'] = pd.to_datetime(self.positionHistorydf['mtime'].astype(float), unit='ms').dt.tz_localize('UTC').dt.tz_convert(cst).dt.strftime('%Y-%m-%d %H:%M:%S')
                self.positionHistorydf['charts'] = self.positionHistorydf.apply(self.add_charts_button, axis=1)
                self.positionHistorydf['bitunix'] = self.positionHistorydf.apply(self.add_bitunix_button, axis=1)

            else:
                self.positionHistorydf = pd.DataFrame()
            self.positionHistorydfStyle= self.positionHistorydfrenderer.render_html(self.positionHistorydf[["symbol", "side", "ROI", "realizedPNL", "ctime", "mtime", "maxQty", "closePrice", "fee", "funding",  "charts", "bitunix"]])
            
        except Exception as e:
            self.logger.info(f"Function: GetPositionHistoryData, {e}, {e.args}, {type(e).__name__}")
        if self.settings.VERBOSE_LOGGING:
            self.logger.info(f"GetPositionHistoryData: elapsed time {time.time()-start}")

    async def GetTradeHistoryData(self):
        start=time.time()
        try:
            self.tradeHistoryData = await self.bitunixApi.GetTradeHistoryData()
            self._store_exchange_page(self.tradeHistoryData, "tradeList")
            if self.tradeHistoryData and 'tradeList' in self.tradeHistoryData:
                self.tradesdf = pd.DataFrame(self.tradeHistoryData['tradeList'], columns=["symbol", "ctime", "qty", "side", "price","realizedPNL","reduceOnly"])
                self.tradesdf['rtime'] = pd.to_datetime(self.tradesdf['ctime'].astype(float), unit='ms').dt.tz_localize('UTC').dt.tz_convert(cst).dt.strftime('%Y-%m-%d %H:%M:%S')
                grouped_trades = self.tradesdf.groupby("symbol")
                for symbol, tickerObj in self.tickerObjects.get_tickerDict().items():
                    if symbol in grouped_trades.groups:
                        # Filter trades for the current symbol and convert them to a list of dicts
                        tickerObj.trades = grouped_trades.get_group(symbol).to_dict("records")                
        except Exception as e:
            self.logger.info(f"Function: GetTradeHistoryData, {e}, {e.args}, {type(e).__name__}")
        if self.settings.VERBOSE_LOGGING:
            self.logger.info(f"GetTradeHistoryData: elapsed time {time.time()-start}")

        
    ###########################################################################################################

    async def BuySellList(self, period):
        try:
            #ticker in positions and orders
            inuse1=[] 
            inuse2=[] 
            if not self.positiondf.empty: 
                inuse1 = self.positiondf['symbol'].to_list() 
            if not self.orderdf.empty: 
                inuse2 = self.orderdf['symbol'].to_list() 
            inuseTickers = set(inuse1 + inuse2)
            
            # Extract buy/sell ticker data
            self.signaldf_filtered = pd.DataFrame();
            self.tickerObjects.getCurrentData(period)

            self.signaldf_full = self.tickerObjects.signaldf_full

            if self.signaldf_full.empty:
                self.positiondf2 = self.positiondf
                self.positiondfStyle= self.positiondfrenderer.render_html(self.positiondf)           
                return

            self.signaldf_full['charts'] = self.signaldf_full.apply(self.add_charts_button, axis=1)
            self.signaldf_full['bitunix'] = self.signaldf_full.apply(self.add_bitunix_button, axis=1)
            

            self.allsignaldfStyle= self.allsignaldfrenderer.render_html(self.signaldf_full)
            
            self.signaldf_filtered = self.tickerObjects.signaldf_filtered

            if not self.positiondf.empty and not self.signaldf_full.empty:
                    columns=['symbol', f"{period}_trend", f"{period}_cb", f"{period}_barcolor", 
                             f"{period}_bos",
                             f"{period}_ema_open", f"{period}_ema_close", f"{period}_macd", f"{period}_bbm", f"{period}_rsi", f"{period}_trendline", f"{period}_candle_trend", f"{period}_open", f"{period}_close", f"{period}_high", f"{period}_low"]
                    columns2=["qty", "side", "unrealizedPNL", "realizedPNL", "ctime", "avgOpenPrice", "bid", "bidcolor", "last", "lastcolor", "ask", "askcolor", "charts", "bitunix", "action", "add", "reduce"]
                    if set(columns).issubset(self.signaldf_full.columns) and set(columns2).issubset(self.positiondf.columns):
                        columnOrder= ['symbol', "side", "ROI", "unrealizedPNL", "realizedPNL", f"{period}_trend", f"{period}_cb", f"{period}_barcolor", 
                                      f"{period}_bos",
                                      f"{period}_ema_open", f"{period}_ema_close", f"{period}_macd", f"{period}_bbm", f"{period}_rsi", f"{period}_trendline", f"{period}_adx", f"{period}_candle_trend", f"{period}_open", f"{period}_close", f"{period}_high", f"{period}_low", "qty", "ctime", "avgOpenPrice", "bid", "bidcolor", "last", "lastcolor", "ask", "askcolor", "charts", "bitunix", "action", "add", "reduce"]                
                        self.positiondf2 = pd.merge(self.positiondf, self.signaldf_full[["symbol", f"{period}_open", f"{period}_close", f"{period}_high", f"{period}_low", 
                                f"{period}_bos",                                                               
                                f"{period}_ema_open", f"{period}_ema_close", f"{period}_macd", f"{period}_bbm", f"{period}_rsi", f"{period}_trendline", f"{period}_adx", f"{period}_candle_trend",
                                f"{period}_trend",f"{period}_cb", f"{period}_barcolor"]], left_on="symbol", right_index=True, how="left")[columnOrder]    
                        self.positiondfStyle= self.positiondfrenderer.render_html(self.positiondf2)
            else:
                self.positiondf2 = pd.DataFrame()
                
            if not self.signaldf_filtered.empty:
                #remove those that are in positon and orders
                self.signaldf_filtered = self.signaldf_filtered[~(self.signaldf_filtered['symbol'].isin(inuseTickers))]

                if not self.signaldf_filtered.empty:
                    # Assign to self.signaldf for HTML rendering
                    self.signaldf = self.signaldf_filtered[[
                        "symbol", f"{period}_trend",f"{period}_cb", f"{period}_barcolor",
                        f"{period}_bos",
                        f"{period}_ema_open", f"{period}_ema_close", f"{period}_macd", f"{period}_bbm", f"{period}_rsi", f"{period}_trendline",f"{period}_adx",f"{period}_candle_trend",
                        'lastcolor', 'bidcolor', 'askcolor', 'bid', 'last', 'ask',
                        f"{period}_open", f"{period}_close", f"{period}_high", f"{period}_low", 
                    ]].sort_values(by=[f'{period}_cb'], ascending=[False])

                    # Add buttons
                    self.signaldf['charts'] = self.signaldf.apply(self.add_charts_button, axis=1)
                    self.signaldf['bitunix'] = self.signaldf.apply(self.add_bitunix_button, axis=1)
                    self.signaldf['buy'] = self.signaldf.apply(self.add_buy_button, axis=1)
                    self.signaldf['sell'] = self.signaldf.apply(self.add_sell_button, axis=1)
                else:
                    self.signaldf = pd.DataFrame()

                #if not self.settings.USE_PUBLIC_WEBSOCKET:                    
                #get bid las ask using api for max_auto_trades rows
                if not self.signaldf.empty:
                    m = min(self.signaldf.shape[0], int(self.settings.MAX_AUTO_TRADES))
                    if not self.settings.USE_PUBLIC_WEBSOCKET:
                        await asyncio.create_task(self.apply_last_data(','.join(self.signaldf['symbol'][:m].astype(str).tolist())))                 
                        await asyncio.gather(
                            *[
                                asyncio.create_task(self.apply_depth_data(row['symbol']))
                                for index, row in self.signaldf[:m].iterrows()
                            ]
                        )  
            else:
                self.signaldf = pd.DataFrame()                        
            self.signaldfStyle= self.signaldfrenderer.render_html(self.signaldf)

        except Exception as e:
            self.logger.info(f"Function: BuySellList, {e}, {e.args}, {type(e).__name__}")
            self.logger.info(traceback.print_exc())
        del inuse1, inuse2, inuseTickers
        gc.collect()

    async def get_duration_minutes_unixtime(self, trade_time_unix):
        """Calculates the duration in minutes from trade time to current time."""
        current_time_unix = int(time.time())  # Get current Unix timestamp
        duration_seconds = current_time_unix - trade_time_unix
        duration_minutes = duration_seconds / 60  # Convert seconds to minutes
        return round(duration_minutes)
    
    async def get_duration(self,datetime_str):
        """Calculates the duration in minutes from a given datetime string to the current time."""
        # Convert input datetime string to a datetime object
        trade_time = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S")
        
        # Convert datetime object to Unix timestamp
        trade_time_unix = int(trade_time.timestamp())

        # Get current Unix timestamp
        current_time_unix = int(time.time())

        # Calculate duration in minutes
        duration_minutes = (current_time_unix - trade_time_unix) / 60
        
        return round(duration_minutes)

    

    async def calculate_candles(self, timeframe, duration_minutes):
        
        timeframe_dict = {"5m": 5, "15m": 15, "1h": 60, "1d": 1440}  # 1 day = 1440 minutes
        if timeframe in timeframe_dict:
            return duration_minutes // timeframe_dict[timeframe]
        else:
            return np.nan

    def _reentry_path(self):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "symbol_reentry.json")

    def _read_reentry(self):
        try:
            with open(self._reentry_path(), "r", encoding="utf-8") as handle:
                state = json.load(handle)
            if isinstance(state, dict):
                return {symbol: int(float(until)) for symbol, until in state.items()}
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass
        return {}

    def _write_reentry(self):
        path = self._reentry_path()
        temporary = path + ".tmp"
        now_ms = time.time() * 1000
        kept = {symbol: until for symbol, until in self._reentry_until.items() if until > now_ms}
        self._reentry_until = kept
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(kept, handle)
        os.replace(temporary, path)

    def note_symbol_closed(self, symbol, closed_at_ms=None):
        """Hold a symbol until the next candle of DELAY_IN_MINUTES closes."""
        minutes = int(self.settings.DELAY_IN_MINUTES_FOR_SAME_TICKER_TRADES or 0)
        if minutes <= 0 or not symbol:
            return
        closed_at_ms = int(closed_at_ms or time.time() * 1000)
        interval = minutes * 60 * 1000
        allow_after = (closed_at_ms // interval + 1) * interval
        if allow_after <= time.time() * 1000:
            return
        with self._reentry_lock:
            if allow_after <= int(self._reentry_until.get(symbol) or 0):
                return
            self._reentry_until[symbol] = allow_after
            self._write_reentry()

    def reentry_blocked_until(self, symbol, history_ms=None):
        if history_ms:
            self.note_symbol_closed(symbol, history_ms)
        allow_after = int(self._reentry_until.get(symbol) or 0)
        if allow_after > time.time() * 1000:
            return allow_after
        return 0

    def _daily_loss_state_path(self):
        return os.path.join(os.path.dirname(os.path.abspath(__file__)), "daily_loss_halt.json")

    def _read_loss_state(self):
        try:
            with open(self._daily_loss_state_path(), "r", encoding="utf-8") as handle:
                state = json.load(handle)
            if isinstance(state, dict):
                return state
        except (OSError, json.JSONDecodeError):
            pass
        return {}

    def _write_loss_state(self, state):
        path = self._daily_loss_state_path()
        temporary = path + ".tmp"
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(state, handle)
        os.replace(temporary, path)

    def _realized_pnl_since(self, measure_from_ms):
        data = self.positionHistoryData if isinstance(self.positionHistoryData, dict) else {}
        realized = 0.0
        for row in data.get("positionList") or []:
            try:
                closed_at = float(row.get("mtime") or 0)
                pnl = float(row.get("realizedPNL") or 0)
            except (TypeError, ValueError):
                continue
            if closed_at >= measure_from_ms:
                realized += pnl
        return realized

    def _wallet_usdt(self):
        """Cash in the futures wallet: available, margin, and open profit or loss.

        This is the USDT balance Bitunix reports. Leverage changes position size.
        It is not applied again on top of this balance.
        """
        portfolio = self.portfoliodf
        if portfolio is None or getattr(portfolio, "empty", True):
            return None
        available = float(portfolio["available"].iloc[0])
        margin = float(portfolio["margin"].iloc[0])
        unrealized = float(portfolio["crossUnrealizedPNL"].iloc[0])
        return available + margin + unrealized

    def opens_halted_by_daily_loss(self):
        """Block new opens after the wallet falls by DAILY_LOSS_LIMIT_PERCENT.

        The starting point is the actual USDT wallet when this window began.
        The loss is how far that wallet has fallen since then. Closes keep
        running. A limit of 0 turns this off.
        """
        limit = float(self.settings.DAILY_LOSS_LIMIT_PERCENT or 0)
        hours = float(self.settings.DAILY_LOSS_HALT_HOURS or 0)
        if limit <= 0 or hours <= 0:
            return False
        now = time.time()
        with self._loss_state_lock:
            state = self._read_loss_state()
            halt_until = float(state.get("halt_until") or 0)
            if halt_until > now:
                resume = datetime.fromtimestamp(halt_until, cst).strftime("%Y-%m-%d %H:%M:%S")
                self.open_block_reason = f"New opens paused until {resume} by the daily loss limit."
                if self._halt_notice_until != halt_until:
                    self.notifications.add_notification(self.open_block_reason)
                    self._halt_notice_until = halt_until
                return True
            if self.open_block_reason.startswith("New opens paused"):
                self.open_block_reason = ""

            wallet = self._wallet_usdt()
            if wallet is None or wallet <= 0:
                return False
            start_equity = float(state.get("start_equity") or 0)
            if start_equity <= 0:
                state["start_equity"] = wallet
                state["measure_from_ms"] = now * 1000
                state["halt_until"] = 0
                self._write_loss_state(state)
                return False
            loss = start_equity - wallet
            if loss < (limit / 100.0) * start_equity:
                return False

            halt_until = now + hours * 3600
            self._write_loss_state({
                "halt_until": halt_until,
                "measure_from_ms": halt_until * 1000,
                "loss": loss,
                "start_equity": 0,
            })
        loss_pct = loss / start_equity * 100
        resume = datetime.fromtimestamp(halt_until, cst).strftime("%Y-%m-%d %H:%M:%S")
        self.notifications.add_notification(
            f"Wallet down ${loss:.2f} ({loss_pct:.2f}%) from ${start_equity:.2f}. New opens paused until {resume}."
        )
        self.logger.info(
            f"Daily loss halt: wallet {wallet:.4f} is down {loss:.4f} from {start_equity:.4f} ({loss_pct:.2f}%). Opens paused until {resume}."
        )
        return True

    def _level_value(self, symbol, key):
        frame = self.signaldf_full
        if frame is None or getattr(frame, "empty", True) or symbol not in frame.index:
            return 0.0
        record = frame.loc[symbol]
        if isinstance(record, pd.DataFrame):
            record = record.iloc[0]
        try:
            value = float(record.get(key, 0) if hasattr(record, "get") else record[key])
        except (KeyError, TypeError, ValueError):
            return 0.0
        if value != value or value <= 0:
            return 0.0
        return value

    def plan_risk_reward(self, side, price, symbol, period):
        """Score reward distance against risk distance for one entry.

        The stop stays at the configured loss. The target starts at the
        configured profit and is pulled in when a band or structure level sits
        closer. A result below MIN_RISK_REWARD is rejected.
        """
        price = float(price)
        rejected = {"ok": False, "ratio": 0.0, "target": 0.0, "stop": 0.0}
        if price <= 0 or side not in ("BUY", "SELL"):
            return rejected
        leverage = float(self.settings.LEVERAGE) or 1.0
        if float(self.settings.PROFIT_PERCENTAGE or 0) > 0:
            reward_pct = float(self.settings.PROFIT_PERCENTAGE) / 100.0 / leverage
        else:
            reward_pct = float(self.settings.PROFIT_AMOUNT or 0)
        if float(self.settings.LOSS_PERCENTAGE or 0) > 0:
            risk_pct = float(self.settings.LOSS_PERCENTAGE) / 100.0 / leverage
        else:
            risk_pct = float(self.settings.LOSS_AMOUNT or 0)
        minimum = float(self.settings.MIN_RISK_REWARD or 0)
        if reward_pct <= 0 or not 0 < risk_pct < 1:
            return rejected
        supports = [
            self._level_value(symbol, f"{period}_{name}")
            for name in ("trend_support", "bos_support", "bb_lower")
        ]
        resistances = [
            self._level_value(symbol, f"{period}_{name}")
            for name in ("trend_resistance", "bos_resistance", "bb_upper")
        ]
        if side == "BUY":
            target = price * (1 + reward_pct)
            stop = price * (1 - risk_pct)
            ceilings = [level for level in resistances if price < level < target]
            if ceilings:
                target = min(ceilings)
            reward = target - price
            risk = price - stop
        else:
            target = price * (1 - reward_pct)
            stop = price * (1 + risk_pct)
            floors = [level for level in supports if target < level < price]
            if floors:
                target = max(floors)
            reward = price - target
            risk = stop - price
        if reward <= 0 or risk <= 0:
            rejected["reason"] = "Target is not above the stop"
            return rejected
        ratio = reward / risk
        pulled = (side == "BUY" and bool(ceilings)) or (side == "SELL" and bool(floors))
        reason = f"R:R {ratio:.2f} below {minimum:g}" if minimum > 0 and ratio + 1e-9 < minimum else ""
        if pulled and reason:
            reason += f", target pulled in to {_price_text(target)} from the percentage target"
        rejected.update({"ratio": ratio, "target": target, "stop": stop, "reason": reason})
        if reason:
            return rejected
        rejected["ok"] = True
        return rejected

    def _publish_skips(self, notes=None):
        notes = list(notes if notes is not None else self._skip_notes)
        if not notes:
            if not self.open_block_reason.startswith("New opens paused"):
                self.open_block_reason = ""
            return
        summary = "; ".join(notes[:6])
        if len(notes) > 6:
            summary += f"; and {len(notes) - 6} more"
        self.open_block_reason = summary
        if summary != self._last_skip_notice:
            self.notifications.add_notification(summary)
            self._last_skip_notice = summary

    async def AutoTradeProcess(self):
        if self.settings.VERBOSE_LOGGING:
            self.logger.info(f"AutoTradeProcess started")
        start=time.time()
        
        period = self.settings.OPTION_MOVING_AVERAGE
        try:
            
            #calulate current data at selected period, this create signaldf
            await self.BuySellList(period)
            
            if not self.settings.AUTOTRADE:
                return
            ##############################################################################################################################
            # open long or short postition
            ##############################################################################################################################
            if self.opens_halted_by_daily_loss():
                count = int(self.settings.MAX_AUTO_TRADES)
            else:
                count=0
            self.pendingPositions = await self.bitunixApi.GetPendingPositionData({})
            if self.pendingPositions:
                count=count+len(self.pendingPositions)
            self.pendingOrders = await self.bitunixApi.GetPendingOrderData({})
            if self.pendingOrders:
                count=count+len(self.pendingOrders['orderList'])

            if count < int(self.settings.MAX_AUTO_TRADES):
                if not self.signaldf.empty:
                    #open position upto a max of max_auto_trades from the signal list
                    self._skip_notes = []
                    df=self.signaldf.copy(deep=False)
                    for index, row in df.iterrows():
                        
                        # Calculate the duration in minutes since the position was opened
                        await asyncio.sleep(5)
                        data = await self.bitunixApi.GetPositionHistoryData({'symbol': row['symbol']})
                        history = data.get('positionList') if isinstance(data, dict) else None
                        if history is None:
                            self.logger.info(f"No position history response for {row['symbol']}, skipping this pass")
                            history_ms = None
                        elif history:
                            history_ms = max(float(item.get("mtime") or 0) for item in history)
                        else:
                            history_ms = None
                        blocked_until = self.reentry_blocked_until(row.symbol, history_ms)
                        if history is None and not blocked_until:
                            continue
                        if not blocked_until:
                            side = "BUY" if row[f'{period}_barcolor'] == self.green and row[f'{period}_trend'] == "BUY"  else "SELL" if row[f'{period}_barcolor'] == self.red and row[f'{period}_trend'] == "SELL" else ""
                            if side == "":
                                self._skip_notes.append(f"{row.symbol} signal does not match the candle color")
                            else:
                                select = True
                                self.pendingPositions = await self.bitunixApi.GetPendingPositionData({'symbol': row.symbol})
                                if self.pendingPositions and len(self.pendingPositions) == 1:
                                    select = False
                                self.pendingOrders = await self.bitunixApi.GetPendingOrderData({'symbol': row.symbol})
                                if self.pendingOrders and len(self.pendingOrders['orderList']) == 1:
                                    select = False
                                if not select:
                                    self._skip_notes.append(f"{row.symbol} already has a position or working order")
                                if select:
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)

                                    price = (bid if side == "BUY" else ask if side == "SELL" else last) if bid<=last<=ask else last

                                    plan = self.plan_risk_reward(side, price, row.symbol, period)
                                    if not plan["ok"]:
                                        reason = plan.get("reason") or (
                                            f"R:R {plan['ratio']:.2f} below {float(self.settings.MIN_RISK_REWARD):g}"
                                        )
                                        self._skip_notes.append(
                                            f"{row.symbol} {reason}. entry {_price_text(price)} target {_price_text(plan['target'])} stop {_price_text(plan['stop'])}"
                                        )
                                        self.logger.info(f"Skipping {row.symbol} {reason}. entry {price} target {plan['target']} stop {plan['stop']}")
                                        self.journal.record_decision(
                                            symbol=row.symbol,
                                            side=side,
                                            action="skip_risk_reward",
                                            reason=reason,
                                            entry_price=price,
                                            stop_price=plan["stop"],
                                            target_price=plan["target"],
                                            risk_reward=plan["ratio"],
                                            studies=self._study_snapshot(row.symbol, period),
                                        )
                                        continue
                                    tp_price = plan["target"]
                                    lp_price = plan["stop"]

                                    balance = float(self.portfoliodf["available"].iloc[0]) + float(self.portfoliodf["crossUnrealizedPNL"].iloc[0])
                                    qty= str(max(balance * (float(self.settings.ORDER_AMOUNT_PERCENTAGE) / 100 ) / price * int(self.settings.LEVERAGE),mtv))

                                    self.notifications.add_notification(
                                        f'{colors.YELLOW} Opening {"long" if side=="BUY" else "short"} position for {row.symbol} with {qty} qty @ {price}  R:R {plan["ratio"]:.2f})'
                                    )
                                    datajs = None
                                    if tp_price == 0 and lp_price == 0:
                                        datajs = await self.bitunixApi.PlaceOrder(row.symbol, qty, price, side)
                                    elif tp_price == 0:
                                        datajs = await self.bitunixApi.PlaceOrder(row.symbol, qty, price, side, stopLossPrice=lp_price)
                                    elif lp_price == 0:
                                        datajs = await self.bitunixApi.PlaceOrder(row.symbol, qty, price, side, takeProfitPrice=tp_price)
                                    else:   
                                        datajs = await self.bitunixApi.PlaceOrder(row.symbol, qty, price, side, takeProfitPrice=tp_price, stopLossPrice=lp_price)
                                    self.journal.record_order(
                                        symbol=row.symbol,
                                        side=side,
                                        trade_side="OPEN",
                                        qty=qty,
                                        entry_price=price,
                                        stop_price=lp_price,
                                        target_price=tp_price,
                                        risk_reward=plan["ratio"],
                                        period=period,
                                        studies=self._study_snapshot(row.symbol, period),
                                        response=datajs,
                                    )
                                    count=count+1
                        else:
                            resume = datetime.fromtimestamp(blocked_until / 1000, cst).strftime("%H:%M")
                            minutes = int(self.settings.DELAY_IN_MINUTES_FOR_SAME_TICKER_TRADES)
                            self._skip_notes.append(
                                f"{row.symbol} waits for the next {minutes}-minute candle close at {resume}"
                            )
                        if count >= int(self.settings.MAX_AUTO_TRADES):
                            break
                        await asyncio.sleep(0)
                    self._publish_skips()
                    del df
                    gc.collect()
                else:
                    self._publish_skips(["No buy or sell signal on this pass"])

            ##############################################################################################################################
            # close long or short postition
            ##############################################################################################################################

            # Close orders that are open for a while
            current_time = time.time() * 1000
            df=self.orderdf.copy(deep=False)
            for index, row in df.iterrows():
                if current_time - int(row.ctime) > 6000000000:
                    self.notifications.add_notification(
                        f'{colors.LBLUE} Canceling order {row.orderId}, {row.symbol} {row.qty} created at {row.rtime} '
                    )
                    datajs = await self.bitunixApi.CancelOrder(row.symbol, row.orderId)
                await asyncio.sleep(0)

            if not self.positiondf.empty:
                df=self.positiondf.copy(deep=False)
                for index, row in df.iterrows():
                    unrealized_pnl = float(row.unrealizedPNL)
                    realized_pnl = float(row.realizedPNL)
                    total_pnl = unrealized_pnl + realized_pnl
                    side=row['side']

                    # Calculate the duration in minutes since the position was opened
                    ctime = row['ctime']
                    duration_minutes = await self.get_duration(ctime)
                    no_of_candles_since_current_open = await self.calculate_candles(period, duration_minutes)
                    
                    requiredCols=[f'{period}_open', f'{period}_close', f'{period}_high', f'{period}_low', f'{period}_ema_open', f"{period}_ema_close", f'{period}_macd', f'{period}_bbm', f'{period}_rsi', f'{period}_trendline', f'{period}_candle_trend', f'{period}_trend', f'{period}_cb', f'{period}_barcolor']    
                    required_cols = set(requiredCols)

                    # Close position that fall the below criteria
                    if not self.signaldf_full.columns.empty and self.signaldf_full['symbol'].isin([row.symbol]).any() and required_cols.issubset(set(self.signaldf_full.columns)):
                        
                        # Check orders
                        select = True
                        self.pendingOrders = await self.bitunixApi.GetPendingOrderData({'symbol': row.symbol})
                        if self.pendingOrders and len(self.pendingOrders['orderList']) == 1:
                            select = False
                            
                        if select and int(self.settings.MAX_AUTO_TRADES)!=0:
                        
                            # Moving average comparison between fast and medium or medium and slow
                            if self.settings.EMA_STUDY and self.settings.EMA_CHECK_ON_CLOSE: 
                                if row.side == 'BUY' and self.signaldf_full.at[row.symbol, f'{period}_ema_close'] == "SELL":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last

                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to MA {period} crossover for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                                if row.side == 'SELL' and self.signaldf_full.at[row.symbol, f'{period}_ema_close'] == "BUY":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to MA {period} crossover for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                            # MACD comparison between MACD and Signal
                            if self.settings.MACD_STUDY and self.settings.MACD_CHECK_ON_CLOSE:
                                if row.side == 'BUY' and self.signaldf_full.at[row.symbol, f'{period}_macd'] == "SELL":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to MACD {period} crossover for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                                if row.side == 'SELL'  and self.signaldf_full.at[row.symbol, f'{period}_macd'] == "BUY":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to MACD {period} crossover for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                            # Bollinger Band comparison between open and BBM
                            if self.settings.BBM_STUDY and self.settings.BBM_CHECK_ON_CLOSE:
                                if row.side == 'BUY' and self.signaldf_full.at[row.symbol, f'{period}_bbm'] == "SELL":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to BBM {period} crossover for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                                if row.side == 'SELL' and self.signaldf_full.at[row.symbol, f'{period}_bbm'] == "BUY":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to BBM {period} crossover for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue
                            
                            # RSI comparison
                            if self.settings.RSI_STUDY and self.settings.RSI_CHECK_ON_CLOSE:
                                if row.side == 'BUY' and self.signaldf_full.at[row.symbol, f'{period}_rsi'] == "SELL":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to RSI {period} crossover for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                                if row.side == 'SELL' and self.signaldf_full.at[row.symbol, f'{period}_rsi'] == "BUY":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to RSI {period} crossover for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                            # BOS
                            if self.settings.BOS_STUDY and self.settings.BOS_CHECK_ON_CLOSE: 
                                if row.side == 'BUY' and self.signaldf_full.at[row.symbol, f'{period}_bos'] == "SELL" and total_pnl < 0:
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last

                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to {period} bos revrsal for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                                if row.side == 'SELL' and self.signaldf_full.at[row.symbol, f'{period}_bos'] == "BUY" and total_pnl <0:
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to {period} bos reversal for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                            # TrendLine
                            if self.settings.TRENDLINE_STUDY and self.settings.TRENDLINE_CHECK_ON_CLOSE: 
                                if row.side == 'BUY' and self.signaldf_full.at[row.symbol, f'{period}_trendline'] == "SELL" and total_pnl < 0:
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last

                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to {period} trendline reversalfor {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                                if row.side == 'SELL' and self.signaldf_full.at[row.symbol, f'{period}_trendline'] == "BUY" and total_pnl < 0:
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to {period} trendline reversal for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                            # Close on weak trend after open
                            if self.settings.ADX_STUDY and self.settings.ADX_CHECK_ON_CLOSE:
                                if row.side == 'BUY' and self.signaldf_full.at[row.symbol, f'{period}_adx'] == "WEAK":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to WEAK ADX for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                                if row.side == 'SELL' and self.signaldf_full.at[row.symbol, f'{period}_adx'] == "WEAK":
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last
                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to WEAK ADX for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                            # candle reversed
                            if self.settings.CANDLE_TREND_STUDY and self.settings.CANDLE_TREND_REVERSAL_CHECK_ON_CLOSE:
                                if row.side == 'BUY' and self.signaldf_full.at[row.symbol, f'{period}_barcolor'] == self.red and self.signaldf_full.at[row.symbol, f'{period}_cb'] > 1 and self.signaldf_full.at[row.symbol, f'{period}_candle_trend'] == "BEARISH" and total_pnl < 0:
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last

                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to bearish candle reversal for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                                if row.side == 'SELL' and self.signaldf_full.at[row.symbol, f'{period}_barcolor'] == self.green and self.signaldf_full.at[row.symbol, f'{period}_cb'] > 1 and self.signaldf_full.at[row.symbol, f'{period}_candle_trend'] == "BULLISH" and total_pnl < 0:
                                    last, bid, ask, mtv = await self.GetTickerBidLastAsk(row.symbol)
                                    price = (ask if row['side'] == "BUY" else bid if row['side'] == "SELL" else last) if bid<=last<=ask else last

                                    self.notifications.add_notification(
                                        f'{colors.CYAN} Closing {"long" if side=="BUY" else "short"} position due to bullish candle reversal for {row.symbol} with {row.qty} qty @ {price})'
                                    )
                                    datajs = await self.bitunixApi.PlaceOrder(
                                        positionId=row.positionId,
                                        ticker=row.symbol,
                                        qty=row.qty,
                                        price=price,
                                        side=row.side,
                                        tradeSide="CLOSE"
                                    )
                                    continue

                    await asyncio.sleep(0)
            
        except Exception as e:
            stack = traceback.extract_stack()
            function_name = stack[-1].name
            self.logger.info(f"Function: {function_name}, {e}, {e.args}, {type(e).__name__}")
            self.logger.info(traceback.format_exc())
        finally:
            self.lastAutoTradeTime = time.time()
                
        if self.settings.VERBOSE_LOGGING:
            self.logger.info(f"AutoTradeProcess: elapsed time {time.time()-start}")
        if self.settings.BENCHMARK:
            self.connection = sqlite3.connect(self.settings.DATABASE) 
            self.cursor = self.connection.cursor()
            self.cursor.execute("INSERT INTO benchmark (process_name, time) VALUES (?, ?)", ("AutoTradeProcess", time.time()-start))
            self.connection.commit()

        del df
        gc.collect()
            
            
    async def GetTickerBidLastAsk(self, symbol):
        tdata = await self.bitunixApi.GetTickerslastPrice(symbol)
        if tdata:
            last=float(tdata[0]['lastPrice'])
        else:
            last=0.0
        ddata = await self.bitunixApi.GetDepthData(symbol,"1")
        if tdata:
            bid = float(ddata['bids'][0][0])
            ask = float(ddata['asks'][0][0])
        else:
            bid=0.0
            ask=0.0 
        pdata = await self.bitunixApi.GetTickersPair(symbol)
        if pdata:
            mtv=float(pdata[0]['minTradeVolume'])
        else:
            mtv=0.0
        tickerObj = self.tickerObjects.get(symbol)
        if tickerObj:
            tickerObj.set_last(last)
            tickerObj.set_bid(bid)
            tickerObj.set_ask(ask)        
            tickerObj.set_mtv(mtv)        
            del tdata, ddata, pdata, tickerObj
            gc.collect()
        return last,bid,ask,mtv

    async def ProcessPrivateData(self, message):
        if message=="":
            return
        try:
            feed = json.loads(message)
            if 'ch' not in feed:
                return

            channel = feed['ch']
            data = feed['data']

            if channel == 'order':
                
                symbol = data['symbol']
                qty = data['qty']
                side = data['side']
                price = data['price']
                event = data['event']
                orderStatus = data['orderStatus']
                self.notifications.add_notification(
                     f'{colors.LBLUE} {orderStatus} {"short" if side=="Sell" else "long"} order for {symbol} with {qty} qty (event: {event})'
                )

            elif channel == 'balance':
                
                self.available = data['available']
                self.margin = data['margin']
                # self.logger.info(feed)

            elif channel == 'position':
                
                ts = int(feed['ts'])
                symbol = data['symbol']
                qty = float(data['qty'])
                side = data['side']
                positionId = data['positionId']
                event = data['event']
                if 'entryValue' in data:
                    entryValue = float(data['entryValue'])
                else:
                    entryValue = 0
                price = entryValue / qty if entryValue != 0 and qty != 0 else 0

                if event == "OPEN":
                    self.notifications.add_notification(
                        f'{colors.PURPLE} Opened {side} position for {symbol} with {qty} qty @ {price}'
                    )

                elif event == "CLOSE":
                    self.note_symbol_closed(symbol, ts)
                    datajs = await self.bitunixApi.GetPositionHistoryData({'positionId': positionId})
                    if datajs and len(datajs['positionList']) == 1:
                        position = datajs['positionList'][0]
                        profit = float(position['realizedPNL'])
                        price = float(position['closePrice'])
                        qty = float(position['maxQty'])
                        self.journal.upsert_positions([position])
                        self.journal.record_close(symbol, positionId, price, profit, qty)
                        self.profit = self.journal.total_realized_pnl()
                        self.notifications.add_notification(
                            f'{colors.GREEN if profit>0 else colors.RED} Closed {side} position for {symbol} with {qty} qty @ {price} and {"profit" if profit>0 else "loss"} of {profit}'
                        )
                    del datajs
                    gc.collect()
                self.tickerObjects.get(symbol).trades.append({'ctime': ts, 'symbol': symbol, 'qty': qty, 'side': side, 'price': price})
            del feed
            gc.collect()
        except Exception as e:
            self.logger.info(f"Function: ProcessPrivateData, {e}, {e.args}, {type(e).__name__}")
            
    def color_cells(val, color):
        return f'background-color: {color}' if val else ''

    def add_charts_button(self, row):
        return f'<button onclick="handleChartsButton(\'{row["symbol"]}\')">charts</button>'

    def add_bitunix_button(self, row):
        return f'<button onclick="handleBitunixButton(\'{row["symbol"]}\')">show</button>'

    def add_buy_button(self, row):
        return f'<button onclick="handleBuyButton(\'{row["symbol"]}\',\'{row["last"]}\')">buy</button>'

    def add_add_button(self, row):
        return f'<button onclick="handleAddButton(\'{row["symbol"]}\',\'{row["last"]}\')">add</button>'

    def add_reduce_button(self, row):
        return f'<button onclick="handleReduceButton(\'{row["symbol"]}\',\'{row["positionId"]}\',\'{row["qty"]}\',\'{row["last"]}\')">reduce</button>'

    def add_sell_button(self, row):
        return f'<button onclick="handleSellButton(\'{row["symbol"]}\',\'{row["last"]}\')">sell</button>'

    def add_close_button(self, row):
        return f'<button onclick="handleCloseButton(\'{row["symbol"]}\',\'{row["positionId"]}\',\'{row["qty"]}\',\'{row["unrealizedPNL"]}\',\'{row["realizedPNL"]}\')">FlashClose</button>'

    def add_order_close_button(self, row):
        return f'<button onclick="handleOrderCloseButton(\'{row["symbol"]}\',\'{row["orderId"]}\')">close</button>'

    async def send_message_to_websocket(self,message):
        async def send_to_all():
            for ws in self.websocket_connections:
                await ws.send_text(message)
        asyncio.run(send_to_all())
        
    async def get_portfolio_tradable_balance(self):
        return float(self.portfoliodf["available"])+float(self.portfoliodf["crossUnrealizedPNL"])   
         
    async def convert_defaultdict_to_dict(self, d):
        if isinstance(d, defaultdict):
            d = {k: self.convert_defaultdict_to_dict(v) for k, v in d.items()}
        return d
    
 
    
