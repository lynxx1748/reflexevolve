# Change Log
## version 3.2.20 ##
Orders use each symbol's `basePrecision` and `quotePrecision` from Bitunix. Quantity is rounded down, and price, take-profit, and stop are rounded to the symbol's price decimals. An open no longer sends `positionId` 0. A quantity that rounds to 0 is not sent.

Public market GETs retry after a dropped connection. Order POSTs are still a single attempt.

Bid and ask are refreshed for every watched symbol, paced under the depth rate limit. The depth sweep updates the heartbeat so a long pass is not treated as a dead process. The five-minute watchdog logs a stall instead of killing the process. An empty position history no longer aborts the auto-trade pass. A ticker with a high or low of 0 is skipped instead of stopping startup.

New opens pause when the USDT wallet falls by `DAILY_LOSS_LIMIT_PERCENT` from the balance at the start of the window. Leverage is not applied, and older closes are not added back into that balance. The pause lasts `DAILY_LOSS_HALT_HOURS`. Closes still run. The wallet baseline is saved in `daily_loss_halt.json`.

Each open is scored as reward distance divided by risk distance. The stop stays at the configured loss. The target starts at the configured profit and is pulled in when a trendline, break-of-structure level, or Bollinger band is closer. An open below `MIN_RISK_REWARD` is skipped. The Opens line on the desk shows why a pass did not trade.

After a symbol closes, including a manual flash close, that symbol waits until the next candle of `DELAY_IN_MINUTES_FOR_SAME_TICKER_TRADES` closes before it can be opened again. The wait is saved in `symbol_reentry.json`.

Closed trades, skip reasons, exchange fills, and wallet snapshots are stored in `config.db`. Total profit on the desk is the sum of stored closed positions and survives a restart. It is shown to the cent.

Indicator work starts with `spawn` and reuses those workers, so Python no longer forks from the running trade threads.

`period_test.py` replays the open rules on past 1-hour candles and ranks period sets on the later bars. It does not place orders or change `config.txt`.

## version 3.2.10 ##
Public last price and depth use the REST API. `USE_PUBLIC_WEBSOCKET` now defaults to False. The public socket, if turned back on, subscribes slowly enough to stay under Bitunix's 5 messages per second limit.

Kline history uses `endTime` and pages backward. `startTime` is no longer a request parameter, and each request is capped at 200 candles.

## version 3.2.7 ##
if PROFIT_PERCANTAGE, PROFIT_AMOUNT is > 0 then The trade is opened with take profit trade immediately instead of bot placing it after profit is reached.
if LOSS_PERCANTAGE, LOSS_AMOUNT is > 0 then The trade is opened with stop loss trade immediately instead of bot placing it after loss is reached.
_PERCANTAGE has greater priority over _AMOUNT. 

Added 2 new config
PROFIT_LOSS_PRICE_TYPE -> this can be "MARK_PRICE" or "LAST_PRICE"
PROFIT_LOSS_ORDER_TYPE -> this can be "MARKET" or "LIMIT" 


## version 3.2.6 ##
Default config:
Will open a trade ema_crossing or bbm_crosing or macd_crossing or rsi_crossing or bos_breakout or trendline_breakout happens and adx is strong and candle_trend is bullish.
Will close a trade profit percentage is 10% or loss percentage is 10%

Reversed the log entries to show the latest changes at the top.

Fix a bug in Selected Signal, not get cleared

Added a new config DELAY_IN_MINUTES_FOR_SAME_TICKER_TRADES, this will prohibit bot from trading the same ticker within the specified delay period. 

## verson 3.2.5 ##
Fixed Total Profit display

Added display of portfolio value in the web page

## version 3.2.4 ##
Added ma_spread to identify bullish or bearish trend, it is now used in EMA signals