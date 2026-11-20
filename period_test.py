"""Rank indicator periods on past Bitunix 1-hour candles.

This does not place orders and does not change config.txt. It replays the
open rule used when OPEN_ON_ANY_SIGNAL is on: one of EMA, MACD, Bollinger, or
RSI crosses, ADX is strong, and the candle color agrees. Break-of-structure
and trendline entries are left out, because those periods are not what this
comparison changes.

The first 70 percent of each symbol's bars are in-sample. The last 30 percent
are the score. A set that only wins on the first part is not treated as better.
"""
import argparse
import time
from pathlib import Path

import numpy as np
import pandas as pd
import requests
import talib

BASE = "https://fapi.bitunix.com"
CACHE = Path(__file__).resolve().parent / "period_test_cache"
REWARD_PCT = 0.20
RISK_PCT = 0.10
FEE = 0.0006 * 2
MAX_HOLD = 72
MIN_VOLUME = 50_000_000
HOLD_SPLIT = 0.70

EMA_SETS = (
    (8, 13, 21),
    (10, 20, 50),
    (9, 21, 50),
    (12, 26, 50),
    (5, 8, 13),
)
RSI_SETS = ((6, 14), (6, 24), (14, 28), (7, 21))
MACD_SETS = ((12, 26, 9), (8, 21, 5))
ADX_PERIODS = (14, 21)
BB_PERIOD = 20
BB_STD = 2


def get_json(path, params=None):
    last_error = None
    for attempt in range(4):
        try:
            response = requests.get(BASE + path, params=params, timeout=20)
            response.raise_for_status()
            payload = response.json()
            if payload.get("code") == 0:
                return payload.get("data")
            last_error = payload.get("msg")
            if "not allowed" in str(last_error).lower():
                return None
        except (requests.RequestException, ValueError) as error:
            last_error = error
        time.sleep(0.4 * (attempt + 1))
    raise RuntimeError(f"{path} failed: {last_error}")


def liquid_symbols(limit):
    rows = get_json("/api/v1/futures/market/tickers")
    chosen = []
    for row in rows:
        try:
            volume = float(row.get("baseVol") or 0)
        except (TypeError, ValueError):
            continue
        if volume >= MIN_VOLUME:
            chosen.append((volume, row["symbol"]))
    chosen.sort(reverse=True)
    return [symbol for _, symbol in chosen[:limit]]


def load_klines(symbol, interval, bars):
    CACHE.mkdir(exist_ok=True)
    cache_file = CACHE / f"{symbol}_{interval}_{bars}.npz"
    if cache_file.exists():
        stored = np.load(cache_file)
        return {key: stored[key] for key in stored.files}
    collected = []
    end_time = None
    while len(collected) < bars:
        page = min(200, bars - len(collected))
        params = {"symbol": symbol, "interval": interval, "limit": page}
        if end_time is not None:
            params["endTime"] = end_time
        batch = get_json("/api/v1/futures/market/kline", params)
        if not batch:
            break
        time.sleep(0.12)
        collected.extend(batch)
        if len(batch) < page:
            break
        end_time = min(int(item["time"]) for item in batch)
    if len(collected) < 200:
        return None
    frame = pd.DataFrame(collected).drop_duplicates("time")
    frame = frame.sort_values("time")
    arrays = {
        "time": frame["time"].astype(np.int64).to_numpy(),
        "open": frame["open"].astype(float).to_numpy(),
        "high": frame["high"].astype(float).to_numpy(),
        "low": frame["low"].astype(float).to_numpy(),
        "close": frame["close"].astype(float).to_numpy(),
    }
    np.savez(cache_file, **arrays)
    return arrays


def consecutive(close, open_):
    color = np.where(close >= open_, 1, -1)
    count = np.ones(len(close), dtype=int)
    for index in range(1, len(color)):
        count[index] = count[index - 1] + 1 if color[index] == color[index - 1] else 1
    return color, count


def candle_position(high, low, close):
    span = high - low
    position = np.zeros(len(close))
    usable = span > 0
    position[usable] = (close[usable] - low[usable]) / span[usable] * 100
    return position


def cross_up(fast, slow):
    signal = np.zeros(len(fast), dtype=bool)
    signal[1:] = (fast[1:] > slow[1:]) & (fast[:-1] <= slow[:-1])
    return signal


def cross_down(fast, slow):
    signal = np.zeros(len(fast), dtype=bool)
    signal[1:] = (fast[1:] < slow[1:]) & (fast[:-1] >= slow[:-1])
    return signal


def filled(values):
    return np.nan_to_num(values, nan=0.0)


def entries_for(candles, ema, rsi, macd, adx_period):
    close = candles["close"]
    color, count = candles["color"], candles["count"]
    fast = filled(talib.EMA(close, timeperiod=ema[0]))
    medium = filled(talib.EMA(close, timeperiod=ema[1]))
    slow = filled(talib.EMA(close, timeperiod=ema[2]))
    spread = np.diff(fast - slow, prepend=0.0)
    ema_buy = cross_up(medium, slow) & (spread > 0)
    ema_sell = cross_down(medium, slow) & (spread > 0)

    macd_line, macd_signal, _ = talib.MACD(close, fastperiod=macd[0], slowperiod=macd[1], signalperiod=macd[2])
    macd_line, macd_signal = filled(macd_line), filled(macd_signal)
    macd_buy = cross_up(macd_line, macd_signal)
    macd_sell = cross_down(macd_line, macd_signal)

    rsi_fast = filled(talib.RSI(close, timeperiod=rsi[0]))
    rsi_slow = filled(talib.RSI(close, timeperiod=rsi[1]))
    rsi_buy = cross_up(rsi_fast, rsi_slow)
    rsi_sell = cross_down(rsi_fast, rsi_slow)

    upper, middle, lower = talib.BBANDS(close, timeperiod=BB_PERIOD, nbdevup=BB_STD, nbdevdn=BB_STD)
    middle = filled(middle)
    bb_buy = cross_up(close, middle)
    bb_sell = cross_down(close, middle)

    adx = filled(talib.ADX(candles["high"], candles["low"], close, timeperiod=adx_period))
    strong = adx > 25
    bullish = candles["position"] > 70
    bearish = candles["position"] < 30
    green = (color == 1) & (count > 1)
    red = (color == -1) & (count > 1)

    study_buy = ema_buy | macd_buy | bb_buy | rsi_buy
    study_sell = ema_sell | macd_sell | bb_sell | rsi_sell
    buy = study_buy & strong & bullish & green
    sell = study_sell & (strong | bearish) & red
    return buy, sell


def trade_r(candles, start, side):
    open_ = candles["open"]
    high = candles["high"]
    low = candles["low"]
    close = candles["close"]
    last = min(len(close) - 1, start + MAX_HOLD - 1)
    if start > last:
        return None
    entry = open_[start]
    if entry <= 0:
        return None
    exit_at = last
    if side == 1:
        stop = entry * (1 - RISK_PCT)
        target = entry * (1 + REWARD_PCT)
        exit_price = close[last]
        for index in range(start, last + 1):
            if low[index] <= stop:
                exit_price = stop
                exit_at = index
                break
            if high[index] >= target:
                exit_price = target
                exit_at = index
                break
        move = (exit_price - entry) / entry
    else:
        stop = entry * (1 + RISK_PCT)
        target = entry * (1 - REWARD_PCT)
        exit_price = close[last]
        for index in range(start, last + 1):
            if high[index] >= stop:
                exit_price = stop
                exit_at = index
                break
            if low[index] <= target:
                exit_price = target
                exit_at = index
                break
        move = (entry - exit_price) / entry
    return (move - FEE) / RISK_PCT, exit_at


def score_symbol(candles, buy, sell):
    split = int(len(candles["close"]) * HOLD_SPLIT)
    sample = []
    holdout = []
    busy_until = -1
    indexes = np.flatnonzero(buy | sell)
    for index in indexes:
        if index < busy_until or index + 1 >= len(candles["close"]):
            continue
        side = 1 if buy[index] else -1
        result = trade_r(candles, index + 1, side)
        if result is None:
            continue
        result, exit_at = result
        busy_until = exit_at
        if index < split:
            sample.append(result)
        else:
            holdout.append(result)
    return sample, holdout


def summarize(values):
    if not values:
        return 0, 0.0, 0.0
    array = np.asarray(values, dtype=float)
    wins = float(np.mean(array > 0))
    return int(array.size), float(array.mean()), wins


def main():
    parser = argparse.ArgumentParser(description="Walk-forward test of indicator periods")
    parser.add_argument("--symbols", type=int, default=40)
    parser.add_argument("--bars", type=int, default=1600)
    parser.add_argument("--interval", default="1h")
    args = parser.parse_args()

    symbols = liquid_symbols(args.symbols * 2)
    books = {}
    for symbol in symbols:
        if len(books) >= args.symbols:
            break
        candles = load_klines(symbol, args.interval, args.bars)
        if not candles:
            print(f"Skipping {symbol}: no candle history", flush=True)
            continue
        color, count = consecutive(candles["close"], candles["open"])
        candles["color"] = color
        candles["count"] = count
        candles["position"] = candle_position(candles["high"], candles["low"], candles["close"])
        books[symbol] = candles
    print(f"Loaded {len(books)} symbols, up to {args.bars} {args.interval} bars.", flush=True)

    rows = []
    for ema in EMA_SETS:
        for rsi in RSI_SETS:
            for macd in MACD_SETS:
                for adx_period in ADX_PERIODS:
                    sample_all = []
                    holdout_all = []
                    for candles in books.values():
                        buy, sell = entries_for(candles, ema, rsi, macd, adx_period)
                        sample, holdout = score_symbol(candles, buy, sell)
                        sample_all.extend(sample)
                        holdout_all.extend(holdout)
                    sample_n, sample_r, sample_win = summarize(sample_all)
                    holdout_n, holdout_r, holdout_win = summarize(holdout_all)
                    live = ema == (8, 13, 21) and rsi == (6, 14) and macd == (12, 26, 9) and adx_period == 21
                    rows.append({
                        "ema": f"{ema[0]}/{ema[1]}/{ema[2]}",
                        "rsi": f"{rsi[0]}/{rsi[1]}",
                        "macd": f"{macd[0]}/{macd[1]}/{macd[2]}",
                        "adx": adx_period,
                        "live": live,
                        "sample_trades": sample_n,
                        "sample_avg_r": round(sample_r, 3),
                        "sample_win": round(sample_win, 3),
                        "holdout_trades": holdout_n,
                        "holdout_avg_r": round(holdout_r, 3),
                        "holdout_win": round(holdout_win, 3),
                    })
                    print(
                        f"EMA {rows[-1]['ema']} RSI {rows[-1]['rsi']} MACD {rows[-1]['macd']} "
                        f"ADX {adx_period}: holdout {holdout_n} trades, avg R {holdout_r:.3f}",
                        flush=True,
                    )

    frame = pd.DataFrame(rows).sort_values(["holdout_avg_r", "holdout_trades"], ascending=[False, False]).reset_index(drop=True)
    out = Path(__file__).resolve().parent / "period_test_results.csv"
    frame.to_csv(out, index=False)
    print("\nBest holdout period sets")
    print(frame.head(8).to_string(index=False))
    live_rows = frame[frame["live"]]
    if not live_rows.empty:
        rank = int(live_rows.index[0]) + 1
        print(f"\nCurrent live settings rank {rank} of {len(frame)} by holdout average R.")
        print(live_rows.to_string(index=False))
    print(f"\nFull table: {out}")


if __name__ == "__main__":
    main()
