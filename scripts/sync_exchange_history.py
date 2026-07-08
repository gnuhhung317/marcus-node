#!/usr/bin/env python3
import os
import sys
import json
import uuid
import math
import re
import sqlite3
import subprocess
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import deque
import psycopg2
from psycopg2.extras import execute_values
import ccxt
import dotenv

# Set sys.path to allow imports from local_executor if needed
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

def load_config():
    dotenv_path = Path(__file__).resolve().parent.parent / ".env"
    dotenv.load_dotenv(dotenv_path)

    repo_root = Path(__file__).resolve().parent.parent
    default_dashboard_inventory = (
        repo_root.parent.parent
        / "self-projects"
        / "macd-overlay - Copy"
        / "ansible"
        / "inventory.ini"
    )
    
    # Load settings
    config = {
        "EXCHANGE_ID": os.getenv("EXCHANGE_ID", "binance"),
        "EXCHANGE_API_KEY": os.getenv("EXCHANGE_API_KEY", "860SirbFKW6cWo5eTCnSs6NErOBWgyMb16FP8gRsocm5djLouOkbEHQQzzM2bE8Y"),
        "EXCHANGE_API_SECRET": os.getenv("EXCHANGE_API_SECRET", "ByUgVbusXOqPj1yECfDetdUFKrlT0hdNgsL3PzVcRQi1frsKNfc3gFV5XCVsyRZ2"),
        "EXCHANGE_SANDBOX": os.getenv("EXCHANGE_SANDBOX", "false").lower() == "true",
        "EXCHANGE_DEFAULT_TYPE": os.getenv("EXCHANGE_DEFAULT_TYPE", "future"),
        "DB_HOST": os.getenv("DB_HOST", "171.244.195.150"),
        "DB_PORT": int(os.getenv("DB_PORT", "5432")),
        "DB_NAME": os.getenv("DB_NAME", "signal_db"),
        "DB_USER": os.getenv("DB_USER", "user"),
        "DB_PASSWORD": os.getenv("DB_PASSWORD", "password"),
        "SYSTEM_WS_TOKEN": os.getenv("SYSTEM_WS_TOKEN", "ws_default_token"),
        "EXECUTION_MODE": os.getenv("EXECUTION_MODE", "live"),
        "DASHBOARD_INVENTORY_PATH": os.getenv("DASHBOARD_INVENTORY_PATH", str(default_dashboard_inventory)),
        "DASHBOARD_BOT_NAME": os.getenv("DASHBOARD_BOT_NAME", "pnl_dashboard"),
        "DASHBOARD_REMOTE_DB_PATH": os.getenv("DASHBOARD_REMOTE_DB_PATH", ""),
        "DASHBOARD_START": os.getenv("DASHBOARD_START", "2026-05-01 00:00:00"),
        "DASHBOARD_ACCOUNT_NAME": os.getenv("DASHBOARD_ACCOUNT_NAME", "binance_livetest"),
        "DASHBOARD_MAX_DRIFT_USDT": float(os.getenv("DASHBOARD_MAX_DRIFT_USDT", "1.0")),
    }
    return config

def get_db_connection(config):
    try:
        conn = psycopg2.connect(
            host=config["DB_HOST"],
            port=config["DB_PORT"],
            database=config["DB_NAME"],
            user=config["DB_USER"],
            password=config["DB_PASSWORD"]
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def list_bots(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT bot_id, name, trading_pair, status, developer_id FROM bots ORDER BY name ASC")
        bots = cur.fetchall()
        return bots
    except Exception as e:
        print(f"Error fetching bots: {e}")
        sys.exit(1)
    finally:
        cur.close()

def _normalize_key(value):
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())

def _email_local_part(email):
    email = str(email or "").strip()
    if "@" in email:
        return email.split("@", 1)[0]
    return email

def select_bot_from_catalog(bots, bot_id=None, bot_name_hint=None, dev_email=None, developer_user_id=None):
    if bot_id:
        return bot_id

    hints = []
    if bot_name_hint:
        hints.append(bot_name_hint)
    if dev_email:
        hints.append(_email_local_part(dev_email))
        hints.append(dev_email)

    normalized_hints = [_normalize_key(hint) for hint in hints if hint]
    if not normalized_hints:
        return bots[0][0] if bots else None

    normalized_rows = [
        (
            bot_id_value,
            name,
            trading_pair,
            status,
            developer_id,
            _normalize_key(bot_id_value),
            _normalize_key(name),
            _normalize_key(trading_pair or ""),
        )
        for bot_id_value, name, trading_pair, status, developer_id in bots
    ]

    if developer_user_id:
        developer_user_id = str(developer_user_id).strip()
        for bot_id_value, name, trading_pair, status, bot_developer_id, bot_key, name_key, pair_key in normalized_rows:
            if str(bot_developer_id).strip() == developer_user_id:
                return bot_id_value

    for hint in normalized_hints:
        for bot_id_value, name, trading_pair, status, bot_developer_id, bot_key, name_key, pair_key in normalized_rows:
            if hint in (bot_key, name_key, pair_key):
                return bot_id_value

    for hint in normalized_hints:
        for bot_id_value, name, trading_pair, status, bot_developer_id, bot_key, name_key, pair_key in normalized_rows:
            if hint and (hint in bot_key or hint in name_key or hint in pair_key):
                return bot_id_value

    return None

def select_user_by_email(users, email):
    if not email:
        return None
    target = str(email).strip().lower()
    for user_id, user_email, username in users:
        if str(user_email).strip().lower() == target:
            return user_id, user_email
    return None

def _parse_inventory_value(line, key, default=None):
    match = re.search(rf"{re.escape(key)}=([^\s]+)", line)
    if match:
        return match.group(1).strip().strip("'\"")
    return default

def load_dashboard_target(config):
    inventory_path = Path(config["DASHBOARD_INVENTORY_PATH"])
    bot_name = config.get("DASHBOARD_BOT_NAME", "pnl_dashboard")
    remote_db_path = config.get("DASHBOARD_REMOTE_DB_PATH") or f"/opt/{bot_name}/pnl_dashboard/pnl_history.db"

    host = "103.216.117.103"
    user = "root"
    port = 24700

    if inventory_path.exists():
        current_section = None
        try:
            for raw_line in inventory_path.read_text(encoding="utf-8", errors="ignore").splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#"):
                    continue
                if line.startswith("[") and line.endswith("]"):
                    current_section = line[1:-1]
                    continue
                if current_section == "all_bots:vars":
                    parsed_port = _parse_inventory_value(line, "ansible_ssh_port")
                    if parsed_port and parsed_port.isdigit():
                        port = int(parsed_port)
                    continue
                if current_section == "dashboards" and line.split()[0] == bot_name:
                    parsed_host = _parse_inventory_value(line, "ansible_host")
                    parsed_user = _parse_inventory_value(line, "ansible_user")
                    parsed_bot_name = _parse_inventory_value(line, "bot_name")
                    if parsed_host:
                        host = parsed_host
                    if parsed_user:
                        user = parsed_user
                    if parsed_bot_name:
                        bot_name = parsed_bot_name
                    break
        except Exception as exc:
            print(f"Warning: Could not parse dashboard inventory '{inventory_path}': {exc}")

    if config.get("DASHBOARD_REMOTE_DB_PATH"):
        remote_db_path = config["DASHBOARD_REMOTE_DB_PATH"]
    else:
        remote_db_path = f"/opt/{bot_name}/pnl_dashboard/pnl_history.db"

    return {
        "inventory_path": inventory_path,
        "bot_name": bot_name,
        "host": host,
        "user": user,
        "port": port,
        "remote_db_path": remote_db_path,
    }

def _parse_dashboard_timestamp(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)

def _normalize_portfolio_points(points):
    deduped = {}
    for point in points:
        deduped[point["timestamp"]] = point
    return sorted(deduped.values(), key=lambda item: item["timestamp"])

def _attach_cumulative_fees(portfolio_points, closed_trades):
    if not portfolio_points:
        return []

    sorted_points = sorted(portfolio_points, key=lambda item: item["timestamp"])
    sorted_trades = sorted(closed_trades, key=lambda item: item["exit_timestamp"])
    enriched = []
    cumulative_fees = 0.0
    trade_index = 0

    for point in sorted_points:
        point_ts = point["timestamp"]
        while trade_index < len(sorted_trades) and sorted_trades[trade_index]["exit_timestamp"] <= point_ts:
            cumulative_fees += float(sorted_trades[trade_index].get("fees", 0.0) or 0.0)
            trade_index += 1

        point_copy = dict(point)
        point_copy["total_fees"] = cumulative_fees
        enriched.append(point_copy)

    return enriched

def calculate_metrics(portfolio_points):
    if len(portfolio_points) < 2:
        return 0.0, 0.0, 0.0, 0

    sorted_points = sorted(portfolio_points, key=lambda x: x['timestamp'])

    first_equity = float(sorted_points[0]['equity'])
    last_equity = float(sorted_points[-1]['equity'])

    if first_equity <= 0:
        return 0.0, 0.0, 0.0, 0

    first_ts = sorted_points[0]['timestamp']
    last_ts = sorted_points[-1]['timestamp']

    days = (last_ts.date() - first_ts.date()).days
    sample_days = max(1, days + 1)

    cagr = ((last_equity / first_equity) ** (365.0 / sample_days)) - 1.0

    peak = first_equity
    max_dd = 0.0
    for point in sorted_points:
        equity = float(point['equity'])
        peak = max(peak, equity)
        if peak > 0:
            drawdown = (peak - equity) / peak
            max_dd = max(max_dd, drawdown)

    daily_returns = []
    for index in range(1, len(sorted_points)):
        prev_equity = float(sorted_points[index - 1]['equity'])
        curr_equity = float(sorted_points[index]['equity'])
        if prev_equity > 0:
            daily_returns.append((curr_equity - prev_equity) / prev_equity)

    if not daily_returns:
        annualized_vol = 0.0
    else:
        mean_return = sum(daily_returns) / len(daily_returns)
        variance = sum((value - mean_return) ** 2 for value in daily_returns) / len(daily_returns)
        daily_vol = math.sqrt(variance)
        annualized_vol = daily_vol * math.sqrt(365.0)

    sharpe = 0.0 if annualized_vol == 0.0 else (cagr / annualized_vol)

    return cagr, -abs(max_dd), sharpe, sample_days

def fetch_dashboard_portfolio_points(config, dashboard_start, dashboard_end=None):
    target = load_dashboard_target(config)
    start_ts = _parse_dashboard_timestamp(dashboard_start)
    end_ts = _parse_dashboard_timestamp(dashboard_end) if dashboard_end else None

    if start_ts is None:
        raise ValueError("dashboard_start is required")

    with tempfile.TemporaryDirectory(prefix="pnl_dashboard_") as temp_dir:
        local_db_path = Path(temp_dir) / "pnl_history.db"
        remote_target = f"{target['user']}@{target['host']}:{target['remote_db_path']}"
        scp_command = [
            "scp",
            "-P",
            str(target["port"]),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
            remote_target,
            str(local_db_path),
        ]
        result = subprocess.run(scp_command, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(f"Failed to download dashboard DB: {result.stderr.strip() or result.stdout.strip()}")

        conn = sqlite3.connect(str(local_db_path))
        try:
            cur = conn.cursor()
            if end_ts:
                cur.execute(
                    """
                    SELECT timestamp, total_equity, total_unrealized_pnl, total_open_positions
                    FROM global_equity_history
                    WHERE timestamp >= ? AND timestamp <= ?
                    ORDER BY timestamp ASC
                    """,
                    (start_ts.strftime("%Y-%m-%d %H:%M:%S"), end_ts.strftime("%Y-%m-%d %H:%M:%S")),
                )
            else:
                cur.execute(
                    """
                    SELECT timestamp, total_equity, total_unrealized_pnl, total_open_positions
                    FROM global_equity_history
                    WHERE timestamp >= ?
                    ORDER BY timestamp ASC
                    """,
                    (start_ts.strftime("%Y-%m-%d %H:%M:%S"),),
                )
            rows = cur.fetchall()
        finally:
            conn.close()

    points = []
    for row in rows:
        timestamp = _parse_dashboard_timestamp(row[0])
        equity = float(row[1] or 0.0)
        unrealized_pnl = float(row[2] or 0.0)
        cash = equity - unrealized_pnl
        points.append(
            {
                "timestamp": timestamp,
                "cash": cash,
                "equity": equity,
                "realized_pnl": 0.0,
                "unrealized_pnl": unrealized_pnl,
                "total_fees": 0.0,
                "open_positions": int(row[3] or 0),
            }
        )

    if not points:
        raise RuntimeError("No dashboard portfolio rows found for the requested window.")

    trimmed_points = list(points)
    while len(trimmed_points) > 1 and float(trimmed_points[0]["equity"]) <= 0.0:
        trimmed_points.pop(0)

    if not trimmed_points:
        raise RuntimeError("Dashboard history only contained non-positive equity rows.")

    initial_cash = float(trimmed_points[0]["cash"])
    for point in trimmed_points:
        point["realized_pnl"] = float(point["cash"]) - initial_cash

    return _normalize_portfolio_points(trimmed_points)

def _market_id_to_symbol(exchange, raw_symbol):
    if not raw_symbol:
        return None

    try:
        market = exchange.market(raw_symbol)
        return market.get("symbol")
    except Exception:
        pass

    try:
        markets = exchange.markets_by_id.get(raw_symbol)
        if isinstance(markets, list) and markets:
            preferred = next((item for item in markets if item.get("swap") or item.get("future")), markets[0])
            return preferred.get("symbol")
        if isinstance(markets, dict):
            return markets.get("symbol")
    except Exception:
        pass

    try:
        return exchange.safe_symbol(raw_symbol, None, None, "swap")
    except Exception:
        return None

def discover_binance_futures_symbols(exchange, since_ts, until_ts):
    if since_ts is None:
        since_ts = int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp() * 1000)

    symbols = set()
    chunk_ms = 7 * 24 * 60 * 60 * 1000 - 1
    window_start = since_ts

    print("Scanning Binance futures income history for traded symbols...")
    while window_start <= until_ts:
        window_end = min(window_start + chunk_ms, until_ts)
        try:
            rows = exchange.fapiPrivateGetIncome({
                "startTime": window_start,
                "endTime": window_end,
                "limit": 1000,
            })
            for row in rows or []:
                symbol = _market_id_to_symbol(exchange, row.get("symbol"))
                if symbol:
                    symbols.add(symbol)
        except Exception as exc:
            print(f"  Warning: Could not scan income window {window_start}-{window_end}: {exc}")

        window_start = window_end + 1

    return symbols

def fetch_binance_futures_my_trades(exchange, symbol, since_ts, until_ts):
    if since_ts is None:
        since_ts = int((datetime.now(timezone.utc) - timedelta(days=365)).timestamp() * 1000)

    all_symbol_trades = []
    seen_trade_ids = set()
    chunk_ms = 7 * 24 * 60 * 60 * 1000 - 1
    window_start = since_ts

    while window_start <= until_ts:
        window_end = min(window_start + chunk_ms, until_ts)
        request_since = window_start

        while request_since <= window_end:
            fetched = exchange.fetch_my_trades(
                symbol,
                since=request_since,
                limit=1000,
                params={"endTime": window_end},
            )

            if not fetched:
                break

            max_timestamp = request_since
            for trade in fetched:
                trade_id = str(trade.get("id") or f"{trade.get('timestamp')}-{trade.get('order')}-{trade.get('side')}-{trade.get('amount')}-{trade.get('price')}")
                if trade_id in seen_trade_ids:
                    continue
                seen_trade_ids.add(trade_id)
                all_symbol_trades.append(trade)
                if trade.get("timestamp") is not None:
                    max_timestamp = max(max_timestamp, int(trade["timestamp"]))

            if len(fetched) < 1000 or max_timestamp < request_since:
                break

            request_since = max_timestamp + 1

        window_start = window_end + 1

    all_symbol_trades.sort(key=lambda item: item["timestamp"])
    return all_symbol_trades

def fetch_trades_from_exchange(config, symbols, since_days, mock=False):
    if mock:
        print("Generating mock trade history...")
        import random
        mock_trades = []
        now_dt = datetime.now(timezone.utc)
        
        # If symbols is empty, generate mock for a default pair
        if not symbols:
            symbols = ["BTC/USDT"]
            
        # Generate some buy/sell fills over the last 15 days
        for symbol in symbols:
            current_price = 65000.0 if "BTC" in symbol else (3500.0 if "ETH" in symbol else 150.0)
            trade_time = now_dt - timedelta(days=15)
            
            # Generate 15 rounds of trades
            for i in range(15):
                # Round-trip entry and exit
                qty = round(random.uniform(0.05, 0.2), 4) if "BTC" in symbol else round(random.uniform(0.5, 2.0), 3)
                
                # 1. Entry Trade (Buy or Sell)
                is_long = random.choice([True, False])
                entry_side = 'buy' if is_long else 'sell'
                entry_price = current_price * random.uniform(0.99, 1.01)
                entry_time = trade_time + timedelta(hours=random.uniform(1, 4))
                entry_fee = entry_price * qty * 0.0004 # 0.04% commission
                
                mock_trades.append({
                    'id': f"mock_ent_{symbol.replace('/', '_')}_{i}",
                    'timestamp': int(entry_time.timestamp() * 1000),
                    'datetime': entry_time.isoformat(),
                    'symbol': symbol,
                    'side': entry_side,
                    'price': entry_price,
                    'amount': qty,
                    'fee': {'cost': entry_fee, 'currency': 'USDT'},
                })
                
                # 2. Exit Trade
                exit_side = 'sell' if is_long else 'buy'
                # Simulate a random gain/loss
                price_move = random.uniform(-0.015, 0.025) if is_long else random.uniform(-0.025, 0.015)
                exit_price = entry_price * (1.0 + price_move)
                exit_time = entry_time + timedelta(hours=random.uniform(2, 24))
                exit_fee = exit_price * qty * 0.0004
                
                mock_trades.append({
                    'id': f"mock_ext_{symbol.replace('/', '_')}_{i}",
                    'timestamp': int(exit_time.timestamp() * 1000),
                    'datetime': exit_time.isoformat(),
                    'symbol': symbol,
                    'side': exit_side,
                    'price': exit_price,
                    'amount': qty,
                    'fee': {'cost': exit_fee, 'currency': 'USDT'},
                })
                
                # Advance time for next trade
                trade_time = exit_time + timedelta(hours=random.uniform(4, 12))
                # Update base price with some random walk
                current_price = exit_price
                
        mock_trades.sort(key=lambda x: x['timestamp'])
        return mock_trades, 10000.0

    exchange_id = config["EXCHANGE_ID"].lower()
    exchange_class = getattr(ccxt, exchange_id, None)
    if not exchange_class:
        print(f"CCXT does not support exchange: {exchange_id}")
        sys.exit(1)
        
    exchange_params = {
        "apiKey": config["EXCHANGE_API_KEY"],
        "secret": config["EXCHANGE_API_SECRET"],
        "enableRateLimit": True,
    }
    
    # Binance Futures defaults
    if exchange_id == "binance":
        exchange_params["options"] = {"defaultType": config["EXCHANGE_DEFAULT_TYPE"]}
        
    exchange = exchange_class(exchange_params)
    
    if config["EXCHANGE_SANDBOX"]:
        if hasattr(exchange, 'enable_demo_trading'):
            exchange.enable_demo_trading(True)
        elif hasattr(exchange, 'enableDemoTrading'):
            exchange.enableDemoTrading(True)
        else:
            exchange.set_sandbox_mode(True)
        print(f"Using sandbox/demo mode for {exchange_id}...")
        
    print(f"Connecting to {exchange_id}...")
    try:
        exchange.load_markets()
    except Exception as e:
        print(f"Error loading exchange markets: {e}")
        sys.exit(1)

    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    since_ts = None
    if since_days:
        since_ts = int((datetime.now(timezone.utc) - timedelta(days=since_days)).timestamp() * 1000)
    is_binance_futures = (
        exchange_id == "binance"
        and str(config.get("EXCHANGE_DEFAULT_TYPE", "")).lower() in ("future", "futures", "swap")
    )
        
    # Discover all traded symbols if symbols is empty
    if not symbols:
        print("No trading symbols specified. Scanning transaction history and positions for traded symbols...")
        discovered_symbols = set()

        if is_binance_futures:
            discovered_symbols.update(discover_binance_futures_symbols(exchange, since_ts, now_ts))
        
        # 1. Scan ledger (funding fees, commission fees, transfers, realized pnl etc)
        try:
            print("Scanning exchange ledger...")
            ledger = exchange.fetch_ledger(since=since_ts, limit=1000) if since_ts else exchange.fetch_ledger(limit=1000)
            for entry in ledger:
                info = entry.get('info', {})
                raw_symbol = info.get('symbol')
                if raw_symbol:
                    symbol = _market_id_to_symbol(exchange, raw_symbol)
                    if symbol:
                        discovered_symbols.add(symbol)
        except Exception as e:
            print(f"  Warning: Could not fetch ledger: {e}")
            
        # 2. Scan active positions
        try:
            print("Scanning active positions...")
            positions = exchange.fetch_positions()
            for pos in positions:
                contracts = float(pos.get('contracts', 0) or pos.get('size', 0) or 0)
                if contracts != 0 and pos.get('symbol'):
                    discovered_symbols.add(pos['symbol'])
        except Exception as e:
            print(f"  Warning: Could not fetch positions: {e}")
            
        symbols = list(discovered_symbols)
        if not symbols:
            print("No traded symbols discovered. Defaulting to BTC/USDT.")
            symbols = ["BTC/USDT"]
        else:
            print(f"Discovered traded symbols: {', '.join(symbols)}")
            
    # Fetch real cash balance
    real_balance = 0.0
    try:
        print("Fetching real account balance...")
        balance = exchange.fetch_balance()
        if 'total' in balance:
            # Check standard margin assets
            for asset in ['USDT', 'USDC', 'USD', 'USDS']:
                if balance['total'].get(asset) is not None and float(balance['total'][asset]) > 0:
                    real_balance = float(balance['total'][asset])
                    print(f"Found non-zero balance for {asset}: {real_balance}")
                    break
            if real_balance == 0.0:
                # Fallback to the first asset with a non-zero balance
                for asset, val in balance['total'].items():
                    if val and float(val) > 0:
                        real_balance = float(val)
                        print(f"Found non-zero balance for {asset}: {real_balance}")
                        break
    except Exception as e:
        print(f"  Warning: Could not fetch balance from exchange: {e}")
        real_balance = 0.0
        
    all_trades = []
        
    for symbol in symbols:
        print(f"Fetching trade history for {symbol}...")
        try:
            if is_binance_futures:
                fetched = fetch_binance_futures_my_trades(exchange, symbol, since_ts, now_ts)
            else:
                fetched = exchange.fetch_my_trades(symbol, since=since_ts, limit=1000)
            if since_ts:
                fetched = [t for t in fetched if t['timestamp'] >= since_ts]
            print(f"  Fetched {len(fetched)} trades for {symbol}")
            all_trades.extend(fetched)
        except Exception as e:
            print(f"  Error fetching trades for {symbol}: {e}")
            
    # Sort all trades chronologically
    all_trades.sort(key=lambda x: x['timestamp'])
    return all_trades, real_balance

def fifo_match_trades(trades):
    # Group trades by symbol
    trades_by_symbol = {}
    for t in trades:
        sym = t['symbol']
        if sym not in trades_by_symbol:
            trades_by_symbol[sym] = []
        trades_by_symbol[sym].append(t)
        
    closed_trades = []
    
    for sym, sym_trades in trades_by_symbol.items():
        sym_trades.sort(key=lambda x: x['timestamp'])
        queue = deque()  # stores active open positions
        
        for trade in sym_trades:
            side = trade['side'].lower()  # 'buy' or 'sell'
            price = float(trade['price'])
            amount = float(trade['amount'])
            fee = float(trade['fee']['cost']) if (trade.get('fee') and trade['fee'].get('cost') is not None) else 0.0
            timestamp = trade['timestamp']
            dt = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc)
            trade_id = trade['id']
            
            if not queue:
                queue.append({
                    'side': side,
                    'price': price,
                    'amount': amount,
                    'fee': fee,
                    'timestamp': timestamp,
                    'dt': dt,
                    'trade_id': trade_id,
                    'remaining_amount': amount,
                })
                continue
                
            queue_side = queue[0]['side']
            if queue_side == side:
                queue.append({
                    'side': side,
                    'price': price,
                    'amount': amount,
                    'fee': fee,
                    'timestamp': timestamp,
                    'dt': dt,
                    'trade_id': trade_id,
                    'remaining_amount': amount,
                })
            else:
                rem_amount = amount
                while rem_amount > 0.00000001 and queue:
                    oldest = queue[0]
                    match_qty = min(rem_amount, oldest['remaining_amount'])
                    
                    # Proportional fees
                    entry_prop_fee = oldest['fee'] * (match_qty / oldest['amount'])
                    exit_prop_fee = fee * (match_qty / amount)
                    
                    oldest['remaining_amount'] -= match_qty
                    rem_amount -= match_qty
                    
                    if oldest['side'] == 'buy':
                        pnl = (price - oldest['price']) * match_qty
                        trade_side = 'LONG'
                    else:
                        pnl = (oldest['price'] - price) * match_qty
                        trade_side = 'SHORT'
                        
                    closed_trades.append({
                        'symbol': sym,
                        'side': trade_side,
                        'quantity': match_qty,
                        'entry_price': oldest['price'],
                        'exit_price': price,
                        'pnl': pnl - (entry_prop_fee + exit_prop_fee), # Net PnL
                        'fees': entry_prop_fee + exit_prop_fee,
                        'entry_timestamp': oldest['dt'],
                        'exit_timestamp': dt,
                        'trade_id': f"{oldest['trade_id']}_{trade_id}",
                    })
                    
                    if oldest['remaining_amount'] <= 0.00000001:
                        queue.popleft()
                        
                if rem_amount > 0.00000001:
                    queue.append({
                        'side': side,
                        'price': price,
                        'amount': rem_amount,
                        'fee': fee * (rem_amount / amount),
                        'timestamp': timestamp,
                        'dt': dt,
                        'trade_id': trade_id,
                        'remaining_amount': rem_amount,
                    })
                    
    # Sort closed trades by exit timestamp
    closed_trades.sort(key=lambda x: x['exit_timestamp'])
    return closed_trades

def insert_trade_data(conn, bot_id, closed_trades, initial_balance, portfolio_points=None):
    cur = conn.cursor()
    try:
        # 1. Purge old dry run and execution history for OUT_OF_SAMPLE
        print("Purging existing dry-run trade logs and portfolios for the bot...")
        cur.execute("DELETE FROM bot_dry_run_portfolios WHERE bot_id = %s;", (bot_id,))
        cur.execute("DELETE FROM bot_dry_run_closed_trades WHERE bot_id = %s;", (bot_id,))
        cur.execute("DELETE FROM execution_event WHERE signal_id IN (SELECT signal_id FROM signals WHERE bot_id = %s);", (bot_id,))
        cur.execute("DELETE FROM execution_state WHERE signal_id IN (SELECT signal_id FROM signals WHERE bot_id = %s);", (bot_id,))
        cur.execute("DELETE FROM signals WHERE bot_id = %s;", (bot_id,))
        
        # 2. Insert signals, execution states, events, and closed trades
        print("Inserting execution states and closed trades...")
        now = datetime.now(timezone.utc)
        
        for trade in closed_trades:
            # Entry Signal
            entry_sig_id = str(uuid.uuid4())
            entry_action = "OPEN_LONG" if trade['side'] == "LONG" else "OPEN_SHORT"
            cur.execute("""
                INSERT INTO signals (
                    id, signal_id, bot_id, symbol, action, market_type, order_type,
                    entry, amount, reduce_only, status, generated_timestamp, timeframe,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                entry_sig_id, entry_sig_id, bot_id, trade['symbol'], entry_action, 'FUTURE', 'MARKET',
                trade['entry_price'], trade['quantity'], False, 'ACKNOWLEDGED', trade['entry_timestamp'], '60',
                now, now
            ))
            
            # Exit Signal
            exit_sig_id = str(uuid.uuid4())
            exit_action = "CLOSE_LONG" if trade['side'] == "LONG" else "CLOSE_SHORT"
            cur.execute("""
                INSERT INTO signals (
                    id, signal_id, bot_id, symbol, action, market_type, order_type,
                    entry, amount, reduce_only, status, generated_timestamp, timeframe,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                exit_sig_id, exit_sig_id, bot_id, trade['symbol'], exit_action, 'FUTURE', 'MARKET',
                trade['exit_price'], trade['quantity'], True, 'ACKNOWLEDGED', trade['exit_timestamp'], '60',
                now, now
            ))
            
            # Entry Execution State
            cur.execute("""
                INSERT INTO execution_state (
                    signal_id, signal_state, order_state, position_state, last_sequence,
                    last_event_time, closed_at, created_at, updated_at, version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                entry_sig_id, 'OPEN', 'FILLED', 'OPENED', 3,
                trade['entry_timestamp'], None, now, now, 0
            ))
            
            # Exit Execution State
            cur.execute("""
                INSERT INTO execution_state (
                    signal_id, signal_state, order_state, position_state, last_sequence,
                    last_event_time, closed_at, created_at, updated_at, version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                exit_sig_id, 'CLOSED', 'FILLED', 'CLOSED', 3,
                trade['exit_timestamp'], trade['exit_timestamp'], now, now, 0
            ))
            
            # Entry Execution Events (Sequence 0 to 3)
            entry_events = [
                ("SIGNAL_ACCEPTED", 0, {"signal_id": entry_sig_id, "status": "accepted"}),
                ("ORDER_PLACED", 1, {"order_id": entry_sig_id + "_ord", "price": trade['entry_price']}),
                ("ORDER_FILLED", 2, {"order_id": entry_sig_id + "_ord", "fill_price": trade['entry_price'], "price": trade['entry_price']}),
                ("POSITION_OPENED", 3, {"position_size": trade['quantity'], "size": trade['quantity']})
            ]
            
            for ev_type, seq, payload in entry_events:
                cur.execute("""
                    INSERT INTO execution_event (
                        event_id, signal_id, sequence, event_type, sent_at, exchange_time, payload, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    str(uuid.uuid4()), entry_sig_id, seq, ev_type, trade['entry_timestamp'], trade['entry_timestamp'],
                    json.dumps(payload), now
                ))
                
            # Exit Execution Events (Sequence 0 to 3)
            exit_events = [
                ("SIGNAL_ACCEPTED", 0, {"signal_id": exit_sig_id, "status": "accepted"}),
                ("ORDER_PLACED", 1, {"order_id": exit_sig_id + "_ord", "price": trade['exit_price']}),
                ("ORDER_FILLED", 2, {"order_id": exit_sig_id + "_ord", "fill_price": trade['exit_price'], "price": trade['exit_price']}),
                ("POSITION_CLOSED", 3, {"pnl": trade['pnl'], "exit_price": trade['exit_price']})
            ]
            
            for ev_type, seq, payload in exit_events:
                cur.execute("""
                    INSERT INTO execution_event (
                        event_id, signal_id, sequence, event_type, sent_at, exchange_time, payload, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    str(uuid.uuid4()), exit_sig_id, seq, ev_type, trade['exit_timestamp'], trade['exit_timestamp'],
                    json.dumps(payload), now
                ))
                
            # Bot Dry Run Closed Trade
            cur.execute("""
                INSERT INTO bot_dry_run_closed_trades (
                    id, bot_id, trade_id, data_source, symbol, market_type, side, quantity,
                    entry_price, exit_price, pnl, fees, entry_timestamp, exit_timestamp,
                    entry_signal_id, exit_signal_id, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (bot_id, trade_id) DO UPDATE SET
                    data_source = EXCLUDED.data_source,
                    symbol = EXCLUDED.symbol,
                    market_type = EXCLUDED.market_type,
                    side = EXCLUDED.side,
                    quantity = EXCLUDED.quantity,
                    entry_price = EXCLUDED.entry_price,
                    exit_price = EXCLUDED.exit_price,
                    pnl = EXCLUDED.pnl,
                    fees = EXCLUDED.fees,
                    entry_timestamp = EXCLUDED.entry_timestamp,
                    exit_timestamp = EXCLUDED.exit_timestamp,
                    entry_signal_id = EXCLUDED.entry_signal_id,
                    exit_signal_id = EXCLUDED.exit_signal_id,
                    updated_at = EXCLUDED.updated_at;
            """, (
                str(uuid.uuid4()), bot_id, trade['trade_id'], 'OUT_OF_SAMPLE', trade['symbol'], 'FUTURE',
                trade['side'], trade['quantity'], trade['entry_price'], trade['exit_price'],
                trade['pnl'], trade['fees'], trade['entry_timestamp'], trade['exit_timestamp'],
                entry_sig_id, exit_sig_id, now, now
            ))
            
        # 3. Generate Equity Curve (Portfolios)
        if portfolio_points:
            print("Using dashboard portfolio snapshots for equity curve...")
            final_portfolios = _attach_cumulative_fees(_normalize_portfolio_points(portfolio_points), closed_trades)
            initial_balance = float(final_portfolios[0]['cash'])
        else:
            print("Reconstructing equity curve points...")
            portfolios = []
            if closed_trades:
                first_entry = min(t['entry_timestamp'] for t in closed_trades)
                initial_ts = first_entry - timedelta(seconds=1)
            else:
                initial_ts = now

            portfolios.append({
                'timestamp': initial_ts,
                'cash': initial_balance,
                'equity': initial_balance,
                'realized_pnl': 0.0,
                'unrealized_pnl': 0.0,
                'total_fees': 0.0,
            })

            current_cash = initial_balance
            cumulative_realized_pnl = 0.0
            cumulative_fees = 0.0

            for trade in closed_trades:
                cumulative_realized_pnl += trade['pnl']
                cumulative_fees += trade['fees']
                current_cash += trade['pnl']

                portfolios.append({
                    'timestamp': trade['exit_timestamp'],
                    'cash': current_cash,
                    'equity': current_cash,
                    'realized_pnl': cumulative_realized_pnl,
                    'unrealized_pnl': 0.0,
                    'total_fees': cumulative_fees,
                })

            final_portfolios = _normalize_portfolio_points(portfolios)

        # Insert Portfolios
        portfolio_rows = [
            (
                str(uuid.uuid4()),
                bot_id,
                'OUT_OF_SAMPLE',
                p['timestamp'],
                p['cash'],
                p['equity'],
                p.get('realized_pnl', 0.0),
                p.get('unrealized_pnl', 0.0),
                p.get('total_fees', 0.0),
                now,
                now,
            )
            for p in final_portfolios
        ]

        execute_values(
            cur,
            """
                INSERT INTO bot_dry_run_portfolios (
                    id, bot_id, data_source, timestamp, cash, equity, realized_pnl, unrealized_pnl, total_fees, created_at, updated_at
                ) VALUES %s
                ON CONFLICT (bot_id, timestamp) DO UPDATE SET
                    data_source = EXCLUDED.data_source,
                    cash = EXCLUDED.cash,
                    equity = EXCLUDED.equity,
                    realized_pnl = EXCLUDED.realized_pnl,
                    unrealized_pnl = EXCLUDED.unrealized_pnl,
                    total_fees = EXCLUDED.total_fees,
                    updated_at = EXCLUDED.updated_at;
            """,
            portfolio_rows,
            page_size=1000,
        )
            
        # 4. Calculate Leaderboard Metrics
        print("Calculating leaderboard metrics...")
        cagr, max_dd, sharpe, sample_days = calculate_metrics(final_portfolios)
        print(f"  CAGR: {cagr:.4%}")
        print(f"  Max Drawdown: {max_dd:.4%}")
        print(f"  Sharpe Ratio: {sharpe:.4f}")
        print(f"  Sample Days: {sample_days}")
        
        cur.execute("""
            INSERT INTO bot_leaderboard_metrics (
                bot_id, data_source, cagr, max_drawdown, sharpe, sample_days, last_calculated_at, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (bot_id, data_source) DO UPDATE SET
                cagr = EXCLUDED.cagr,
                max_drawdown = EXCLUDED.max_drawdown,
                sharpe = EXCLUDED.sharpe,
                sample_days = EXCLUDED.sample_days,
                last_calculated_at = EXCLUDED.last_calculated_at,
                updated_at = EXCLUDED.updated_at;
        """, (
            bot_id, 'DRY_RUN', cagr, max_dd, sharpe, sample_days, now, now, now
        ))
        
        conn.commit()
        print("Successfully seeded all trade logs, portfolios, and leaderboard metrics!")
    except Exception as e:
        conn.rollback()
        print(f"Transaction failed and was rolled back: {e}")
        sys.exit(1)
    finally:
        cur.close()

def list_users(conn):
    cur = conn.cursor()
    try:
        cur.execute("SELECT user_id, email, username FROM users ORDER BY email ASC")
        return cur.fetchall()
    except Exception as e:
        print(f"Error fetching users: {e}")
        sys.exit(1)
    finally:
        cur.close()

def insert_portfolio_data(conn, user_id, bot_id, closed_trades, initial_balance, config, portfolio_points=None):
    cur = conn.cursor()
    try:
        print(f"\nSeeding portfolio data for user {user_id} and bot {bot_id}...")
        now = datetime.now(timezone.utc)
        
        # Determine ws_token, exchange_id, execution_mode
        ws_token = config.get("SYSTEM_WS_TOKEN", "ws_default_token")
        exchange_id = config.get("EXCHANGE_ID", "binance").upper()
        execution_mode = config.get("EXECUTION_MODE", "live").upper()
        
        # Calculate final equity and PnL
        total_pnl = sum(t['pnl'] for t in closed_trades)

        if portfolio_points:
            normalized_points = _attach_cumulative_fees(_normalize_portfolio_points(portfolio_points), closed_trades)
            final_portfolios = normalized_points
            initial_balance = float(final_portfolios[0]['cash'])
            final_equity = float(final_portfolios[-1]['equity'])
            realized_pnl = float(final_portfolios[-1].get('realized_pnl', final_equity - initial_balance))
            unrealized_pnl = float(final_portfolios[-1].get('unrealized_pnl', 0.0))
            start_date = final_portfolios[0]['timestamp']
            last_sync_at = final_portfolios[-1]['timestamp']
        else:
            final_equity = initial_balance + total_pnl
            realized_pnl = total_pnl
            unrealized_pnl = 0.0

            if closed_trades:
                first_entry = min(t['entry_timestamp'] for t in closed_trades)
                start_date = first_entry - timedelta(seconds=1)
                last_sync_at = max(t['exit_timestamp'] for t in closed_trades)
            else:
                start_date = now
                last_sync_at = now

        # 1. Upsert ACTIVE subscription in subscriptions table
        cur.execute("SELECT user_subscription_id FROM subscriptions WHERE user_id = %s AND bot_id = %s AND status = 'ACTIVE';", (user_id, bot_id))
        row = cur.fetchone()
        if row:
            user_subscription_id = row[0]
            print(f"  Found existing subscription {user_subscription_id}, updating ws_token...")
            cur.execute("""
                UPDATE subscriptions 
                SET ws_token = %s, executor_connected = true, updated_at = %s
                WHERE user_subscription_id = %s;
            """, (ws_token, now, user_subscription_id))
        else:
            user_subscription_id = "sub_" + str(uuid.uuid4()).replace("-", "")[:28]
            print(f"  Creating new subscription {user_subscription_id}...")
            cur.execute("""
                INSERT INTO subscriptions (
                    id, user_id, bot_id, user_subscription_id, ws_token, status, 
                    executor_connected, start_date, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                str(uuid.uuid4()), user_id, bot_id, user_subscription_id, ws_token, 'ACTIVE',
                True, start_date, now, now
            ))

        # 2. Upsert account details in portfolio_accounts table
        cur.execute("SELECT id FROM portfolio_accounts WHERE user_subscription_id = %s;", (user_subscription_id,))
        row = cur.fetchone()
        if row:
            account_id = row[0]
            print(f"  Updating portfolio account {account_id}...")
            cur.execute("""
                UPDATE portfolio_accounts
                SET total = %s, free = %s, used = 0, realized_pnl = %s, unrealized_pnl = %s,
                    last_sync_at = %s, updated_at = %s, is_active = true
                WHERE id = %s;
            """, (final_equity, final_equity, realized_pnl, unrealized_pnl, last_sync_at, now, account_id))
        else:
            account_id = str(uuid.uuid4())
            print(f"  Creating new portfolio account {account_id}...")
            cur.execute("""
                INSERT INTO portfolio_accounts (
                    id, user_id, user_subscription_id, bot_id, ws_token, exchange_id, currency,
                    execution_mode, total, free, used, realized_pnl, unrealized_pnl, last_sync_at,
                    is_active, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 0, %s, 0, %s, true, %s, %s);
            """, (
                account_id, user_id, user_subscription_id, bot_id, ws_token, exchange_id, 'USDT',
                execution_mode, final_equity, final_equity, realized_pnl, last_sync_at, now, now
            ))

        # 3. Generate Equity Curve history points
        if not portfolio_points:
            portfolios = []
            portfolios.append({
                'timestamp': start_date,
                'cash': initial_balance,
                'equity': initial_balance,
                'realized_pnl': 0.0,
                'unrealized_pnl': 0.0,
            })

            current_equity = initial_balance
            cumulative_pnl = 0.0
            for trade in closed_trades:
                cumulative_pnl += trade['pnl']
                current_equity += trade['pnl']
                portfolios.append({
                    'timestamp': trade['exit_timestamp'],
                    'cash': current_equity,
                    'equity': current_equity,
                    'realized_pnl': cumulative_pnl,
                    'unrealized_pnl': 0.0,
                })

            final_portfolios = _normalize_portfolio_points(portfolios)

        # 4. Seed portfolio_balance_history (purge first)
        print(f"  Seeding portfolio_balance_history ({len(final_portfolios)} points)...")
        cur.execute("DELETE FROM portfolio_balance_history WHERE user_subscription_id = %s;", (user_subscription_id,))
        for p in final_portfolios:
            cur.execute("""
                INSERT INTO portfolio_balance_history (
                    id, created_at, updated_at, exchange_id, free, snapshot_at, total, unrealized_pnl, used,
                    user_id, user_subscription_id, bot_id, currency, execution_mode, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 0, %s, %s, %s, %s, %s, true);
            """, (
                str(uuid.uuid4()), now, now, exchange_id, p['equity'], p['timestamp'], p['equity'],
                p.get('unrealized_pnl', 0.0),
                user_id, user_subscription_id, bot_id, 'USDT', execution_mode
            ))

        # 5. Seed portfolio_aggregate_history (purge first)
        print(f"  Seeding portfolio_aggregate_history ({len(final_portfolios)} points)...")
        cur.execute("DELETE FROM portfolio_aggregate_history WHERE user_id = %s;", (user_id,))
        for p in final_portfolios:
            cur.execute("""
                INSERT INTO portfolio_aggregate_history (
                    id, user_id, total, free, used, realized_pnl, unrealized_pnl,
                    fresh_accounts_count, stale_accounts_count, data_freshness, exchange_id,
                    snapshot_at, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, 0, %s, %s, 1, 0, 'FRESH', %s, %s, %s, %s);
            """, (
                str(uuid.uuid4()), user_id, p['equity'], p['equity'], p.get('realized_pnl', 0.0), p.get('unrealized_pnl', 0.0),
                exchange_id, p['timestamp'], now, now
            ))

        # 6. Upsert aggregate user state in user_portfolios
        cur.execute("SELECT id FROM user_portfolios WHERE user_id = %s;", (user_id,))
        row = cur.fetchone()
        if row:
            print("  Updating user_portfolios aggregate stats...")
            cur.execute("""
                UPDATE user_portfolios
                SET total_capital = %s, available_balance = %s, realized_pnl = %s, unrealized_pnl = %s,
                    exchange_id = %s, last_sync_at = %s, fresh_accounts_count = 1, stale_accounts_count = 0,
                    data_freshness = 'FRESH', updated_at = %s
                WHERE user_id = %s;
            """, (final_equity, final_equity, realized_pnl, unrealized_pnl, exchange_id, last_sync_at, now, user_id))
        else:
            print("  Creating user_portfolios row...")
            cur.execute("""
                INSERT INTO user_portfolios (
                    id, user_id, total_capital, available_balance, realized_pnl, unrealized_pnl,
                    max_drawdown_threshold, medium_risk_threshold, exchange_id, last_sync_at,
                    fresh_accounts_count, stale_accounts_count, data_freshness, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, 0, 0.1000, 0.0500, %s, %s, 1, 0, 'FRESH', %s, %s);
            """, (
                str(uuid.uuid4()), user_id, final_equity, final_equity, realized_pnl,
                unrealized_pnl, exchange_id, last_sync_at, now, now
            ))

        conn.commit()
        print("Successfully seeded all portfolio accounts, histories, and user aggregates!")
    except Exception as e:
        conn.rollback()
        print(f"Portfolio seeding failed and was rolled back: {e}")
        sys.exit(1)
    finally:
        cur.close()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Marcus Trading - Exchange History Synchronizer & Seeder")
    parser.add_argument("--bot-id", type=str, help="Direct Bot ID (e.g., bot_...)")
    parser.add_argument("--bot-name", type=str, default=None, help="Bot name hint used to auto-select a bot when bot-id is not provided")
    parser.add_argument("--symbols", type=str, help="Comma-separated symbols to sync (e.g., BTC/USDT)")
    parser.add_argument("--days", type=str, help="Days of history to fetch (default: 30)")
    parser.add_argument("--initial-cash", type=float, help="Initial cash balance (default: 10000.0)")
    parser.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
    parser.add_argument("--mock", action="store_true", help="Generate mock trades history instead of fetching from exchange")
    parser.add_argument("--dashboard-live", action="store_true", help="Seed portfolio tables from the remote pnl_dashboard SQLite snapshot")
    parser.add_argument("--dashboard-start", type=str, default=None, help="Dashboard snapshot start timestamp (default: 2026-05-01 00:00:00)")
    parser.add_argument("--dashboard-end", type=str, default=None, help="Dashboard snapshot end timestamp (optional)")
    parser.add_argument("--trader-email", type=str, default="demo-trader@gmail.com", help="Trader account email used for portfolio seeding")
    parser.add_argument("--dev-email", type=str, default="demo-dev@gmail.com", help="Developer email used as bot selection hint")
    
    args = parser.parse_args()

    if args.mock and args.dashboard_live:
        print("Error: --mock and --dashboard-live cannot be used together.")
        sys.exit(1)
    
    print("=" * 60)
    print("  MARCUS TRADING - EXCHANGE HISTORY SYNCHRONIZER & SEEDER")
    print("=" * 60)
    
    config = load_config()
    conn = get_db_connection(config)
    
    # 1. Select bot
    users = list_users(conn)
    developer_user = select_user_by_email(users, args.dev_email)
    developer_user_id = developer_user[0] if developer_user else None

    bots = list_bots(conn)
    if not bots:
        print("No active bots found in signal_db.")
        sys.exit(1)
        
    selected_bot_id = select_bot_from_catalog(
        bots,
        bot_id=args.bot_id,
        bot_name_hint=args.bot_name,
        dev_email=args.dev_email,
        developer_user_id=developer_user_id,
    )
    default_symbol = "BTC/USDT"

    # If bot_id was provided, or we resolved from hints, look up its trading pair
    if selected_bot_id:
        bot_found = False
        for bot_id, name, pair, status, developer_id in bots:
            if bot_id == selected_bot_id:
                bot_found = True
                if pair:
                    default_symbol = pair
                print(f"Selected Bot: {selected_bot_id} ({name})")
                break
        if not bot_found:
            print(f"Warning: Bot ID '{selected_bot_id}' not found in the database. Proceeding anyway.")
            print(f"Selected Bot: {selected_bot_id}")
    else:
        if args.yes:
            print(f"Error: No bot matched dev email/hint '{args.dev_email}'. Provision the bot first or pass --bot-id.")
            sys.exit(1)
        print("\nAvailable Bots:")
        for idx, (bot_id, name, pair, status, developer_id) in enumerate(bots, 1):
            print(f"  [{idx}] {name} ({bot_id}) | Pair: {pair} | Status: {status}")
            
        bot_choice = input(f"\nSelect a bot (1-{len(bots)}) or enter bot_id manually: ").strip()
        if bot_choice.isdigit():
            choice_idx = int(bot_choice) - 1
            if 0 <= choice_idx < len(bots):
                selected_bot_id = bots[choice_idx][0]
                if bots[choice_idx][2]:
                    default_symbol = bots[choice_idx][2]
        else:
            for bot_id, name, pair, status, developer_id in bots:
                if bot_id == bot_choice:
                    selected_bot_id = bot_choice
                    if pair:
                        default_symbol = pair
                    break

        if not selected_bot_id:
            print("Invalid bot selection.")
            sys.exit(1)

        print(f"Selected Bot: {selected_bot_id}")
    
    # 2. Enter symbols
    symbols_input = args.symbols
    if symbols_input is None:
        if args.yes:
            symbols_input = ""
        else:
            symbols_input = input(f"Enter trading symbol(s) to fetch from the exchange (comma-separated, press enter to auto-discover all traded symbols): ").strip()
        
    if symbols_input:
        symbols = [s.strip() for s in symbols_input.split(",") if s.strip()]
    else:
        symbols = []
        
    # 3. Enter days
    days_input = args.days
    if not days_input:
        if args.yes:
            days_input = "30"
        else:
            days_input = input("Enter days history to sync (e.g. 30, default 30): ").strip()
            if not days_input:
                days_input = "30"
            
    since_days = 30
    if days_input:
        if days_input.lower() == 'all':
            since_days = None
        elif days_input.isdigit():
            since_days = int(days_input)
            
    # Fetch exchange trades and real balance
    trades, real_balance = fetch_trades_from_exchange(config, symbols, since_days, mock=args.mock)
    if not trades:
        print("No trades returned from the exchange.")
        sys.exit(0)
        
    print(f"\nFetched {len(trades)} fills from the exchange.")
    print("Running FIFO matching algorithm...")
    closed_trades = fifo_match_trades(trades)
    
    if not closed_trades:
        print("No completed (closed) trades could be matched from the retrieved fills.")
        sys.exit(0)
        
    print(f"Successfully matched {len(closed_trades)} round-trip closed trades.")

    dashboard_portfolio_points = None
    if args.dashboard_live:
        dashboard_start = args.dashboard_start or config["DASHBOARD_START"]
        dashboard_portfolio_points = fetch_dashboard_portfolio_points(
            config,
            dashboard_start=dashboard_start,
            dashboard_end=args.dashboard_end,
        )
        print(f"Loaded {len(dashboard_portfolio_points)} dashboard portfolio snapshots.")
        if dashboard_portfolio_points:
            dashboard_initial_cash = float(dashboard_portfolio_points[0]["cash"])
            dashboard_final_equity = float(dashboard_portfolio_points[-1]["equity"])
            dashboard_final_realized = float(dashboard_portfolio_points[-1].get("realized_pnl", 0.0))
            dashboard_final_unrealized = float(dashboard_portfolio_points[-1].get("unrealized_pnl", 0.0))
            print(f"Dashboard window: {dashboard_portfolio_points[0]['timestamp']} -> {dashboard_portfolio_points[-1]['timestamp']}")
            print(f"Dashboard initial cash: {dashboard_initial_cash:.4f}")
            print(f"Dashboard final equity: {dashboard_final_equity:.4f}")
            print(f"Dashboard final realized pnl: {dashboard_final_realized:.4f}")
            print(f"Dashboard final unrealized pnl: {dashboard_final_unrealized:.4f}")
        else:
            print("No dashboard portfolio points were loaded.")

        if dashboard_portfolio_points:
            trade_total_pnl = sum(trade["pnl"] for trade in closed_trades)
            drift = abs(trade_total_pnl - float(dashboard_portfolio_points[-1].get("realized_pnl", 0.0)))
            if drift > float(config["DASHBOARD_MAX_DRIFT_USDT"]):
                print(
                    f"Warning: trade PnL drift vs dashboard realized PnL is {drift:.4f} USDT "
                    f"(threshold {config['DASHBOARD_MAX_DRIFT_USDT']:.4f})."
                )
    
    # 4. Determine initial cash balance
    if dashboard_portfolio_points:
        initial_balance = float(dashboard_portfolio_points[0]["cash"])
        print(f"Using dashboard initial cash balance: {initial_balance:.4f}")
    elif args.mock:
        initial_balance = args.initial_cash
        if initial_balance is None:
            cash_input = input("Enter initial cash balance (default 10000): ").strip()
            initial_balance = 10000.0
            if cash_input:
                try:
                    initial_balance = float(cash_input)
                except ValueError:
                    print("Invalid cash input, using 10000.0")
    else:
        # Calculate start balance: real_balance - total_pnl
        total_pnl = sum(t['pnl'] for t in closed_trades)
        if real_balance and real_balance > 0.0:
            initial_balance = real_balance - total_pnl
            print(f"Fetched real account balance from exchange: {real_balance:.4f}")
            print(f"Total net PnL of matched trades: {total_pnl:.4f}")
            print(f"Computed initial equity balance: {initial_balance:.4f}")
        else:
            print("Warning: Real account balance could not be fetched or is zero.")
            initial_balance = args.initial_cash
            if initial_balance is None:
                cash_input = input("Enter initial cash balance (default 10000): ").strip()
                initial_balance = 10000.0
                if cash_input:
                    try:
                        initial_balance = float(cash_input)
                    except ValueError:
                        print("Invalid cash input, using 10000.0")
                        
    # Print sample trades
    print("\nSample Matched Trades:")
    for t in closed_trades[:5]:
        print(f"  {t['side']} {t['symbol']} | Qty: {t['quantity']} | Entry: {t['entry_price']} | Exit: {t['exit_price']} | PnL: {t['pnl']:.4f}")
    if len(closed_trades) > 5:
        print(f"  ... and {len(closed_trades) - 5} more.")
        
    # 5. Select user profile for client portfolio seeding
    selected_user_id = None
    selected_user_email = None
    if users:
        requested_user = select_user_by_email(users, args.trader_email)
        if requested_user:
            selected_user_id, selected_user_email = requested_user
            print(f"Selected trader profile: {selected_user_email}")
        elif args.yes:
            print(f"Error: trader email '{args.trader_email}' not found in local DB. Provision that account first.")
            sys.exit(1)
        else:
            print("\nAvailable User Profiles for Portfolio Seeding:")
            default_idx = 1
            for idx, (user_id, email, username) in enumerate(users, 1):
                if email.lower() == args.trader_email.lower():
                    default_idx = idx
                print(f"  [{idx}]{' (DEFAULT)' if email.lower() == args.trader_email.lower() else ''} {email} ({user_id})")

            user_choice = input(f"Select user (1-{len(users)}, default [{default_idx}]): ").strip()
            if not user_choice:
                selected_user_id = users[default_idx - 1][0]
                selected_user_email = users[default_idx - 1][1]
            elif user_choice.isdigit() and 1 <= int(user_choice) <= len(users):
                selected_user_id = users[int(user_choice) - 1][0]
                selected_user_email = users[int(user_choice) - 1][1]
            else:
                for user_id, email, username in users:
                    if user_choice in (user_id, email):
                        selected_user_id = user_id
                        selected_user_email = email
                        break

    if not selected_user_id:
        print("Warning: No user profile selected or available. Client portfolio seeding will be skipped.")

    if args.yes:
        confirm = 'y'
    else:
        confirm = input("\nProceed to seed database? WARNING: This will overwrite dry-run and execution history for this bot! (y/n): ").strip().lower()
        
    if confirm in ('y', 'yes'):
        insert_trade_data(conn, selected_bot_id, closed_trades, initial_balance, portfolio_points=dashboard_portfolio_points)
        if selected_user_id:
            insert_portfolio_data(conn, selected_user_id, selected_bot_id, closed_trades, initial_balance, config, portfolio_points=dashboard_portfolio_points)
    else:
        print("Aborted Seeding.")
        
    conn.close()

if __name__ == "__main__":
    main()
