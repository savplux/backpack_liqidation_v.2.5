import logging
import time
import random
import sys
from pathlib import Path
import yaml
import math
import traceback
import threading

# Добавить импорт модуля обработки прокси
from proxy_handler import patch_sdk_clients, init_proxy_from_config

from backpack_exchange_sdk.authenticated import AuthenticationClient
from backpack_exchange_sdk.public import PublicClient
import colorlog

import os, csv, time, logging
from collections import deque
from datetime import datetime

# Отключение root логгера полностью
logging.getLogger().handlers = []

# --- FluctuationMonitor: собирает последние N секунд цен и считает дельту ---
class FluctuationMonitor:
    def __init__(self, window_seconds: int):
        self.window = window_seconds
        self.prices = deque()  # (timestamp, price)

    def add_price(self, price: float):
        now = time.time()
        self.prices.append((now, price))
        cutoff = now - self.window
        while self.prices and self.prices[0][0] < cutoff:
            self.prices.popleft()

    def get_relative_delta(self) -> float:
        # Абсолютный разброс (max–min) цен в USDC за window_seconds.
        if not self.prices:
            return 0.0
        vals = [p for _, p in self.prices]
        return max(vals) - min(vals)

# --- CSV-лог для офлайн-анализа ---
LOG_FILE = 'fluctuation_log.csv'
if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow([
            'timestamp',
            'window_seconds',   # новый столбец
            'start_price',      # цена в начале окна
            'end_price',        # цена в конце окна
            'delta',            # абсолютная дельта
            'level','offset',
            'tp_short','tp_long'
        ])

def pair_worker(pair: dict, cfg: dict):
    short_name = pair["short_account"]["name"]
    long_name  = pair["long_account"]["name"]
    delay_max  = float(cfg.get("pair_start_delay_max", 0))
    
    # Добавляем счетчик циклов для этой пары
    cycle_counter = 0
    # Извлекаем номер пары из конфигурации или устанавливаем по индексу
    pair_number = pair.get("pair_number", cfg["pairs"].index(pair) + 1)
   
    # начальная задержка перед первым циклом (без cycle_wait_time)
    if delay_max > 0:
        initial_delay = random.uniform(0, delay_max)
        logging.info(f"Pair {short_name}/{long_name}: initial delay {initial_delay:.2f}s")
        time.sleep(initial_delay)

    # бесконечный цикл работы этой пары
    while True:
        # Увеличиваем счетчик циклов при каждой итерации
        cycle_counter += 1
        logging.info(f"Pair {short_name}/{long_name}: starting new cycle #{cycle_counter}")
        try:
            success = process_pair(pair, cfg, pair_number, cycle_counter)
            if not success:
                logging.warning(f"Pair {short_name}/{long_name}: cycle #{cycle_counter} finished with errors")
        except Exception as e:
            logging.error(f"Pair {short_name}/{long_name}: exception in cycle #{cycle_counter}: {e}", exc_info=True)

        window = int(cfg['fluctuation']['window_seconds'])
        logging.info(f"Pair {short_name}/{long_name}: sleeping {window}s until next cycle")  # <-- ДО sleep
        time.sleep(window)

# -------------------- Logger Setup --------------------
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)
log_filename = f"backpack_liquidation_{time.strftime('%Y%m%d_%H%M%S')}.log"
log_path = logs_dir / log_filename

console_handler = colorlog.StreamHandler()
console_handler.setFormatter(
    colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(levelname)s - %(message)s',
        log_colors={'DEBUG':'cyan','INFO':'green','WARNING':'yellow','ERROR':'red','CRITICAL':'red,bg_white'}
    )
)
file_handler = logging.FileHandler(log_path)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger = colorlog.getLogger()
logger.addHandler(console_handler)
logger.addHandler(file_handler)
logger.setLevel(logging.INFO)

import glob
import re

# Поиск всех существующих файлов pnl_log
pnl_log_files = glob.glob('pnl_log_*.csv')
next_num = 1

if pnl_log_files:
    # Извлекаем числа из имен файлов
    numbers = []
    for filename in pnl_log_files:
        match = re.search(r'pnl_log_(\d+)\.csv', filename)
        if match:
            numbers.append(int(match.group(1)))
    
    if numbers:
        next_num = max(numbers) + 1

# Создаем новый файл с инкрементным номером
PNL_LOG_FILE = f'pnl_log_{next_num}.csv'
with open(PNL_LOG_FILE, 'w', newline='') as f:
    w = csv.writer(f)
    w.writerow([
        'pair_number',
        'cycle_number',
        'pnl_short',
        'pnl_long',
        'total_fees_short',
        'total_fees_long',
        'total_pnl'
    ])
logger.info(f"Created new PnL log file: {PNL_LOG_FILE}")

# -------------------- Trader Wrapper --------------------
class BackpackTrader:
    def __init__(self, api_key: str, api_secret: str):
        self.auth = AuthenticationClient(api_key, api_secret)
        self.pub = PublicClient()
        
        # Логирование информации о прокси (если используется)
        if hasattr(self.auth, 'session') and self.auth.session.proxies:
            proxy_info = next(iter(self.auth.session.proxies.values()), None)
            if proxy_info:
                # Извлекаем только домен и порт для логирования (без логина/пароля)
                safe_proxy = proxy_info.split('@')[-1] if '@' in proxy_info else proxy_info
                logging.info(f"{self.__class__.__name__} initialized with proxy: {safe_proxy}")

    def get_order_book(self, symbol: str) -> dict:
        try:
            return self.pub.get_depth(symbol)
        except Exception as e:
            logging.error(f"Failed to get order book for {symbol}: {e}")
            return {"asks": [], "bids": []}

    def place_limit_order(self, symbol: str, side: str, price: float, quantity: float, reduce_only: bool=False) -> dict:
        try:
            price_str = f"{price:.2f}"
            resp = self.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
            markets = resp.get("data", resp) if isinstance(resp, dict) else resp
            step = next((float(m.get("baseIncrement", 0.01)) for m in markets if m.get("symbol")==symbol), 0.01)
            decimals = len(str(step).split('.')[-1])
            qty_str = f"{quantity:.{decimals}f}"
            logging.info(f"Placing Limit Order: {side} {symbol} @ price={price_str}, qty={qty_str}, reduceOnly={reduce_only}")
            
            return self.auth.execute_order(
                orderType="Limit",
                side=side,
                symbol=symbol,
                price=price_str,
                quantity=qty_str,
                reduceOnly=reduce_only,
                timeInForce="GTC",
                autoBorrow=True,
                autoBorrowRepay=True,
                autoLend=True,
                autoLendRedeem=True,
                selfTradePrevention="RejectTaker"
            )
        except Exception as e:
            logging.error(f"Failed to place limit order: {e}")
            return {"error": str(e)}
            
    def place_market_order(self, symbol: str, side: str, quote_quantity: float) -> dict:
        try:
            qty = round(quote_quantity, 2)
            qty_str = f"{qty:.2f}"
            logging.info(f"Placing Market Order: {side} {symbol} quoteQuantity={qty_str}")
            return self.auth.execute_order(
                orderType="Market",
                side=side,
                symbol=symbol,
                quoteQuantity=qty_str,
                autoBorrow=True,
                autoBorrowRepay=True,
                autoLend=True,
                autoLendRedeem=True,
                selfTradePrevention="RejectTaker"
            )
        except Exception as e:
            logging.error(f"Failed to place market order: {e}")
            return {"error": str(e)}

    def place_take_profit_order(self, symbol: str, side: str, stop_price: float, quantity: float = None) -> dict:
        """Place a limit order with reduceOnly=True to function as a take-profit order."""
        try:
            price_str = f"{stop_price:.2f}"
            logging.info(f"Placing Take Profit Limit Order: {side} {symbol} @ price={price_str}")
            
            # Get decimals for quantity formatting
            resp = self.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
            markets = resp.get("data", resp) if isinstance(resp, dict) else resp
            step = next((float(m.get("baseIncrement", 0.01)) for m in markets if m.get("symbol")==symbol), 0.01)
            decimals = len(str(step).split('.')[-1])
            
            # Format quantity if provided
            qty_str = None
            if quantity is not None:
                qty_str = f"{quantity:.{decimals}f}"
                logging.info(f"Using quantity {qty_str} for take profit order")
            
            # Use place_limit_order with reduceOnly=True
            return self.place_limit_order(
                symbol=symbol,
                side=side,
                price=stop_price,
                quantity=quantity,
                reduce_only=True
            )
        except Exception as e:
            logging.error(f"Failed to place take profit order: {e}")
            return {"error": str(e)}

    def cancel_order(self, symbol: str, order_id: str) -> dict:
        """Fixed method to cancel an order"""
        try:
            logging.info(f"Cancelling order {order_id} for {symbol}")
            return self.auth._send_request(
                "DELETE",
                "api/v1/order",
                "orderCancel",
                {"symbol": symbol, "orderId": order_id}
            )
        except Exception as e:
            logging.error(f"Failed to cancel order: {e}")
            return {"error": str(e)}

    def check_order_status(self, symbol: str, order_id: str) -> str:
        try:
            resp = self.auth._send_request(
                "GET",
                "api/v1/order",
                "orderQuery",
                {"symbol": symbol, "orderId": order_id}
            )
            status = ""
            if isinstance(resp, dict):
                status = resp.get("status") or resp.get("data", {}).get("status", "")
            logging.info(f"Order status for {order_id}: {status}")
            return status
        except Exception as e:
            if "RESOURCE_NOT_FOUND" in str(e):
                # This could mean the order was filled and is no longer active
                # Check position to confirm
                pos = self.get_position(symbol)
                if pos and float(pos.get("netQuantity", 0)) != 0:
                    logging.info(f"Order {order_id} not found but position exists - likely filled")
                    return "FILLED"
            logging.warning(f"Failed to check order status: {e}")
            return "ERROR"

    def get_position(self, symbol: str) -> dict:
        try:
            resp = self.auth._send_request("GET", "api/v1/position", "positionQuery", {})
            positions = resp.get("data", resp) if isinstance(resp, dict) else resp
            
            if not positions or not isinstance(positions, list):
                logging.debug(f"No positions data returned or invalid format: {positions}")
                # Don't return empty dict immediately, retry once
                time.sleep(1)
                try:
                    resp = self.auth._send_request("GET", "api/v1/position", "positionQuery", {})
                    positions = resp.get("data", resp) if isinstance(resp, dict) else resp
                    if not positions or not isinstance(positions, list):
                        logging.debug(f"Still no positions data after retry: {positions}")
                        return {}
                except Exception:
                    return {}
                
            for pos in positions:
                sym = pos.get("symbol", "")
                if sym == symbol or sym.replace("_","-") == symbol or sym.replace("-","_") == symbol:
                    return pos
            
            # No position found for symbol
            # This is normal if no position exists, so use debug level
            logging.debug(f"No position found for {symbol}")
            return {}
        except Exception as e:
            logging.error(f"Failed to get position: {e}")
            return {}

    def get_available_margin(self) -> float:
        max_attempts = 5
        for i in range(1, max_attempts+1):
            try:
                resp = self.auth._send_request("GET", "api/v1/capital/collateral", "collateralQuery", {})
                data = resp.get("data", resp) if isinstance(resp, dict) else resp
                items = data.get("collateral", data) if isinstance(data, dict) else data
                
                if not items or not isinstance(items, list):
                    logging.warning(f"No margin data returned or invalid format: {items}")
                    continue
                    
                for itm in items:
                    if itm.get("symbol") == "USDC":
                        margin = float(itm.get("availableQuantity", 0) or 0)
                        logging.info(f"Available margin: {margin} USDC (attempt {i}/{max_attempts})")
                        return margin
                return 0.0
            except Exception as e:
                logging.warning(f"Margin fetch attempt {i} error: {e}")
                time.sleep(1)
        logging.error("Failed to fetch margin, returning 0")
        return 0.0
# -------------------- Core Logic --------------------
def process_pair(pair_cfg: dict, cfg: dict, pair_number: int, cycle_number: int) -> bool:
    try:
        # Извлечение основных параметров
        symbol          = cfg["symbol"]
        leverage        = float(cfg.get("leverage", 1))
        maker_off_short = float(cfg.get("maker_offset", {}).get("short", 0.0005))
        maker_off_long  = float(cfg.get("maker_offset", {}).get("long",  0.0005))
        limit_to        = int(cfg.get("limit_order_timeout", 30))
        retries         = int(cfg.get("limit_order_retries", 10))
        gmin            = float(cfg.get("general_delay", {}).get("min", 0))
        gmax            = float(cfg.get("general_delay", {}).get("max", 0))
        sweep_tries     = int(cfg.get("sweep_attempts", 8))
        initial_dep     = float(cfg.get("initial_deposit", 0))
        
        # === ИНИЦИАЛИЗАЦИЯ ТРЕЙДЕРОВ И ИМЁН ===
        sa_cfg = pair_cfg["short_account"]
        la_cfg = pair_cfg["long_account"]
        short_tr = BackpackTrader(sa_cfg["api_key"], sa_cfg["api_secret"])
        long_tr  = BackpackTrader(la_cfg["api_key"], la_cfg["api_secret"])
        short_acc_name = sa_cfg.get("name", "ShortAccount")
        long_acc_name  = la_cfg.get("name", "LongAccount")

        try:
            # Функция депозита в фоне
            def do_deposits():
                parent = AuthenticationClient(cfg["api"]["key"], cfg["api"]["secret"])
                for side in ("short_account", "long_account"):
                    acc = pair_cfg[side]
                    acc_name = acc["name"]
                    
                    # Точная сумма депозита из конфига
                    deposit_amount = initial_dep
                    qty_str = f"{deposit_amount:.6f}"
                    
                    logging.info(f"[Deposit] {acc_name}: Starting deposit of EXACTLY {qty_str} USDC")
                    
                    for attempt in range(1, sweep_tries + 1):
                        try:
                            # Проверяем баланс родительского аккаунта для уверенности
                            parent_balance = 0
                            try:
                                resp = parent._send_request("GET", "api/v1/capital/collateral", "collateralQuery", {})
                                data = resp.get("data", resp) if isinstance(resp, dict) else resp
                                items = data.get("collateral", data) if isinstance(data, dict) else data
                                
                                for item in items:
                                    if item.get("symbol") == "USDC":
                                        parent_balance = float(item.get("availableQuantity", 0) or 0)
                                        break
                                
                                logging.info(f"[Deposit] Parent account balance: {parent_balance} USDC")
                                
                                # Если баланс родительского аккаунта меньше запрашиваемого депозита
                                if parent_balance < deposit_amount:
                                    logging.warning(f"[Deposit] Insufficient parent balance: {parent_balance} USDC, needed: {deposit_amount} USDC")
                                    logging.warning(f"[Deposit] Will use available balance: {parent_balance} USDC")
                                    deposit_amount = parent_balance
                                    qty_str = f"{deposit_amount:.6f}"
                            except Exception as e:
                                logging.warning(f"[Deposit] Failed to check parent balance: {e}")
                            
                            # Выполняем перевод с точной суммой
                            result = parent.request_withdrawal(
                                address=acc["address"], 
                                blockchain="Solana",
                                quantity=qty_str, 
                                symbol="USDC"
                            )
                            
                            # Проверяем результат
                            if isinstance(result, dict):
                                actual_amount = result.get("quantity", "unknown")
                                tx_id = result.get("id", "unknown")
                                status = result.get("status", "unknown")
                                
                                logging.info(f"[Deposit] {acc_name}: Deposit details:"
                                        f"\n- Requested: {qty_str} USDC"
                                        f"\n- Actual: {actual_amount} USDC"
                                        f"\n- ID: {tx_id}"
                                        f"\n- Status: {status}")
                                
                                # Если суммы не совпадают, выводим предупреждение
                                if str(actual_amount) != qty_str:
                                    logging.warning(f"[Deposit] {acc_name}: AMOUNT MISMATCH! Requested: {qty_str}, Actual: {actual_amount}")
                            else:
                                logging.info(f"[Deposit] {acc_name}: deposited {qty_str} USDC, unexpected response format: {result}")
                            
                            # Задержка между депозитами с логированием
                            sleep_time = random.uniform(gmin, gmax)
                            logging.info(f"[Deposit] {acc_name}: sleeping for {sleep_time:.2f}s before next action")
                            time.sleep(sleep_time)
                            
                            break  # успешный депозит → выходим из попыток
                        except Exception as e:
                            logging.warning(f"[Deposit] {acc_name}: attempt {attempt} failed: {e}")
                            traceback.print_exc()  # Печатаем полный стектрейс для отладки
                            time.sleep(1)

            # Запуск депозита в отдельном потоке (не ждем его завершения)
            deposit_thread = threading.Thread(target=do_deposits, daemon=True)
            deposit_thread.start()

            # Измерение цены для market_dir - фиксированное время
            md_window = int(cfg['fluctuation']['window_seconds'])
            fm = FluctuationMonitor(md_window)

            # создаём объект PublicClient один раз
            pub = PublicClient()
            logging.info(f"[MarketDir] Starting price measurement window: {md_window} seconds")
            
            # Начинаем измерения, сохраняем стартовое время
            start_time = time.time()
            
            # Цикл на точно md_window секунд
            while time.time() - start_time < md_window:
                try:
                    raw = pub.get_mark_price(symbol)
                    entry = raw[0] if isinstance(raw, list) else raw
                    price = float(entry.get("markPrice", 0))
                except Exception as e:
                    logging.warning(f"Failed to get mark price: {e}, trying ticker...")
                    try:
                        tick = pub.get_ticker(symbol)
                        tick = tick[0] if isinstance(tick, list) else tick
                        price = float(tick.get("lastPrice", 0))
                    except Exception as e2:
                        logging.error(f"Failed to get ticker price as well: {e2}")
                        price = 0  # в случае полной ошибки — ставим 0
                fm.add_price(price)
                
                # Делаем небольшие паузы, но следим за общим временем
                remain = md_window - (time.time() - start_time)
                if remain <= 0:
                    break
                time.sleep(min(1, remain))  # Пауза не более 1 секунды или оставшегося времени

            # После завершения измерения цены, ждём завершения депозитов если они еще не закончились
            logging.info(f"[MarketDir] Price measurement completed in {time.time() - start_time:.2f} seconds")
            if deposit_thread.is_alive():
                logging.info("[MarketDir] Waiting for deposits to complete...")
                deposit_thread.join()
            else:
                logging.info("[MarketDir] Deposits already completed")

            # Вычисление направления рынка
            if len(fm.prices) < 2:
                logging.warning("[MarketDir] Not enough price samples collected, defaulting to 'flat'")
                market_dir = 'flat'
            else:
                start_price = round(fm.prices[0][1], 2)
                end_price   = round(fm.prices[-1][1], 2)
                delta       = end_price - start_price
                if delta < 0:
                    market_dir = 'down'
                elif delta > 0:
                    market_dir = 'up'
                else:
                    market_dir = 'flat'
                logging.info(f"[MarketDir] dir={market_dir} ({start_price}->{end_price})")

        except Exception as e:
            logging.error(f"Error in deposit+market measurement: {e}", exc_info=True)
            return False

        if market_dir == 'down':
            # === РЫНОК УПАЛ → открываем ЛОНГ по лимиту, затем ШОРТ по рынку ===
            # === 1.1) Maker limit order на LONG с обработкой частичных заполнений ===
            long_position_opened = False
            target_long_size = 0

            for attempt in range(1, retries + 1):
                # 1) Проверка на частично заполненную позицию
                current_pos = long_tr.get_position(symbol)
                current_size = abs(float(current_pos.get("netQuantity", 0))) if current_pos else 0

                if current_size > 0:
                    logging.info(f"{long_acc_name}: Current long size = {current_size} (partial fill)")
                    long_position_opened = True

                    # Первый раз — рассчитываем полный target с учётом уже заложенной маржи
                    if target_long_size == 0:
                        margin = long_tr.get_available_margin()
                        if margin <= 0:
                            logging.warning(f"{long_acc_name}: No margin available after partial fill")
                            break

                        # Получаем ордербук непосредственно от long_tr для текущей рыночной цены
                        ob = long_tr.get_order_book(symbol)
                        if not ob.get("bids") or len(ob.get("bids", [])) == 0:
                            logging.warning(f"{long_acc_name}: No bids in order book for size calculation")
                            break

                        # Берем лучший BID и применяем оффсет
                        best_bid = float(ob["bids"][0][0])
                        price = round(best_bid * (1 - maker_off_long), 2)

                        # шаг лота
                        resp_m = long_tr.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
                        mkts = resp_m.get("data", resp_m) if isinstance(resp_m, dict) else resp_m
                        step = next((float(m.get("baseIncrement", 0.01)) for m in mkts if m.get("symbol")==symbol), 0.01)
                        logging.info(f"[{long_acc_name}] Using baseIncrement: {step} for {symbol}")

                        step = 0.01
                        for m in mkts:
                            if m.get("symbol") == symbol:
                                base_increment = m.get("baseIncrement")
                                if base_increment:
                                    step = float(base_increment)
                                    logging.info(f"[{long_acc_name}] Found baseIncrement for {symbol}: {step}")
                                else:
                                    logging.warning(f"[{long_acc_name}] baseIncrement not found for {symbol}, using default 0.01")
                                break

                        notional = (margin + float(current_pos.get("initialMargin", 0))) * leverage
                        target_long_size = math.floor((notional / price) / step) * step
                        logging.info(f"{long_acc_name}: Calculated target long size = {target_long_size}")

                    # если уже закрыто >= target — выходим
                    if current_size >= target_long_size:
                        logging.info(f"{long_acc_name}: Long already {current_size}/{target_long_size} filled")
                        break

                    remaining_size = target_long_size - current_size
                else:
                    remaining_size = None

                # 2) Новый лимитный ордер для ЛОНГА

                # Получаем ордербук для текущей цены (как в коде для short)
                ob = long_tr.get_order_book(symbol)
                if not ob.get("asks") or len(ob.get("asks", [])) == 0:
                    logging.warning(f"[{long_acc_name}] No asks in order book for {symbol}, retrying...")
                    time.sleep(2)
                    continue
                    
                # Используем best_ask вместо best_bid, как в коде для short
                best_ask = float(ob.get("asks", [[0]])[0][0])
                if best_ask <= 0:
                    logging.warning(f"[{long_acc_name}] Invalid best ask price: {best_ask}, retrying...")
                    time.sleep(2)
                    continue
                    
                # Вычисляем цену, но с противоположным оффсетом (минус вместо плюс)
                # Для шорта: price = round(best_ask * (1 + maker_off_short), 2)
                # Для лонга: price = round(best_ask * (1 - maker_off_long), 2)
                price = round(best_ask * (1 - maker_off_long), 2)
                logging.info(f"[{long_acc_name}] Preparing to place LONG limit order at {price}")

                # Теперь рассчитываем шаг лота правильно от long_tr
                resp_m = long_tr.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
                mkts = resp_m.get("data", resp_m) if isinstance(resp_m, dict) else resp_m
                step = next((float(m.get("baseIncrement", 0.01)) for m in mkts if m.get("symbol")==symbol), 0.01)
                logging.info(f"[{long_acc_name}] Using baseIncrement: {step} for {symbol}")

                step = 0.01  # по дефолту
                for m in mkts:
                    if m.get("symbol") == symbol:
                        base_increment = m.get("baseIncrement")
                        if base_increment:
                            step = float(base_increment)
                            logging.info(f"[{long_acc_name}] Found baseIncrement for {symbol}: {step}")
                        else:
                            logging.warning(f"[{long_acc_name}] baseIncrement not found for {symbol}, using default 0.01")
                        break

                # Теперь рассчитываем размер позиции
                margin = long_tr.get_available_margin()
                if margin <= 0:
                    logging.warning(f"[{long_acc_name}] No margin available, retrying…")
                    time.sleep(2)
                    continue

                # Используем оставшуюся часть позиции, если это частичное заполнение
                if remaining_size is not None:
                    qty = remaining_size
                    logging.info(f"[{long_acc_name}] Using remaining size: {qty} for partial fill")
                else:
                    # Расчет для нового ордера
                    notional = margin * leverage
                    qty = math.floor((notional / price) / step) * step

                if qty <= 0:
                    logging.warning(f"[{long_acc_name}] Invalid qty={qty}, retrying…")
                    time.sleep(2)
                    continue

                # Ставим лимитный ордер
                order = long_tr.place_limit_order(symbol, "Bid", price, qty)
                if order.get("error"):
                    logging.error(f"{long_acc_name}: Failed to place long maker order: {order['error']}")
                    time.sleep(2)
                    continue

                oid = order.get("orderId") or order.get("data", {}).get("orderId") or order.get("id")
                if not oid:
                    logging.error(f"{long_acc_name}: No order ID returned for long limit order: {order}")
                    time.sleep(2)
                    continue
                    
                logging.info(f"{long_acc_name}: Long maker order {oid}@{price}, qty={qty}")

                # 3) Ожидаем заполнения — ускоренный вариант
                start_ts = time.time()
                order_filled = False

                # Интервал опроса в секундах (например, 0.2—0.5)
                poll_interval = cfg.get("order_poll_interval", 0.3)
                deadline = start_ts + limit_to

                while time.time() < deadline:
                    # 1) Сразу пробуем узнать статус ордера
                    status = long_tr.check_order_status(symbol, oid)
                    if status == "FILLED":
                        logging.info(f"{long_acc_name}: Order {oid} filled")
                        order_filled = True
                        long_position_opened = True
                        break

                    # 2) Проверяем изменение позиции (вдруг был частичный филл)
                    pos = long_tr.get_position(symbol)
                    new_size = abs(float(pos.get("netQuantity", 0))) if pos else 0
                    if new_size > current_size:
                        s_l = float(pos.get("netQuantity", 0))
                        e_l = float(pos.get("entryPrice", 0))
                        m_l = float(pos.get("markPrice", 0))
                        l_l = pos.get("estLiquidationPrice", "Unknown")
                        pnl_l = pos.get("unrealizedPnl", "---")
                        logging.info(
                            f"{long_acc_name}: {symbol}, "
                            f"size={s_l:.2f} (~{abs(s_l*e_l):.2f} USDC), "
                            f"entry={e_l}, mark={m_l}, liq={l_l}, PnL={pnl_l}"
                        )
                        if abs(new_size - qty) < step or new_size >= target_long_size:
                            logging.info(f"{long_acc_name}: Order fully filled")
                            order_filled = True
                        long_position_opened = True
                        break

                    # 3) Ждём перед следующим опросом
                    time.sleep(poll_interval)

                if not order_filled:
                    logging.warning(f"{long_acc_name}: Order {oid} not filled within {limit_to}s, proceeding with timeout handling")


                # 4) При таймауте — отменяем и проверяем частичное
                if not order_filled:
                    try:
                        long_tr.cancel_order(symbol, oid)
                        logging.info(f"{long_acc_name}: Cancelled order {oid}")
                    except Exception as e:
                        logging.error(f"{long_acc_name}: Cancel failed: {e}")

                    pos = long_tr.get_position(symbol)
                    if pos and abs(float(pos.get("netQuantity", 0))) > 0:
                        new_size = abs(float(pos.get("netQuantity", 0)))
                        if new_size >= target_long_size or attempt == retries:
                            long_position_opened = True
                            break

                        logging.info(f"{long_acc_name}: Partial fill detected: {new_size} units")
                        if attempt < retries:
                            logging.info(f"{long_acc_name}: Retrying to fill remaining size...")
                            continue

                    if long_position_opened and attempt == retries:
                        logging.info(f"{long_acc_name}: Final attempt reached with partial fill - continuing with current position")
                        break

                if order_filled or (long_position_opened and attempt == retries):
                    break

            if not long_position_opened:
                logging.error(f"{long_acc_name}: Failed to open long position after {retries} retries")
                return False
                
            # === 1.2) Immediate market order on SHORT - with retry for failures ===
            short_position_opened = False
            max_short_attempts = 3
            
            for short_attempt in range(1, max_short_attempts + 1):
                try:
                    margin_s = short_tr.get_available_margin()
                    if margin_s <= 0:
                        logging.error(f"{short_acc_name}: No margin available. Trying again in 5 seconds (attempt {short_attempt}/{max_short_attempts})")
                        time.sleep(5)
                        continue
                        
                    # Используем всю доступную маржу
                    notional_s = margin_s * leverage  # 100% доступной маржи
                    
                    if notional_s < 0.1:
                        logging.error(f"{short_acc_name}: Available margin too low: {margin_s} USDC, notional: {notional_s}. Retrying...")
                        time.sleep(5)
                        continue
                        
                    logging.info(f"{short_acc_name}: Placing market order with notional: {notional_s:.2f} from margin: {margin_s:.2f}")
                    short_order = short_tr.place_market_order(symbol, "Ask", notional_s)
                    
                    if short_order.get("error"):
                        logging.error(f"{short_acc_name}: Failed to place short order: {short_order.get('error')}. Retrying...")
                        time.sleep(5)
                        continue
                        
                    # Ждем обработки ордера
                    time.sleep(3)
                    
                    # Проверяем открытие позиции
                    for check in range(5):
                        pos_s = short_tr.get_position(symbol)
                        if pos_s and float(pos_s.get("netQuantity", 0)) != 0:
                            s_s = float(pos_s.get("netQuantity", 0))
                            e_s = float(pos_s.get("entryPrice", 0))
                            m_s = float(pos_s.get("markPrice", 0))
                            l_s = pos_s.get("estLiquidationPrice", "Unknown")
                            pnl_s = pos_s.get("unrealizedPnl", "---")
                            logging.info(f"{short_acc_name}: {symbol}, size={s_s:.2f} (~{abs(s_s*e_s):.2f} USDC), entry={e_s}, mark={m_s}, liq={l_s}, PnL={pnl_s}")
                            short_position_opened = True
                            break
                        else:
                            logging.warning(f"{short_acc_name}: Position not found after market order, checking again in {check+1} seconds...")
                            time.sleep(check + 1)
                    
                    if short_position_opened:
                        break
                    else:
                        logging.error(f"{short_acc_name}: Position not found after multiple checks. Retrying market order.")
                except Exception as e:
                    logging.error(f"{short_acc_name}: Error placing short market order (attempt {short_attempt}): {e}")
                    traceback.print_exc()
                    time.sleep(5)
                    
            if not short_position_opened:
                logging.error(f"{short_acc_name}: Failed to open short position after all attempts")
                # Пытаемся закрыть лонг, чтобы избежать несбалансированного риска
                try:
                    long_pos = long_tr.get_position(symbol)
                    if long_pos and float(long_pos.get("netQuantity", 0)) != 0:
                        logging.warning(f"{long_acc_name}: Closing long position because short position could not be opened")
                        long_qty = abs(float(long_pos.get("netQuantity", 0)))
                        # Маркет ордер на закрытие лонга
                        close_order = long_tr.place_market_order(symbol, "Ask", long_qty * float(long_pos.get("markPrice", 0)))
                        logging.info(f"{long_acc_name}: Closed long position: {close_order}")
                except Exception as e:
                    logging.error(f"Error closing long position: {e}")
                    
                return False
                
        else:  # market_dir == 'up' или market_dir == 'flat'
            # === РЫНОК ВЫРОС → открываем ШОРТ по лимиту, затем ЛОНГ по рынку ===
            # === 2.1) Limit order on SHORT with handling of partial fills ===
            short_position_opened = False
            target_short_size = 0  # Target size of short position

            for attempt in range(1, retries+1):
                try:
                    # Check if position already exists (partial fill from previous attempts)
                    current_pos = short_tr.get_position(symbol)
                    current_size = abs(float(current_pos.get("netQuantity", 0))) if current_pos else 0
                    
                    if current_size > 0:
                        logging.info(f"{short_acc_name}: Current position size: {current_size} (partial fill)")
                        short_position_opened = True
                        
                        # If target size not yet set, this is our first partial fill
                        if target_short_size == 0:
                            # Calculate original target size based on available margin
                            margin = short_tr.get_available_margin()
                            if margin <= 0:
                                logging.warning(f"{short_acc_name}: No margin available after partial fill")
                                break  # Use existing partial position
                                
                            # Get price information
                            ob = short_tr.get_order_book(symbol)
                            if not ob.get("asks") or len(ob.get("asks", [])) == 0:
                                logging.warning(f"{short_acc_name}: No order book data for size calculation")
                                break  # Use existing partial position
                                
                            best_ask = float(ob.get("asks", [[0]])[0][0])
                            price = round(best_ask * (1 + maker_off_short), 2)
                            
                            # Get step size
                            resp_m = short_tr.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
                            mkts = resp_m.get("data", resp_m) if isinstance(resp_m, dict) else resp_m
                            step = next((float(m.get("baseIncrement",0.01)) for m in mkts if m.get("symbol")==symbol),0.01)
                            
                            # Calculate original intended size
                            notional = (margin + current_pos.get("initialMargin", 0)) * leverage
                            target_short_size = math.floor((notional/price)/step)*step
                            logging.info(f"{short_acc_name}: Target position size: {target_short_size}")
                        
                        # If already reached target or close enough, consider it done
                        if current_size >= target_short_size:  
                            logging.info(f"{short_acc_name}: Position already {current_size}/{target_short_size} " +
                                            f"({current_size/target_short_size*100:.1f}%) filled - continuing")
                            break
                        
                        # Otherwise, need to place another order for the remaining size
                        remaining_size = target_short_size - current_size
                        logging.info(f"{short_acc_name}: Need to fill remaining {remaining_size} units")
                    
                    # Get order book for current price
                    ob = short_tr.get_order_book(symbol)
                    if not ob.get("asks") or len(ob.get("asks", [])) == 0:
                        logging.warning(f"{short_acc_name}: No asks in order book for {symbol}, retrying...")
                        time.sleep(2)
                        continue
                        
                    best_ask = float(ob.get("asks", [[0]])[0][0])
                    if best_ask <= 0:
                        logging.warning(f"{short_acc_name}: Invalid best ask price: {best_ask}, retrying...")
                        time.sleep(2)
                        continue
                        
                    price = round(best_ask * (1 + maker_off_short), 2)
                    
                    # If this is a follow-up order for remaining size
                    if current_size > 0 and target_short_size > 0:
                        qty = remaining_size
                    else:
                        # Calculate fresh order size
                        margin = short_tr.get_available_margin()
                        if margin <= 0:
                            logging.warning(f"{short_acc_name}: No margin available, retrying...")
                            time.sleep(2)
                            continue
                            
                        notional = margin * leverage
                        
                        # Get step size
                        resp_m = short_tr.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
                        mkts = resp_m.get("data", resp_m) if isinstance(resp_m, dict) else resp_m
                        step = next((float(m.get("baseIncrement",0.01)) for m in mkts if m.get("symbol")==symbol),0.01)
                        qty = math.floor((notional/price)/step)*step
                        
                        # Store this as target size for potential partial fills
                        target_short_size = qty
                    
                    if qty <= 0:
                        logging.warning(f"{short_acc_name}: Calculated quantity is invalid: {qty}, retrying...")
                        time.sleep(2)
                        continue
                    
                    order = short_tr.place_limit_order(symbol, "Ask", price, qty)
                    
                    if order.get("error"):
                        logging.error(f"{short_acc_name}: Failed to place short order: {order.get('error')}")
                        time.sleep(2)
                        continue
                        
                    oid = order.get("orderId") or order.get("data", {}).get("orderId") or order.get("id")
                    
                    if not oid:
                        logging.error(f"{short_acc_name}: No order ID returned for short order: {order}")
                        time.sleep(2)
                        continue
                        
                    logging.info(f"{short_acc_name}: Attempt {attempt}/{retries}: Short maker {oid}@{price}, qty={qty}")
                    
                    # Wait for order to fill
                    st = time.time()
                    order_filled = False
                    last_check_time = 0
                    
                    while time.time()-st < limit_to:
                        # Don't check position too frequently
                        current_time = time.time()
                        if current_time - last_check_time >= 2:  # Check every 2 seconds
                            last_check_time = current_time
                            
                            pos = short_tr.get_position(symbol)
                            if pos:
                                new_size = abs(float(pos.get("netQuantity", 0)))
                                
                                # If position exists or size increased
                                if new_size > 0 and (current_size == 0 or new_size > current_size):
                                    s = float(pos.get("netQuantity", 0))
                                    e = float(pos.get("entryPrice", 0))
                                    m = float(pos.get("markPrice", 0))
                                    l = pos.get("estLiquidationPrice", "Unknown")
                                    pnl = pos.get("unrealizedPnl", "---")
                                    logging.info(f"{short_acc_name}: {symbol}, size={s:.2f} (~{abs(s*e):.2f} USDC), entry={e}, mark={m}, liq={l}, PnL={pnl}")
                                    
                                    # Check if fully filled
                                    if abs(new_size - qty) < step or new_size >= target_short_size:
                                        logging.info(f"{short_acc_name}: Order fully filled (100% of target)")
                                        order_filled = True
                                        short_position_opened = True
                                        break
                                        
                                    # Partial fill
                                    if new_size > current_size:
                                        logging.info(f"{short_acc_name}: Order partially filled: {new_size}/{qty} units")
                                        # Exit loop and try to place another order
                                        short_position_opened = True
                                        break
                            
                        # Also check order status directly
                        status = short_tr.check_order_status(symbol, oid)
                        if status == "FILLED":
                            order_filled = True
                            short_position_opened = True
                            break
                            
                        time.sleep(1)
                        
                    # If timed out or partial fill, cancel remaining order
                    if not order_filled:
                        try:
                            short_tr.cancel_order(symbol, oid)
                            logging.info(f"{short_acc_name}: Cancelled unfilled order {oid}")
                        except Exception as e:
                            logging.error(f"{short_acc_name}: Error cancelling order: {e}")
                        
                        # Check one more time if any fills happened during cancellation
                        pos = short_tr.get_position(symbol)
                        if pos:
                            new_size = abs(float(pos.get("netQuantity", 0)))
                            if new_size > 0:
                                short_position_opened = True
                                if new_size >= target_short_size:
                                    logging.info(f"{short_acc_name}: Position size after cancel: {new_size}/{target_short_size} - sufficient for continuing")
                                    break
                                elif attempt == retries:
                                    logging.info(f"{short_acc_name}: Final attempt reached with partial fill: {new_size}/{target_short_size} - continuing with partial position")
                                    break
                        
                        # If position exists but not fully filled, continue to next attempt
                        if short_position_opened and attempt < retries:
                            logging.info(f"{short_acc_name}: Will try to increase position size in next attempt")
                            continue
                    
                    # If order fully filled or close enough, break the loop
                    if order_filled or (short_position_opened and attempt == retries):
                        break
                        
                except Exception as e:
                    logging.error(f"{short_acc_name}: Error in short order placement attempt {attempt}: {e}")
                    traceback.print_exc()
                    time.sleep(2)
                    
            if not short_position_opened:
                logging.error(f"{short_acc_name}: Failed to open short position after all retries")
                return False

            # === 2.2) Immediate market order on LONG - with retry for failures ===
            long_position_opened = False
            max_long_attempts = 3

            for long_attempt in range(1, max_long_attempts + 1):
                try:
                    margin_l = long_tr.get_available_margin()
                    if margin_l <= 0:
                        logging.error(f"{long_acc_name}: No margin available. Trying again in 5 seconds (attempt {long_attempt}/{max_long_attempts})")
                        time.sleep(5)
                        continue
                        
                    # ФИХ: Используем доступную маржу вместо фиксированного значения
                    # Применяем к маржe коэффициент leverage
                    notional_l = margin_l * leverage  # Используем 100% доступной маржи
                    
                    # Убедимся, что сумма не слишком мала
                    if notional_l < 0.1:
                        logging.error(f"{long_acc_name}: Available margin too low: {margin_l} USDC, notional: {notional_l}. Retrying...")
                        time.sleep(5)
                        continue
                        
                    logging.info(f"{long_acc_name}: Placing market order with notional: {notional_l:.2f} from margin: {margin_l:.2f}")
                    long_order = long_tr.place_market_order(symbol, "Bid", notional_l)
                    
                    if long_order.get("error"):
                        logging.error(f"{long_acc_name}: Failed to place long order: {long_order.get('error')}. Retrying...")
                        time.sleep(5)
                        continue
                        
                    # Wait a moment for the order to process
                    time.sleep(3)
                    
                    # Verify position was opened
                    for check in range(5):  # Multiple checks with increasing delays
                        pos_l = long_tr.get_position(symbol)
                        if pos_l and float(pos_l.get("netQuantity", 0)) != 0:
                            s_l   = float(pos_l.get("netQuantity",0))
                            e_l   = float(pos_l.get("entryPrice",0))
                            m_l   = float(pos_l.get("markPrice",0))
                            l_l   = pos_l.get("estLiquidationPrice","Unknown")
                            pnl_l = pos_l.get("unrealizedPnl","---")
                            logging.info(f"{long_acc_name}: {symbol}, size={s_l:.2f} (~{abs(s_l*e_l):.2f} USDC), entry={e_l}, mark={m_l}, liq={l_l}, PnL={pnl_l}")
                            long_position_opened = True
                            break
                        else:
                            logging.warning(f"{long_acc_name}: Position not found after market order, checking again in {check+1} seconds...")
                            time.sleep(check + 1)
                    
                    if long_position_opened:
                        break
                    else:
                        logging.error(f"{long_acc_name}: Position not found after multiple checks. Retrying market order.")
                except Exception as e:
                    logging.error(f"{long_acc_name}: Error placing long market order (attempt {long_attempt}): {e}")
                    traceback.print_exc()
                    time.sleep(5)

            if not long_position_opened:
                logging.error(f"{long_acc_name}: Failed to open long position after all attempts")
                # At this point we have a short position but no long position
                # Try to close the short position to avoid unbalanced risk
                try:
                    short_pos = short_tr.get_position(symbol)
                    if short_pos and float(short_pos.get("netQuantity", 0)) != 0:
                        logging.warning(f"{short_acc_name}: Closing short position because long position could not be opened")
                        short_qty = abs(float(short_pos.get("netQuantity", 0)))
                        # Market order to close the short
                        close_order = short_tr.place_market_order(symbol, "Bid", short_qty * float(short_pos.get("markPrice", 0)))
                        logging.info(f"{short_acc_name}: Closed short position: {close_order}")
                except Exception as e:
                    logging.error(f"Error closing short position: {e}")
                    
                return False
                
        # 3.x) Получаем свежие данные по позициям и цены ликвидации
        short_pos = short_tr.get_position(symbol)
        long_pos  = long_tr .get_position(symbol)
        # если не удалось взять позиции или они закрыты — прерываем цикл
        if (not short_pos or float(short_pos.get("netQuantity",0)) == 0 or
            not long_pos  or float(long_pos .get("netQuantity",0)) == 0):
            logging.error("Не удалось получить открытые позиции для расчёта цен ликвидации")
            return False

        # теперь safe читать liquidationPrice
        liq_short = float(short_pos['estLiquidationPrice'])
        liq_long  = float(long_pos ['estLiquidationPrice'])


        # 4) Динамическая расстановка TP с учётом flucutation
        fm = FluctuationMonitor(cfg['fluctuation']['window_seconds'])
        last_tp = {'short': None, 'long': None}

        # 4.1) Первичная расстановка TP по static offset
        # первые TP-ордера прямо на цене ликвидации противолежащей позиции
        tp_long_price  = round(liq_short, 2)
        tp_short_price = round(liq_long,  2)

        long_tp = long_tr .place_limit_order(symbol,'Ask',   tp_long_price, abs(float(long_pos ['netQuantity'])), reduce_only=True)
        short_tp= short_tr.place_limit_order(symbol,'Bid',   tp_short_price,abs(float(short_pos['netQuantity'])), reduce_only=True)
        last_tp['long'] = ( long_tp.get('orderId')
                    or long_tp.get('data',{}).get('orderId')
                    or long_tp.get('id') )
        last_tp['short'] = ( short_tp.get('orderId')
                        or short_tp.get('data',{}).get('orderId')
                        or short_tp.get('id') )

        # 4.2) Цикл динамической корректировки TP (пока хотя бы одна позиция ОТКРЫТА)
        window   = cfg['fluctuation']['window_seconds']
        sampling = 1  # интервал сбора mid в секундах
        last_adjust = time.time()

        while True:
            # обновляем позиции
            sp      = short_tr.get_position(symbol)
            lp      = long_tr.get_position(symbol)

            # 2) заново читаем актуальные цены ликвидации
            liq_short = float(sp.get('estLiquidationPrice', 0))
            liq_long  = float(lp.get('estLiquidationPrice', 0))
            
            # 3) проверяем, открыты ли позиции
            sp_open = bool(sp and float(sp.get("netQuantity",0)) != 0)
            lp_open = bool(lp and float(lp.get("netQuantity",0)) != 0)

            # если хотя бы одна позиция закрыта — выходим
            if not sp_open or not lp_open:
                logging.info("[DynamicTP] at least one position closed - exiting loop")
                break

            # рассчитываем mid
            if sp_open and lp_open:
                mid = (float(sp['markPrice']) + float(lp['markPrice'])) / 2
            else:
                mid = float(sp['markPrice']) if sp_open else float(lp['markPrice'])

            fm.add_price(mid)

            # каждую window секунд — пересчитываем TP и логируем
            now = time.time()
            if now - last_adjust >= window:
                start_price = fm.prices[0][1] if fm.prices else mid
                end_price   = mid
                delta_usdc  = fm.get_relative_delta()

                # --- НОВАЯ ЛОГИКА: не двигать TP, если price близко к liq ---
                close_thr = float(cfg.get('close_price_usdc', 0))
                # liq_short и liq_long уже определены чуть выше в цикле
                if abs(end_price - liq_short) <= close_thr or abs(end_price - liq_long) <= close_thr:
                    # Логируем, что TP не меняем из-за близости цены к одной из цен ликвидации
                    logging.info(
                        f"[DynamicTP] Skipping TP adjustment: "
                        f"end_price={end_price:.2f}, "
                        f"liq_short={liq_short:.2f}, liq_long={liq_long:.2f}, "
                        f"threshold={close_thr:.2f}"
                    )
                    last_adjust = now
                    continue
                # --- КОНЕЦ новой логики ---

                # лог окна с 3 знаками
                logging.info(f"[DynamicTP] window={window}s start={start_price:.3f} end={end_price:.3f}")
                logging.info(f"[DynamicTP] delta={delta_usdc:.4f} USDC")
                th = cfg['fluctuation']['thresholds_usdc']
                if   delta_usdc >= th['too_strong']:
                    lvl = 'too_strong'
                elif delta_usdc >= th['strong']:
                    lvl = 'strong'
                elif delta_usdc >= th['medium']:
                    lvl = 'medium'
                elif delta_usdc >= th['weak']:
                    lvl = 'weak'
                else:
                    lvl = 'too_weak'
                offset = cfg['fluctuation']['offsets'][lvl]
                logging.info(f"[DynamicTP] level={lvl}, offset={offset}")

                # подробно логируем позиции (2 знака)
                size_s = abs(float(sp.get("netQuantity",0) or 0))
                entry_s = float(sp.get("entryPrice",0) or 0)
                pnl_s = float(sp.get("unrealizedPnl", 0) or 0)
                liq_s   = float(sp.get("estLiquidationPrice",0) or 0)
                logging.info(f"{short_acc_name} = {sp_open}, entry={entry_s:.2f}, mark={mid:.2f}, liquidation={liq_s:.2f}, PnL={pnl_s:.2f}")

                size_l = abs(float(lp.get("netQuantity",0) or 0))
                entry_l = float(lp.get("entryPrice",0) or 0)
                pnl_l = float(lp.get("unrealizedPnl", 0) or 0)
                liq_l   = float(lp.get("estLiquidationPrice",0) or 0)
                logging.info(f"{long_acc_name} = {lp_open}, entry={entry_l:.2f}, mark={mid:.2f}, liquidation={liq_l:.2f}, PnL={pnl_l:.2f}")

                # при необходимости пересоздаём TP-ордера
                if offset < 0:
                    # «раньше»: 
                    #   long:  от цены liquidation-short минус offset (–offset = +|offset|)
                    #   short: от цены liquidation-long плюс offset (offset<0 → –|offset|)
                    new_long  = round(liq_short - offset, 2)
                    new_short = round(liq_long  + offset, 2)
                else:
                    # «позже»:
                    #   long:  от цены liquidation-short плюс offset
                    #   short: от цены liquidation-long минус offset
                    new_long  = round(liq_short + offset, 2)
                    new_short = round(liq_long  - offset, 2)

                if new_long != tp_long_price or new_short != tp_short_price:
                    logging.info(f"[DynamicTP] adjust TP: {tp_long_price}->{new_long}, {tp_short_price}->{new_short}")
                    
                    # Всегда отменяем предыдущие ордера перед размещением новых
                    # И дожидаемся полной отмены с задержкой!
                    if last_tp['short']:
                        try: 
                            short_tr.cancel_order(symbol, last_tp['short'])
                            last_tp['short'] = None  # Сбрасываем ID отмененного ордера
                            logging.info(f"[DynamicTP] Short TP order canceled successfully")
                        except Exception as e: 
                            logging.warning(f"cancel short failed: {e}")
                        # Добавляем задержку после отмены для обработки запроса биржей
                        time.sleep(1)
                            
                    if last_tp['long']:
                        try: 
                            long_tr.cancel_order(symbol, last_tp['long'])
                            last_tp['long'] = None  # Сбрасываем ID отмененного ордера
                            logging.info(f"[DynamicTP] Long TP order canceled successfully")
                        except Exception as e: 
                            logging.warning(f"cancel long failed: {e}")
                        # Добавляем задержку после отмены для обработки запроса биржей
                        time.sleep(1)
                    
                    # Размещаем новые TP ордера с повторными попытками
                    max_tp_attempts = 3
                    
                    # TP для LONG (сторона Ask/Sell)
                    if lp_open:
                        for attempt in range(max_tp_attempts):
                            try:
                                lt = long_tr.place_limit_order(symbol, 'Ask', new_long, abs(float(lp['netQuantity'])), reduce_only=True)
                                if lt.get('error'):
                                    logging.error(f"[DynamicTP] Attempt {attempt+1}: Failed to place LONG TP: {lt.get('error')}")
                                    # Если ошибка связана с reduceOnly, пробуем с более безопасной ценой
                                    if "REDUCE_ONLY" in str(lt.get('error')) or "INVALID_ORDER" in str(lt.get('error')):
                                        # Увеличиваем цену TP для LONG на 1% от текущей цены
                                        safer_price = round(float(lp['markPrice']) * 1.01, 2)
                                        logging.warning(f"[DynamicTP] Adjusting LONG TP price to safer value: {new_long} -> {safer_price}")
                                        new_long = safer_price
                                        time.sleep(1)
                                        continue
                                    time.sleep(1)
                                else:
                                    last_tp['long'] = lt.get('orderId') or lt.get('data', {}).get('orderId') or lt.get('id')
                                    logging.info(f"[DynamicTP] LONG TP order placed at {new_long}")
                                    break
                            except Exception as e:
                                logging.error(f"[DynamicTP] Error placing LONG TP: {e}")
                                time.sleep(1)
                    
                    # TP для SHORT (сторона Bid/Buy)
                    if sp_open:
                        for attempt in range(max_tp_attempts):
                            try:
                                st = short_tr.place_limit_order(symbol, 'Bid', new_short, abs(float(sp['netQuantity'])), reduce_only=True)
                                if st.get('error'):
                                    logging.error(f"[DynamicTP] Attempt {attempt+1}: Failed to place SHORT TP: {st.get('error')}")
                                    # Если ошибка связана с reduceOnly, пробуем с более безопасной ценой
                                    if "REDUCE_ONLY" in str(st.get('error')) or "INVALID_ORDER" in str(st.get('error')):
                                        # Уменьшаем цену TP для SHORT на 1% от текущей цены
                                        safer_price = round(float(sp['markPrice']) * 0.99, 2)
                                        logging.warning(f"[DynamicTP] Adjusting SHORT TP price to safer value: {new_short} -> {safer_price}")
                                        new_short = safer_price
                                        time.sleep(1)
                                        continue
                                    time.sleep(1)
                                else:
                                    last_tp['short'] = st.get('orderId') or st.get('data', {}).get('orderId') or st.get('id')
                                    logging.info(f"[DynamicTP] SHORT TP order placed at {new_short}")
                                    break
                            except Exception as e:
                                logging.error(f"[DynamicTP] Error placing SHORT TP: {e}")
                                time.sleep(1)
                    
                    # Обновляем сохраненные цены независимо от успеха размещения
                    tp_long_price, tp_short_price = new_long, new_short

                # сохранение в CSV
                with open(LOG_FILE, 'a', newline='') as f:
                    w = csv.writer(f)
                    w.writerow([
                        datetime.utcnow().isoformat(),
                        window,
                        f"{start_price:.6f}",
                        f"{end_price:.6f}",
                        f"{delta_usdc:.6f}",
                        lvl,
                        offset,
                        tp_short_price,
                        tp_long_price
                    ])

                last_adjust = now

            # собираем данные в окне
            time.sleep(sampling)


        # 5) Monitor until both positions closed
        max_monitor_time = 3600 * 24  # 24 hours maximum monitoring time
        start_monitoring = time.time()
        # Интервал между проверками из конфига, по умолчанию 30
        check_int = int(cfg.get("check_interval", 30))

        short_acc_name = sa_cfg.get("name", "ShortAccount")
        long_acc_name = la_cfg.get("name", "LongAccount")

        try:
            while time.time() - start_monitoring < max_monitor_time:
                short_pos = short_tr.get_position(symbol)
                long_pos = long_tr.get_position(symbol)
                
                # Check if both positions exist
                short_active = short_pos and float(short_pos.get("netQuantity", 0)) != 0
                long_active = long_pos and float(long_pos.get("netQuantity", 0)) != 0
                
                # Log position details if active
                if short_active:
                    s = float(short_pos.get("netQuantity", 0))
                    e = float(short_pos.get("entryPrice", 0))
                    m = float(short_pos.get("markPrice", 0))
                    l = short_pos.get("estLiquidationPrice", "Unknown")
                    pnl = short_pos.get("unrealizedPnl", "---")
                    logging.info(f"{short_acc_name}: Short position: size={s:.2f}, entry={e}, mark={m}, liquidation={l}, PnL={pnl}")
                else:
                    logging.info(f"{short_acc_name}: No active short position found")
                
                if long_active:
                    s = float(long_pos.get("netQuantity", 0))
                    e = float(long_pos.get("entryPrice", 0))
                    m = float(long_pos.get("markPrice", 0))
                    l = long_pos.get("estLiquidationPrice", "Unknown")
                    pnl = long_pos.get("unrealizedPnl", "---")
                    logging.info(f"{long_acc_name}: Long position: size={s:.2f}, entry={e}, mark={m}, liquidation={l}, PnL={pnl}")
                else:
                    logging.info(f"{long_acc_name}: No active long position found")
                
                # Check if both positions are closed - use the explicit check with short_active and long_active flags
                if not short_active and not long_active:
                    # Double-check with a small delay to make sure it's not just API flakiness
                    time.sleep(3)
                    
                    # Re-check positions
                    short_pos = short_tr.get_position(symbol)
                    long_pos = long_tr.get_position(symbol)
                    
                    short_active = short_pos and float(short_pos.get("netQuantity", 0)) != 0
                    long_active = long_pos and float(long_pos.get("netQuantity", 0)) != 0
                    
                    if not short_active and not long_active:
                        logging.info("Both positions confirmed closed. Moving to sweep phase.")
                        break
                
                # If we've been monitoring for more than 1 hour, implement safety checks
                if time.time() - start_monitoring > 3600:
                    # If we haven't seen any positions in a long time, something might be wrong
                    if not short_active and not long_active:
                        logging.warning("No positions found after 1 hour of monitoring. Moving to sweep phase.")
                        break
                
                time.sleep(check_int)
        except Exception as e:
            logging.error(f"Error during position monitoring: {e}")
            traceback.print_exc()

        # 6) Sweep ALL remaining funds FROM sub-accounts TO parent account
        # …после мониторинга и подтверждения закрытия позиций…
        logging.info("Both positions confirmed closed. Moving to sweep phase.")
        # пауза перед свипом
        time.sleep(10)  # Заменяем log_sleep, так как этой функции нет в коде

        # ВСТАВЬТЕ КОД PNL ЗДЕСЬ:
        # После завершения мониторинга и перед свипом, собираем данные PnL
        logging.info("Logging PnL data before sweep phase.")

        # Получение данных PnL и комиссий
        short_pnl, short_fees = get_account_pnl_data(short_tr, short_acc_name, symbol)
        long_pnl, long_fees = get_account_pnl_data(long_tr, long_acc_name, symbol)
        total_pnl = short_pnl + long_pnl - short_fees - long_fees

        # Записываем данные PnL в файл
        with open(PNL_LOG_FILE, 'a', newline='') as f:
            w = csv.writer(f)
            w.writerow([
                pair_number,
                cycle_number,
                f"{short_pnl:.6f}",
                f"{long_pnl:.6f}",
                f"{short_fees:.6f}",
                f"{long_fees:.6f}",
                f"{total_pnl:.6f}"
            ])

        logging.info(f"PnL data for pair #{pair_number}, cycle #{cycle_number} logged successfully")

        all_funds_swept = True

        # Объявляем две функции для получения информации о балансе и вывода средств
        def get_subaccount_balance(trader, acc_name):
            """Берём доступную маржу как сумму для свипа"""
            try:
                bal = trader.get_available_margin()
                logging.info(f"{acc_name}: Available USDC margin = {bal}")
                return bal
            except Exception as e:
                logging.error(f"{acc_name}: Failed to get balance: {e}")
                return 0.0

        def withdraw_from_subaccount(trader, acc_name, amount, main_address):
            """Вывести USDC из суб-аккаунта на основной счет, вернуть результат запроса"""
            try:
                qty_str = f"{amount:.6f}"
                logging.info(f"{acc_name}: Withdrawing {qty_str} USDC -> {main_address}")
                result = trader.auth.request_withdrawal(
                    address=main_address,
                    blockchain="Solana",
                    quantity=qty_str,
                    symbol="USDC"
                )
                return result  # raw dict
            except Exception as e:
                logging.error(f"{acc_name}: Withdrawal failed: {e}")
                return None

                
        # Сам свип
        for side in ("short_account", "long_account"):
            tr = short_tr if side == "short_account" else long_tr
            acc_name = sa_cfg["name"] if side=="short_account" else la_cfg["name"]

            bal = get_subaccount_balance(tr, acc_name)
            if bal > 0.1:
                # Без задержки, сразу выводим
                res = withdraw_from_subaccount(tr, acc_name, bal, cfg["main_account"]["address"])
                if res:
                    logging.info(
                        f"{acc_name}: Withdrawal submitted — "
                        f"ID={res.get('id')} | "
                        f"Amount={res.get('quantity')} USDC | "
                        f"To={res.get('toAddress')} | "
                        f"Status={res.get('status')}"
                    )
                else:
                    logging.warning(f"{acc_name}: Withdrawal attempt failed")
            else:
                logging.info(f"{acc_name}: No significant funds to withdraw (balance: {bal} USDC)")

    # Return the status of fund sweeping
        return all_funds_swept
    except Exception as e:
        logging.error(f"Error in process_pair: {str(e)}")
        traceback.print_exc()
        return False  # Indicate error

def get_account_pnl_data(trader, acc_name, symbol):
    """
    Получает данные о PnL и комиссиях для аккаунта, используя различные API эндпоинты.
    
    Args:
        trader: Объект BackpackTrader для доступа к API
        acc_name: Имя аккаунта для логирования
        symbol: Торговая пара
        
    Returns:
        tuple: (pnl, fees) - данные о PnL и комиссиях
    """
    try:
        pnl = 0.0
        fees = 0.0
        
        # Метод 1: Попытка получить позицию напрямую
        position_found = False
        pos = trader.get_position(symbol)
        if pos and isinstance(pos, dict) and pos.get("symbol") == symbol:
            position_found = True
            pnl = float(pos.get("realizedPnl", 0) or 0)
            logger.info(f"{acc_name}: Found current position data - Realized PnL={pnl:.6f}")
            
            # Если realizedPnl = 0, проверяем unrealizedPnl
            if pnl == 0 and "unrealizedPnl" in pos:
                pnl = float(pos.get("unrealizedPnl", 0) or 0)
                logger.info(f"{acc_name}: Using unrealized PnL={pnl:.6f}")
                
        # Метод 2: Попытка получить историю через другие эндпоинты
        if not position_found or pnl == 0:
            # Попробуем /api/v1/fill (история сделок)
            try:
                # Используем эндпоинт сделок (fills) вместо позиций
                fills = trader.auth._send_request(
                    "GET", 
                    "api/v1/fill", 
                    "fillQuery", 
                    {"symbol": symbol, "limit": 50}
                )
                
                if fills and isinstance(fills, (list, dict)):
                    data = fills
                    if isinstance(fills, dict) and "data" in fills:
                        data = fills["data"]
                        
                    if data and len(data) > 0:
                        logger.info(f"{acc_name}: Found {len(data)} fills")
                        # Суммируем PnL и комиссии от всех сделок
                        for fill in data:
                            if fill.get("symbol") == symbol:
                                fill_pnl = float(fill.get("realizedPnl", 0) or 0)
                                fill_fee = float(fill.get("fee", 0) or 0)
                                pnl += fill_pnl
                                fees += fill_fee
                                
                        logger.info(f"{acc_name}: Calculated from fills - PnL={pnl:.6f}, Fees={fees:.6f}")
                        position_found = True
            except Exception as e:
                logger.warning(f"{acc_name}: Error accessing fills: {e}")
                
        # Метод 3: Попытка получить информацию о комиссиях
        if fees == 0:
            try:
                # Попробуем получить историю ордеров
                orders = trader.auth._send_request(
                    "GET", 
                    "api/v1/order/history", 
                    "orderHistoryQuery", 
                    {"symbol": symbol, "limit": 50}
                )
                
                if orders and isinstance(orders, (list, dict)):
                    data = orders
                    if isinstance(orders, dict) and "data" in orders:
                        data = orders["data"]
                        
                    if data and len(data) > 0:
                        logger.info(f"{acc_name}: Found {len(data)} orders")
                        # Собираем информацию о комиссиях
                        for order in data:
                            if order.get("symbol") == symbol:
                                order_fee = float(order.get("fee", 0) or 0)
                                fees += order_fee
                                
                        logger.info(f"{acc_name}: Calculated fees from orders: {fees:.6f}")
            except Exception as e:
                logger.warning(f"{acc_name}: Error accessing order history: {e}")
        
        # Метод 4: Попытка получить информацию из истории транзакций
        if not position_found or pnl == 0 or fees == 0:
            try:
                # Попробуем получить историю транзакций
                transactions = trader.auth._send_request(
                    "GET", 
                    "api/v1/transaction", 
                    "transactionQuery", 
                    {"limit": 50}
                )
                
                if transactions and isinstance(transactions, (list, dict)):
                    data = transactions
                    if isinstance(transactions, dict) and "data" in transactions:
                        data = transactions["data"]
                        
                    if data and len(data) > 0:
                        logger.info(f"{acc_name}: Found {len(data)} transactions")
                        # Ищем транзакции, связанные с PnL или комиссиями
                        for tx in data:
                            if tx.get("type") == "REALIZED_PNL":
                                # Предполагаем, что есть поле amount или подобное
                                tx_pnl = float(tx.get("amount", 0) or 0)
                                pnl += tx_pnl
                            elif tx.get("type") == "FEE":
                                tx_fee = float(tx.get("amount", 0) or 0)
                                fees += tx_fee
                                
                        logger.info(f"{acc_name}: Calculated from transactions - PnL={pnl:.6f}, Fees={fees:.6f}")
            except Exception as e:
                logger.warning(f"{acc_name}: Error accessing transaction history: {e}")
        
        # Метод 5: Попробуем напрямую API для позиционной истории
        try:
            # Это специфический эндпоинт, который может быть доступен
            position_history = trader.auth._send_request(
                "GET", 
                "api/v1/position/closed", 
                "closedPositionQuery", 
                {"symbol": symbol, "limit": 10}
            )
            
            if position_history and isinstance(position_history, (list, dict)):
                data = position_history
                if isinstance(position_history, dict) and "data" in position_history:
                    data = position_history["data"]
                    
                if data and len(data) > 0:
                    logger.info(f"{acc_name}: Found {len(data)} closed positions")
                    # Берем последнюю закрытую позицию
                    last_position = data[0]
                    history_pnl = float(last_position.get("realizedPnl", 0) or 0)
                    history_fees = float(last_position.get("fee", 0) or last_position.get("totalFees", 0) or 0)
                    
                    # Если мы нашли какую-то сумму, используем ее
                    if history_pnl != 0:
                        pnl = history_pnl
                    if history_fees != 0:
                        fees = history_fees
                        
                    logger.info(f"{acc_name}: Data from closed positions - PnL={pnl:.6f}, Fees={fees:.6f}")
                    position_found = True
        except Exception as e:
            logger.warning(f"{acc_name}: Error accessing closed positions: {e}")
        
        # Если до сих пор у нас нет данных, пробуем получить остальные эндпоинты API
        # Это перебор возможных путей API, которые могут содержать нужную информацию
        if not position_found or (pnl == 0 and fees == 0):
            possible_endpoints = [
                "api/v1/account/pnl",
                "api/v1/position/all",
                "api/v1/position/summary",
                "api/v1/trading/pnl"
            ]
            
            for endpoint in possible_endpoints:
                try:
                    response = trader.auth._send_request(
                        "GET", 
                        endpoint, 
                        endpoint.replace("/", "_"), 
                        {"symbol": symbol}
                    )
                    
                    if response and isinstance(response, (list, dict)):
                        logger.info(f"{acc_name}: Successfully queried {endpoint}")
                        # Если получили ответ, пытаемся извлечь pnl и fees
                        # Логика извлечения зависит от формата ответа
                        # ...
                        
                except Exception:
                    pass  # Тихо игнорируем ошибки при поиске подходящих эндпоинтов
        
        # Если комиссии все еще не найдены, используем приблизительную оценку
        if fees == 0.0:
            # Используем типичный размер комиссии на биржах - обычно 0.1% от объема сделки
            fees = abs(pnl) * 0.001
            logger.warning(f"{acc_name}: Using estimated fees based on PnL: {fees:.6f}")
        
        logger.info(f"{acc_name}: Final data - PnL={pnl:.6f}, Fees={fees:.6f}")
        return pnl, fees
        
    except Exception as e:
        logger.error(f"{acc_name}: Failed to get PnL data: {e}")
        traceback.print_exc()
        return 0.0, 0.0
    
# -------------------- Main --------------------
def main() -> None:
    try:
        cfg_path = Path("config.yaml")
        if not cfg_path.exists():
            logging.error(f"Config not found: {cfg_path}")
            sys.exit(1)

        cfg = yaml.safe_load(cfg_path.read_text(encoding='utf-8'))
        
        # Валидация конфига
        if not cfg or "pairs" not in cfg or not cfg["pairs"]:
            logging.error("Config is empty or no pairs configured")
            sys.exit(1)
        if not cfg.get("main_account", {}).get("address"):
            logging.error("Main account address missing in config")
            sys.exit(1)
        # Инициализация прокси из конфига
        proxy_url = init_proxy_from_config(cfg)
        
        # Патчим SDK клиенты для использования прокси
        patch_sdk_clients(proxy_url)

        threads = []

        # Для каждой пары создаём и запускаем воркер-поток
        for pair in cfg["pairs"]:
            short_name = pair["short_account"]["name"]
            long_name  = pair["long_account"]["name"]
            thread_name = f"{short_name}/{long_name}"
            t = threading.Thread(
                target=pair_worker,
                args=(pair, cfg),
                name=thread_name,
                daemon=True
            )
            t.start()
            threads.append(t)
            logging.info(f"Started worker thread for pair {thread_name}")

        # Главный поток просто ждёт, пока живут все воркеры
        for t in threads:
            t.join()

    except Exception as e:
        logging.error(f"Fatal error in main: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
