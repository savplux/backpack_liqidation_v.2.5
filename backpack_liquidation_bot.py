import logging
import time
import random
import sys
from pathlib import Path
import yaml
import math
import traceback
import threading
import json
import websocket
import hmac
import hashlib
from urllib.parse import urlencode
from datetime import datetime, timedelta

# Добавить импорт модуля обработки прокси
from proxy_handler import patch_sdk_clients, init_proxy_from_config

from backpack_exchange_sdk.authenticated import AuthenticationClient
from backpack_exchange_sdk.public import PublicClient
import colorlog

import os, csv, time, logging
from collections import deque
from datetime import datetime

# ДЛЯ def close_position_market
FETCH_RETRIES      = 3       # сколько раз пробуем получить позиции
FETCH_RETRY_DELAY  = 1       # пауза между попытками (сек)
CLOSE_RETRIES      = 3       # сколько раз пробуем выставить ордер на закрытие
CLOSE_RETRY_DELAY  = 1       # пауза между попытками закрытия (сек)

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

# Добавьте в начало файла (рядом с другими хелперами)
def _wait_for_sweep_confirmation(parent, tx_id: str, timeout: int = 60, poll_interval: float = 5.0):
    start = time.time()
    while time.time() - start < timeout:
        try:
            status = parent._send_request(
                "GET", "api/v1/transaction", "txStatusQuery", {"txId": tx_id}
            ).get("status", "")
        except Exception:
            status = ""
        if status.lower() == "confirmed":
            return True
        logging.info(f"[Sweep] waiting for tx {tx_id} confirmation, status={status}")
        time.sleep(poll_interval)
    raise TimeoutError(f"Sweep tx {tx_id} not confirmed within {timeout}s")


# -------------------- WebSocket PnL Tracker --------------------
class PnLWebSocketTracker:
    """
    Класс для отслеживания unrealized PnL через веб-сокеты.
    Подключается к приватным веб-сокетам Backpack Exchange и отслеживает обновления позиций.
    """
    def __init__(self, api_key, api_secret, symbol, name):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol        # ← новый атрибут
        self.name = name          # ← новый атрибут
        self.ws = None
        self.pnl_data = {}  # Словарь для хранения данных PnL по символам
        self.position_updates = {}  # Словарь для хранения последних обновлений позиций
        self.last_update_time = {}  # Время последнего обновления для каждого символа
        self.running = False
        self.ws_thread = None
        self.lock = threading.Lock()  # Блокировка для безопасного доступа к данным
        self.ping_thread = None
        self.connection_time = None
        self.current_unrealized_pnl = 0.0   # ← новое поле

    def generate_signature(self, params_dict):
        """Генерирует подпись для аутентификации веб-сокета."""
        # Сортируем параметры по ключу
        sorted_params = sorted(params_dict.items())
        query_string = urlencode(sorted_params)
        
        # Создаем подпись
        signature = hmac.new(
            self.api_secret.encode(),
            query_string.encode(),
            hashlib.sha256
        ).hexdigest()
        
        return signature

    def on_message(self, ws, message):
        try:
            msg = json.loads(message)
        except json.JSONDecodeError:
            logging.warning(f"WebSocket: не JSON: {message}")
            return

        # 1) Дебаг всех сообщений
        logging.debug(f"[WS RAW] {msg}")

        # 2) После успешного auth — подписываемся
        if msg.get("event") == "auth" and msg.get("success") is True:
            sub_msg = json.dumps({
                "op": "subscribe",
                "args": [f"position.{self.symbol}"]
            })
            ws.send(sub_msg)
            logging.info(f"WebSocket: Подписка на position.{self.symbol} отправлена после авторизации")
            return

        # 3) Разбор обновлений позиций
        if msg.get("table") == "position" or msg.get("topic") == "position":
            for pos in msg.get("data", []):
                    if pos.get("symbol") != self.symbol:
                        continue

                    size = float(pos.get("size", 0))

                    # 1) При закрытии позиции — сбрасываем PnL, запускаем трекер, если не запущен
                    if size == 0:
                        with self.lock:
                            # сбрасываем PnL в 0 во всех хранилищах
                            self.current_unrealized_pnl = 0.0
                            self.pnl_data[self.symbol] = 0.0
                            self.last_update_time[self.symbol] = time.time()
                        logging.info(f"{self.name}: Позиция закрыта, PnL сброшен в 0")
                        # lazy-создаем и стартуем трекер только после закрытия
                        if not self.running:
                            logging.info(f"{self.name}: Старт WebSocket-трекера после закрытия позиции")
                            self.start()
                        continue

                    # 2) При первой полученной цене для вновь открытой позиции — тоже стартуем
                    if size > 0 and not self.running:
                        logging.info(f"{self.name}: Обнаружена открытая позиция, старт WebSocket-трекера")
                        self.start()

                    # 3) Обновляем unrealized PnL
                    pnl = pos.get("unrealizedPnl") or pos.get("unrealized_pnl")
                    if pnl is None:
                        logging.debug(f"[WS POS KEYS] {list(pos.keys())}")
                        continue

                    val = float(pnl)
                    with self.lock:
                        self.current_unrealized_pnl = val
                        self.pnl_data[self.symbol] = val
                        self.last_update_time[self.symbol] = time.time()
                    logging.info(f"{self.name}: Текущий unrealized PnL открытой позиции: {val}")
                    return
            
            # Если дошли сюда — открытой позиции нет или в данных не было нужного символа
            self.current_unrealized_pnl = 0.0
            logging.info(f"{self.name}: Открытой позиции нет, unrealized PnL = 0.0")

    def on_error(self, ws, error):
        """Обрабатывает ошибки веб-сокета."""
        logging.error(f"WebSocket: Ошибка: {error}")
        # При ошибке попробуем переподключиться через некоторое время
        if self.running:
            time.sleep(5)
            self.connect()

    def on_close(self, ws, close_status_code, close_msg):
        """Обрабатывает закрытие соединения веб-сокета."""
        logging.info(f"WebSocket: Соединение закрыто (код: {close_status_code}, сообщение: {close_msg})")
        # При закрытии соединения попробуем переподключиться, если до сих пор работаем
        if self.running:
            time.sleep(5)
            self.connect()

    def on_open(self, ws):
        logging.info("Websocket connected")
        auth_msg = json.dumps({
            "op": "auth",
            "args": [self.api_key, self.api_secret]
        })
        ws.send(auth_msg)
        logging.info("WebSocket: Запрос аутентификации отправлен")

        # Теперь сразу подписываемся на позицию для нашего символа
        sub_msg = json.dumps({
            "op": "subscribe",
            "args": [f"position.{self.symbol}"]
        })
        ws.send(sub_msg)
        logging.info(f"WebSocket: Подписка на position.{self.symbol} отправлена (on_open)")

    def _ping_thread(self):
        """Поток для отправки PING сообщений серверу для поддержания соединения."""
        while self.running and self.ws and self.ws.sock:
            try:
                ping_message = {"op": "ping"}  # Исправлено с {"type": "PING"}
                self.ws.send(json.dumps(ping_message))
                logging.debug("WebSocket: PING отправлен")
                time.sleep(30)  # Отправляем PING каждые 30 секунд
            except Exception as e:
                logging.error(f"WebSocket: Ошибка отправки PING: {e}")
                break

    def connect(self):
        """Устанавливает соединение с WebSocket API Backpack Exchange."""
        websocket.enableTrace(True)

        # Правильный endpoint WebSocket API (вместо устаревшего stream.backpack.exchange)
        ws_url = "wss://ws.backpack.exchange"  # :contentReference[oaicite:0]{index=0}

        self.ws = websocket.WebSocketApp(
            ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

        # Запускаем WebSocket в отдельном потоке, с автоматической отправкой PING каждые 30 сек
        self.ws_thread = threading.Thread(
            target=lambda: self.ws.run_forever(ping_interval=30),
            daemon=True
        )
        self.ws_thread.start()
        logging.info(f"WebSocket: Запущен в отдельном потоке на {ws_url}")

    def start(self):
        """Запускает отслеживание PnL через веб-сокеты."""
        if self.running:
            logging.warning("WebSocket: Уже запущен")
            return
            
        self.running = True
        self.connect()
        logging.info("WebSocket: Отслеживание PnL запущено")

    def stop(self):
        """Останавливает отслеживание PnL."""
        self.running = False
        if self.ws:
            self.ws.close()
        self.ping_thread = threading.Thread(target=self._ping_thread, daemon=True)
        self.ping_thread.start()
        logging.info("WebSocket: Отслеживание PnL остановлено")

    def get_unrealized_pnl(self, symbol):
        """
        Возвращает текущий unrealized PnL для указанного символа.
        
        Args:
            symbol: Символ торговой пары (например, SOL_USDC_PERP)
            
        Returns:
            float: Текущий unrealized PnL или None, если данные недоступны
        """
        with self.lock:
            # Проверяем, есть ли данные для символа
            if symbol in self.pnl_data:
                # Проверяем, не устарели ли данные (не старше 60 секунд)
                if symbol in self.last_update_time:
                    last_update = self.last_update_time[symbol]
                    if time.time() - last_update > 60:
                        logging.warning(f"WebSocket: Данные PnL для {symbol} устарели (последнее обновление: {datetime.fromtimestamp(last_update)})")
                        return None
                return self.pnl_data[symbol]
            return None

    def get_position_data(self, symbol):
        """
        Возвращает полные данные о позиции для указанного символа.
        
        Args:
            symbol: Символ торговой пары (например, SOL_USDC_PERP)
            
        Returns:
            dict: Полные данные о позиции или None, если данные недоступны
        """
        with self.lock:
            if symbol in self.position_updates:
                # Проверяем, не устарели ли данные (не старше 60 секунд)
                if symbol in self.last_update_time:
                    last_update = self.last_update_time[symbol]
                    if time.time() - last_update > 60:
                        logging.warning(f"WebSocket: Данные позиции для {symbol} устарели (последнее обновление: {datetime.fromtimestamp(last_update)})")
                        return None
                return self.position_updates[symbol]
            return None

# Создаем глобальный словарь для хранения экземпляров трекеров PnL для каждого аккаунта
ws_pnl_trackers = {}

def get_or_create_pnl_tracker(api_key, api_secret, account_name, symbol):
    # ключ остаётся таким же, чтобы tracker.lookup срабатывал правильно
    key = api_key + symbol
    if key not in ws_pnl_trackers:
        logging.info(f"{account_name}: Инициализация и запуск WebSocket трекера PnL")
        tracker = PnLWebSocketTracker(api_key, api_secret, symbol, account_name)
        tracker.start()  # ← сразу подключаемся и подписываемся
        tracker.start()  # ← сразу создаём соединение и подписываемся на обновления
        ws_pnl_trackers[key] = tracker
    return ws_pnl_trackers[key]

def get_unrealized_pnl_websocket(trader, account_name, symbol):
    """
    Получает текущий unrealized PnL для указанного символа через веб-сокеты.
    
    Args:
        trader: Объект BackpackTrader для получения API ключа и секрета
        account_name: Имя аккаунта для логирования
        symbol: Символ торговой пары (например, SOL_USDC_PERP)
        
    Returns:
        float: Текущий unrealized PnL или None, если данные недоступны
    """
    try:
        # Получаем API ключ и секрет из объекта trader
        # В объекте AuthenticationClient ключи хранятся по-другому
        # Исправляем доступ к ключам
        api_key = None
        api_secret = None
        
        # Проверяем доступность ключей напрямую в trader
        if hasattr(trader, "api_key") and hasattr(trader, "api_secret"):
            api_key = trader.api_key
            api_secret = trader.api_secret
        # Проверяем доступность через auth объект с учетом разных возможных названий атрибутов
        elif hasattr(trader, "auth"):
            auth = trader.auth
            # Прямые атрибуты
            if hasattr(auth, "api_key") and hasattr(auth, "api_secret"):
                api_key = auth.api_key
                api_secret = auth.api_secret
            # Через _auth_credentials в некоторых версиях SDK
            elif hasattr(auth, "_auth_credentials"):
                credentials = auth._auth_credentials
                api_key = credentials.get("apiKey") or credentials.get("api_key")
                api_secret = credentials.get("secret") or credentials.get("api_secret")
            # Через конфигурацию
            elif hasattr(auth, "config"):
                config = auth.config
                api_key = config.get("apiKey") or config.get("api_key")
                api_secret = config.get("secret") or config.get("api_secret")
        
        # Если не смогли найти ключи
        if not api_key or not api_secret:
            logging.error(f"{account_name}: Не удалось получить API ключи для WebSocket")
            return None
            
        # Получаем или создаем трекер PnL
        tracker = get_or_create_pnl_tracker(api_key, api_secret, account_name, symbol)
        # на всякий случай: если из-за каких-то причин трекер ещё не запущен — стартанём его
        if not tracker.running:
            logging.info(f"{account_name}: WS-трекер не был запущен, запускаем его")
            tracker.start()
       
        # Получаем unrealized PnL
        pnl = tracker.get_unrealized_pnl(symbol)
        
        if pnl is not None:
            logging.info(f"{account_name}: Получен unrealized PnL через WebSocket: {pnl}")
            return pnl
        else:
            logging.warning(f"{account_name}: Не удалось получить unrealized PnL через WebSocket")
            return None
    except Exception as e:
        logging.error(f"{account_name}: Ошибка при получении unrealized PnL через WebSocket: {e}")
        traceback.print_exc()
        return None
    
# Добавляю новую функцию для получения PnL закрытой позиции через правильный API эндпоинт
def get_closed_position_pnl(trader, account_name, symbol=None):
    """
    Получает PnL последней закрытой позиции с помощью API запроса к /wapi/v1/history/settlement
    с правильным методом подписи 'settlementHistoryQueryAll'
    
    Args:
        trader: Объект BackpackTrader для доступа к API
        account_name: Имя аккаунта для логирования
        symbol: Опциональный символ торговой пары для фильтрации
        
    Returns:
        float: Реализованный PnL последней закрытой позиции или None в случае ошибки
    """
    try:
        logging.info(f"{account_name}: Запрос PnL последней закрытой позиции через /wapi/v1/history/settlement")
        
        # Параметры запроса - получаем только последние записи, отсортированные по убыванию (новые в начале)
        params = {
            "limit": 5,  # Ограничиваем самыми последними записями
            "source": "RealizePnl",  # Фильтрация по типу RealizePnl
            "sortDirection": "Desc"  # Сортировка по убыванию - новые позиции первыми
        }
        
        if symbol:
            params["symbol"] = symbol
            
        # Запрос к API с правильным методом подписи 'settlementHistoryQueryAll'
        for attempt in range(1, 4):  # 3 попытки
            try:
                settlement_history = trader.auth._send_request(
                    "GET", 
                    "wapi/v1/history/settlement", 
                    "settlementHistoryQueryAll",  # Правильное название метода для подписи
                    params
                )
                
                if settlement_history:
                    logging.info(f"{account_name}: Успешно получены данные settlement history (попытка {attempt})")
                    break
            except Exception as e:
                logging.warning(f"{account_name}: Ошибка при запросе settlement history (попытка {attempt}): {e}")
                if attempt < 3:
                    time.sleep(2)
                    
                # Если это последняя попытка, пробуем без параметра source
                if attempt == 2:
                    params.pop("source", None)
                    logging.info(f"{account_name}: Последняя попытка без параметра source")
                    
                if attempt == 3:
                    # Если все попытки не удались, возвращаем None
                    return None
        
        logging.info(f"{account_name}: Получен ответ от API settlement: {settlement_history}")
        
        # Парсинг ответа API
        if settlement_history and isinstance(settlement_history, (list, dict)):
            data = settlement_history
            if isinstance(settlement_history, dict) and "data" in settlement_history:
                data = settlement_history["data"]
            
            if isinstance(data, list) and len(data) > 0:
                # Берем первую запись (самую свежую, так как используем sortDirection=Desc)
                first_entry = data[0]
                
                # Проверяем, что это запись типа RealizePnl
                if first_entry.get("source") == "RealizePnl" or not "source" in params:
                    # Получаем значение PnL из поля quantity
                    amount = float(first_entry.get("quantity", 0) or 0)
                    settlement_time = first_entry.get("timestamp", "unknown")
                    entry_symbol = first_entry.get("symbol", "unknown")
                    
                    logging.info(f"{account_name}: Найдена запись RealizePnl: сумма={amount}, "
                                f"время={settlement_time}, символ={entry_symbol}")
                    return amount
                else:
                    logging.warning(f"{account_name}: Первая запись не содержит source=RealizePnl: {first_entry}")
                    
                    # Ищем первую запись с source=RealizePnl
                    for entry in data:
                        if entry.get("source") == "RealizePnl":
                            amount = float(entry.get("quantity", 0) or 0)
                            settlement_time = entry.get("timestamp", "unknown")
                            entry_symbol = entry.get("symbol", "unknown")
                            
                            logging.info(f"{account_name}: Найдена запись RealizePnl: сумма={amount}, "
                                        f"время={settlement_time}, символ={entry_symbol}")
                            return amount
                            
                logging.warning(f"{account_name}: Не найдено записей с source=RealizePnl в истории расчетов")
            else:
                logging.warning(f"{account_name}: Не найдено записей в истории расчетов")
        else:
            logging.warning(f"{account_name}: Некорректный формат ответа API: {settlement_history}")
        
        # Если не удалось получить PnL через API settlement, 
        # возвращаем None и будем использовать альтернативный метод
        return None
    except Exception as e:
        logging.error(f"{account_name}: Ошибка при получении PnL закрытой позиции: {e}")
        traceback.print_exc()
        return None

def close_position_market(trader, symbol, _, account_name):
    """
    Закрывает открытую позицию рыночным ордером (reduceOnly) с ретраями
    и добивает остатки по минимальному шагу.
    """
    # 1) Получаем список позиций с несколькими попытками
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            logging.info(f"[Action] Fetching positions for {symbol} (attempt {attempt}/{FETCH_RETRIES})")
            positions = trader.auth._send_request(
                "GET", "api/v1/position", "positionQuery", {}
            )
            break
        except Exception as e:
            logging.error(f"[Error] Fetching positions: {e}")
            if attempt < FETCH_RETRIES:
                time.sleep(FETCH_RETRY_DELAY)
            else:
                return False

    # 2) Пробегаем по всем позициям и ищем нашу
    for pos in positions:
        if pos.get("symbol") != symbol:
            continue

        size = float(pos.get("netQuantity", 0))
        if size == 0:
            logging.info(f"[Info] No position for {symbol}")
            return True

        side = "Ask" if size > 0 else "Bid"
        abs_size = abs(size)

        # 3) Пытаемся закрыть в один ордер полным объёмом
        for close_attempt in range(1, CLOSE_RETRIES + 1):
            try:
                logging.info(
                    f"[Action] Closing position {symbol}, size={size}, "
                    f"side={side} (try {close_attempt})"
                )
                trader.auth.execute_order(
                    orderType="Market",
                    side=side,
                    symbol=symbol,
                    quantity=str(abs_size),
                    reduceOnly=True
                )
                logging.info(f"[Success] Sent market-close for {symbol}, size={size}")

                # небольшая задержка после удачного отправления (фиксированный 1 сек)
                time.sleep(1)

                # 4) Проверяем, осталась ли «хвостовая» позиция
                # Если позиция исчезла или ≤ одного шага лота — OK
                current = trader.get_position(symbol) or {}
                rem = abs(float(current.get("netQuantity", 0) or 0))
                # узнаём шаг лота
                m = trader.auth._send_request("GET", "api/v1/markets", "marketQuery", {})
                mkts = m.get("data", m) if isinstance(m, dict) else m
                step = next((float(x.get("baseIncrement", 0.01)) for x in mkts if x.get("symbol")==symbol), 0.01)
                if rem <= step:
                    logging.info(f"[Info] Position {symbol} fully closed (remaining {rem:.6f} <= step {step})")
                    return True
                # иначе — пытаемся закрыть остаток
                abs_size = rem
                logging.info(f"[Info] Residual {rem:.6f} remains, retrying close")
                continue

            except Exception as err:
                logging.error(f"[Error] Closing position {symbol}: {err}")
                if close_attempt < CLOSE_RETRIES:
                    time.sleep(CLOSE_RETRY_DELAY)
                else:
                    return False

    logging.info(f"[Info] Symbol {symbol} not in positions")
    return True

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

        # Получаем настройки для новой логики закрытия по PnL
        pnl_diff_check_delay = int(cfg.get("pnl_diff", {}).get("check_delay_seconds", 10))
        pnl_diff_min_threshold = float(cfg.get("pnl_diff", {}).get("min_threshold", -1.0))
        pnl_diff_max_threshold = float(cfg.get("pnl_diff", {}).get("max_threshold", 2.0))
        pnl_diff_recheck_interval = int(cfg.get("pnl_diff", {}).get("recheck_interval_seconds", 30))
        
        # === ИНИЦИАЛИЗАЦИЯ ТРЕЙДЕРОВ И ИМЁН ===
        sa_cfg = pair_cfg["short_account"]
        la_cfg = pair_cfg["long_account"]
        short_tr = BackpackTrader(sa_cfg["api_key"], sa_cfg["api_secret"])
        long_tr  = BackpackTrader(la_cfg["api_key"], la_cfg["api_secret"])
        short_acc_name = sa_cfg.get("name", "ShortAccount")
        long_acc_name  = la_cfg.get("name", "LongAccount")

        # Сохраняем API ключи для WebSocket
        websocket_keys = {
            "short": {
                "api_key": sa_cfg["api_key"], 
                "api_secret": sa_cfg["api_secret"],
                "name": short_acc_name
            },
            "long": {
                "api_key": la_cfg["api_key"], 
                "api_secret": la_cfg["api_secret"],
                "name": long_acc_name
            }
        }
      
        def get_pnl_from_tracker(side, symbol):
            api_key    = websocket_keys[side]["api_key"]
            api_secret = websocket_keys[side]["api_secret"]
            name       = websocket_keys[side]["name"]

            # Получаем (создаём) трекер по ключу api_key+symbol, но не стартуем его сразу
            tracker = get_or_create_pnl_tracker(api_key, api_secret, name, symbol)
            pnl = tracker.get_unrealized_pnl(symbol)
            if pnl is not None:
                logging.info(f"{name}: Получен unrealized PnL через WebSocket: {pnl}")
            else:
                logging.warning(f"{name}: Нет данных unrealized PnL для {symbol}")
            return pnl
            
            return None
        
        try:
            # Функция депозита в фоне
            def do_deposits():
                parent = AuthenticationClient(cfg["api"]["key"], cfg["api"]["secret"])
                for side in ("short_account", "long_account"):
                    acc = pair_cfg[side]
                    acc_name = acc["name"]
                    # трейдер для саб-акка
                    sub_trader = BackpackTrader(acc["api_key"], acc["api_secret"])
                    
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
                                
                                # вместо двух логов — один, и считаем 1.000000 == 1
                                mismatch = abs(float(actual_amount) - float(qty_str)) > 0
                                logging.info(
                                    f"[Deposit] {acc_name}: requested={qty_str} USDC, actual={actual_amount} USDC, "
                                    f"id={tx_id}, status={status}"
                                    f"{' – AMOUNT MISMATCH!' if mismatch else ''}"
                                )
                            else:
                                logging.info(f"[Deposit] {acc_name}: deposited {qty_str} USDC, unexpected response format: {result}")
                            
                            # Задержка между депозитами с логированием
                            sleep_time = random.uniform(gmin, gmax)
                            logging.info(f"[Deposit] {acc_name}: sleeping for {sleep_time:.2f}s before next action")
                            time.sleep(sleep_time)
                            
                            # --- Проверяем баланс суб-акка через max_deviation из конфига и, при необходимости, ресетим ---
                            # читаем допустимую дельту (доля от initial_dep), по умолчанию 1%
                            max_dev = float(cfg.get("deviation", {}).get("max_deviation", 0.01))
                            # получаем текущий доступный баланс суб-акка
                            sub_bal = float(sub_trader.get_available_margin() or 0)
                            expected = initial_dep
                            tol = expected * max_dev
                            if abs(sub_bal - expected) > tol:
                                # 1) формируем строку для свипа из текущего баланса sub-акка
                                sweep_qty = f"{sub_bal:.6f}"
                                logging.warning(
                                    f"[Deposit] {acc_name}: баланс {sub_bal:.6f} выходит за пределы "
                                    f"[{expected - tol:.6f}…{expected + tol:.6f}], инициируем sweep qty={sweep_qty}"
                                )
                                # делаем свип с sub-акка на главный аккаунт
                                sub_client = AuthenticationClient(acc["api_key"], acc["api_secret"])
                                res = sub_client.request_withdrawal(
                                    address=cfg["main_account"]["address"],
                                    blockchain="Solana",
                                    quantity=sweep_qty,
                                    symbol="USDC"
                                )
                                tx_id = res.get("id", "")
                                logging.info(f"[Sweep] {acc_name}: initiated sweep tx={tx_id}, qty={sweep_qty}")

                                # Вместо опроса статуса — просто короткая пауза
                                sleep_time = random.uniform(
                                    cfg["general_delay"]["min"],
                                    cfg["general_delay"]["max"]
                                )
                                logging.info(f"[Sleep] пауза {sleep_time:.2f}s после sweep")
                                time.sleep(sleep_time)

                                # 2) формируем строку для повторного депозита
                                retry_qty = f"{initial_dep:.6f}"
                                logging.info(f"[Deposit] {acc_name}: после sweep делаем депозит qty={retry_qty}")
                                res2 = parent.request_withdrawal(
                                    address=acc["address"],
                                    blockchain="Solana",
                                    quantity=retry_qty,
                                    symbol="USDC"
                                )
                                tx2 = res2.get("id", "")
                                logging.info(f"[Deposit] {acc_name}: повторный депозит qty={retry_qty}, tx={tx2}")

                                # Вместо ожидания подтверждения — короткая рандомная пауза перед следующим действием
                                sleep_time = random.uniform(
                                    cfg["general_delay"]["min"],
                                    cfg["general_delay"]["max"]
                                )
                                logging.info(f"[Sleep] пауза {sleep_time:.2f}s после повторного депозита")
                                time.sleep(sleep_time)

                                # выход из цикла попыток
                                break
                            else:
                                logging.info(f"[Deposit] {acc_name}: баланс {sub_bal:.6f} в пределах допустимого ±{tol:.6f}")
                                break
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


        # 5) Monitor until both positions closed or PnL difference condition met
        max_monitor_time = 3600 * 24  # 24 hours maximum monitoring time
        start_monitoring = time.time()
        # Интервал между проверками из конфига, по умолчанию 30
        check_int = int(cfg.get("check_interval", 30))

        # Получаем настройки для логики закрытия по PnL
        pnl_diff_check_delay = int(cfg.get("pnl_diff", {}).get("check_delay_seconds", 10))
        pnl_diff_min_threshold = float(cfg.get("pnl_diff", {}).get("min_threshold", -1.0))
        pnl_diff_max_threshold = float(cfg.get("pnl_diff", {}).get("max_threshold", 2.0))
        pnl_diff_recheck_interval = int(cfg.get("pnl_diff", {}).get("recheck_interval_seconds", 30))

        # Вспомогательная функция для получения PnL из трекера веб-сокетов
        def get_pnl_from_tracker(side, symbol):
            """
            Получает unrealized PnL из соответствующего трекера.
            
            Args:
                side: "short" или "long" для указания нужного трекера
                symbol: Символ торговой пары
                
            Returns:
                float: unrealized PnL или None, если данные недоступны
            """
            global ws_pnl_trackers
            
            # Получаем ключ трекера из конфигурации
            if side == "short":
                api_key = sa_cfg["api_key"]
            else:
                api_key = la_cfg["api_key"]
            
            # Проверяем наличие трекера
            if api_key in ws_pnl_trackers:
                tracker = ws_pnl_trackers[api_key]
                pnl = tracker.get_unrealized_pnl(symbol)
                if pnl is not None:
                    acc_name = short_acc_name if side == "short" else long_acc_name
                    logging.info(f"{acc_name}: Получен unrealized PnL через WebSocket: {pnl}")
                    return pnl
            
            return None

        # Флаги для отслеживания состояния позиций
        was_short_active = True  # Изначально считаем обе позиции активными
        was_long_active = True
        closed_position_pnl = None
        closed_position_account = None
        time_position_closed = None

        while time.time() - start_monitoring < max_monitor_time:
            short_pos = short_tr.get_position(symbol)
            long_pos = long_tr.get_position(symbol)
            
            # Check if positions exist
            short_active = short_pos and abs(float(short_pos.get("netQuantity", 0) or 0)) > 0.01
            long_active = long_pos and abs(float(long_pos.get("netQuantity", 0) or 0)) > 0.01
            
            # Log position details if active
            if short_active:
                s = float(short_pos.get("netQuantity", 0))
                e = float(short_pos.get("entryPrice", 0))
                m = float(short_pos.get("markPrice", 0))
                l = short_pos.get("estLiquidationPrice", "Unknown")
                
                # Пытаемся получить unrealized PnL через веб-сокеты
                websocket_pnl = get_pnl_from_tracker("short", symbol)
                if websocket_pnl is not None:
                    pnl = websocket_pnl
                else:
                    # Если через веб-сокеты не удалось - используем значение из API
                    pnl = float(short_pos.get("unrealizedPnl", 0) or 0)
                
                logging.info(f"{short_acc_name}: Short position: size={s:.2f}, entry={e}, mark={m}, liquidation={l}, PnL={pnl}")
            else:
                logging.info(f"{short_acc_name}: No active short position found")
            
            if long_active:
                s = float(long_pos.get("netQuantity", 0))
                e = float(long_pos.get("entryPrice", 0))
                m = float(long_pos.get("markPrice", 0))
                l = long_pos.get("estLiquidationPrice", "Unknown")
                
                # Пытаемся получить unrealized PnL через веб-сокеты
                websocket_pnl = get_pnl_from_tracker("long", symbol)
                if websocket_pnl is not None:
                    pnl = websocket_pnl
                else:
                    # Если через веб-сокеты не удалось - используем значение из API
                    pnl = float(long_pos.get("unrealizedPnl", 0) or 0)
                    
                logging.info(f"{long_acc_name}: Long position: size={s:.2f}, entry={e}, mark={m}, liquidation={l}, PnL={pnl}")
            else:
                logging.info(f"{long_acc_name}: No active long position found")
            
            # Проверяем, закрылась ли одна из позиций (но не обе)
            if was_short_active and not short_active and long_active:
                logging.info(f"{short_acc_name}: Позиция закрылась, запускаем логику проверки PnL")
                time_position_closed = time.time()
                closed_position_account = "short"
                was_short_active = False
            elif was_long_active and not long_active and short_active:
                logging.info(f"{long_acc_name}: Позиция закрылась, запускаем логику проверки PnL")
                time_position_closed = time.time()
                closed_position_account = "long"
                was_long_active = False
            
            # Логика проверки PnL если одна позиция закрылась
            if time_position_closed is not None and closed_position_account is not None:
                # Определяем объекты для закрытой и открытой позиций
                closed_trader = short_tr if closed_position_account == "short" else long_tr
                closed_acc_name = short_acc_name if closed_position_account == "short" else long_acc_name
                open_trader = long_tr if closed_position_account == "short" else short_tr
                open_acc_name = long_acc_name if closed_position_account == "short" else short_acc_name
                open_position = long_pos if closed_position_account == "short" else short_pos
                open_side = "long" if closed_position_account == "short" else "short"
                
                # Проверяем, прошло ли нужное время после закрытия позиции
                time_since_close = time.time() - time_position_closed
                
                if time_since_close >= pnl_diff_check_delay and closed_position_pnl is None:
                    # Получаем PnL закрытой позиции через API settlement
                    closed_position_pnl = get_closed_position_pnl(closed_trader, closed_acc_name, symbol)
                    logging.info(f"{closed_acc_name}: Получен PnL закрытой позиции из settlement API: {closed_position_pnl}")
                    
                    # Если не удалось получить, попробуем другим методом
                    if closed_position_pnl is None:
                        logging.warning(f"{closed_acc_name}: Не удалось получить PnL через settlement API, используем альтернативный метод")
                        pnl_value, _ = get_account_pnl_data(closed_trader, closed_acc_name, symbol)
                        closed_position_pnl = pnl_value
                        logging.info(f"{closed_acc_name}: Альтернативный PnL закрытой позиции: {closed_position_pnl}")
                        
                        # Если все равно не удалось получить PnL
                        if closed_position_pnl is None:
                            logging.warning(f"{closed_acc_name}: Не удалось получить PnL. Используем последнее известное значение.")
                            # Если у нас нет PnL, можно использовать альтернативную логику
                            if not open_active:  # Если открытая позиция тоже закрылась
                                logging.info("Обе позиции уже закрыты. Продолжаем без проверки PnL.")
                                time_position_closed = None
                                closed_position_pnl = None
                                closed_position_account = None
                                continue
                
                # Если уже получили PnL закрытой позиции, проверяем условия
                if closed_position_pnl is not None:
                    # Пытаемся получить unrealized PnL через веб-сокеты
                    websocket_pnl = get_pnl_from_tracker(open_side, symbol)
                    if websocket_pnl is not None:
                        open_pnl = websocket_pnl
                        logging.info(f"{open_acc_name}: Unrealized PnL через WebSocket: {open_pnl:.6f}")
                    else:
                        # Фолбэк: считаем PnL "вручную" по позиции из API
                        size  = float(open_position.get("netQuantity", 0) or 0)
                        entry = float(open_position.get("entryPrice",   0) or 0)
                        mark  = float(open_position.get("markPrice",    0) or 0)
                        # для лонга PnL = (mark – entry) * size, для шорта — наоборот
                        if open_side == "long":
                            manual_pnl = (mark - entry) * size
                        else:
                            manual_pnl = (entry - mark) * abs(size)
                        open_pnl = manual_pnl
                        logging.info(
                            f"{open_acc_name}: Manual unrealized PnL: "
                            f"size={size:.4f}, entry={entry:.2f}, mark={mark:.2f} -> PnL={open_pnl:.6f}"
                        )
                    
                    # Рассчитываем разницу (берем абсолютные значения для сравнения)
                    pnl_diff = abs(closed_position_pnl) - abs(open_pnl)
                    logging.info(f"Разница PnL: |{closed_position_pnl}| - |{open_pnl}| = {pnl_diff}")
                    
                    # Проверяем условие закрытия в зависимости от знака разницы
                    if pnl_diff < 0 and pnl_diff < pnl_diff_min_threshold:
                        # Отрицательная разница ниже порога - закрываем позицию
                        logging.warning(f"Отрицательная разница PnL ({pnl_diff}) ниже минимального порога ({pnl_diff_min_threshold}). Закрываем оставшуюся позицию.")
                        close_success = close_position_market(open_trader, symbol, open_position, open_acc_name)
                        if close_success:
                            logging.info(f"{open_acc_name}: Позиция успешно закрыта по условию минимальной разницы PnL")
                        else:
                            logging.error(f"{open_acc_name}: Не удалось закрыть позицию по условию минимальной разницы PnL")
                        # Независимо от результата, считаем логику завершенной
                        time_position_closed = None
                        closed_position_pnl = None
                        closed_position_account = None
                    elif pnl_diff > 0 and pnl_diff > pnl_diff_max_threshold:
                        # Положительная разница выше порога - закрываем позицию
                        logging.warning(f"Положительная разница PnL ({pnl_diff}) выше максимального порога ({pnl_diff_max_threshold}). Закрываем оставшуюся позицию.")
                        close_success = close_position_market(open_trader, symbol, open_position, open_acc_name)
                        if close_success:
                            logging.info(f"{open_acc_name}: Позиция успешно закрыта по условию максимальной разницы PnL")
                        else:
                            logging.error(f"{open_acc_name}: Не удалось закрыть позицию по условию максимальной разницы PnL")
                        # Независимо от результата, считаем логику завершенной
                        time_position_closed = None
                        closed_position_pnl = None
                        closed_position_account = None
                    else:
                        # Если разница в допустимых пределах, ждем следующей проверки
                        if pnl_diff < 0:
                            logging.info(f"Отрицательная разница PnL ({pnl_diff}) в допустимых пределах (выше {pnl_diff_min_threshold})")
                        else:
                            logging.info(f"Положительная разница PnL ({pnl_diff}) в допустимых пределах (ниже {pnl_diff_max_threshold})")
                        # Подготавливаем время для следующей проверки
                        time_position_closed = time.time() - pnl_diff_check_delay + pnl_diff_recheck_interval
            
            # Check if both positions are closed
            if not short_active and not long_active:
                # Double-check with a small delay to make sure it's not just API flakiness
                time.sleep(3)
                
                # Re-check positions
                short_pos = short_tr.get_position(symbol)
                long_pos = long_tr.get_position(symbol)
                
                short_active = short_pos and abs(float(short_pos.get("netQuantity", 0) or 0)) > 0.01
                long_active = long_pos and abs(float(long_pos.get("netQuantity", 0) or 0)) > 0.01
                
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
    Получает данные о PnL и комиссиях для последней закрытой позиции.
    
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
        
        # Проверяем текущую позицию
        pos = trader.get_position(symbol)
        if pos and isinstance(pos, dict):
            logging.info(f"{acc_name}: Данные позиции: {pos}")
            
            # Проверяем realizedPnl в текущей позиции
            if "realizedPnl" in pos and pos.get("realizedPnl") is not None:
                pnl = float(pos.get("realizedPnl", 0) or 0)
                logging.info(f"{acc_name}: Реализованный PnL из позиции: {pnl:.6f}")
            
            # Проверяем комиссии
            if "commission" in pos:
                fees = float(pos.get("commission", 0) or 0)
                logging.info(f"{acc_name}: Комиссии из позиции: {fees:.6f}")
        else:
            logging.warning(f"{acc_name}: Не удалось получить данные о позиции {symbol}")
        
        # Если не получили PnL из текущей позиции, пробуем запросить историю позиций
        if pnl == 0:
            try:
                # Пробуем использовать правильный эндпоинт из документации
                positions_history = trader.auth._send_request(
                    "GET", 
                    "api/v1/positions/history", 
                    "positionsHistoryQuery", 
                    {"symbol": symbol, "limit": 1}
                )
                
                if positions_history and isinstance(positions_history, (list, dict)):
                    data = positions_history
                    if isinstance(positions_history, dict) and "data" in positions_history:
                        data = positions_history["data"]
                    
                    if data and len(data) > 0:
                        last_position = data[0]
                        logging.info(f"{acc_name}: Получена последняя позиция из истории: {last_position}")
                        
                        # Извлекаем PnL
                        if "realizedPnl" in last_position:
                            pnl = float(last_position.get("realizedPnl", 0) or 0)
                            logging.info(f"{acc_name}: PnL из истории позиций: {pnl:.6f}")
                        
                        # Извлекаем комиссии
                        if "commission" in last_position or "fee" in last_position:
                            fees = float(last_position.get("commission", 0) or last_position.get("fee", 0) or 0)
                            logging.info(f"{acc_name}: Комиссии из истории позиций: {fees:.6f}")
            except Exception as e:
                logging.warning(f"{acc_name}: Ошибка при запросе истории позиций: {e}")
        
        # Если комиссии все еще не найдены, используем типичную оценку
        if fees == 0.0 and pnl != 0.0:
            fees = abs(pnl) * 0.001  # Примерно 0.1% от объема торгов
            logging.warning(f"{acc_name}: Используем оценочные комиссии: {fees:.6f}")
        
        # Последняя попытка - запрашиваем баланс и сравниваем с предыдущим
        if pnl == 0.0:
            try:
                # Запрашиваем баланс
                balance_info = trader.auth._send_request(
                    "GET", 
                    "api/v1/capital", 
                    "capitalQuery", 
                    {}
                )
                
                logging.info(f"{acc_name}: Данные баланса: {balance_info}")
                # Здесь можно добавить логику для вычисления PnL на основе изменений баланса
                # Но для этого нужно знать предыдущий баланс
            except Exception as e:
                logging.warning(f"{acc_name}: Ошибка при запросе баланса: {e}")
        
        logging.info(f"{acc_name}: Итоговые данные - PnL={pnl:.6f}, Комиссии={fees:.6f}")
        return pnl, fees
        
    except Exception as e:
        logging.error(f"{acc_name}: Не удалось получить данные PnL: {e}")
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

