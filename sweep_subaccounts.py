#!/usr/bin/env python3
"""
sweep_subaccounts.py

Скрипт последовательно и параллельно обрабатывает аккаунты (пары):
- Пары аккаунтов запускаются в отдельных потоках.
- Первый поток стартует сразу, последующие — с задержкой до pair_start_delay_max секунд.
- При ошибках при получении позиций или закрытии — ретраи, чтобы не оставить висячие позиции.
- Внутренние варнинги при получении баланса подавляются, и при нулевом балансе сразу выводится сообщение о пропуске свипа.
"""
import yaml
import sys
import time
import random
import logging
import threading
from proxy_handler import init_proxy_from_config, patch_sdk_clients

# Константы
USDC_SYMBOL = 'USDC'
BLOCKCHAIN = 'Solana'
MIN_BALANCE = 0  # Минимальный баланс для операций
FETCH_RETRIES = 3
FETCH_RETRY_DELAY = 2  # seconds
CLOSE_RETRIES = 2
CLOSE_RETRY_DELAY = 2  # seconds

# Задержки между действиями (будут загружены из конфига)
general_DELAY_MIN = 0
general_DELAY_MAX = 0


def safe_get_available_margin(client) -> float:
    """Получить доступный баланс без внутренних варнов. Возвращает 0.0 при ошибках."""
    root_logger = logging.getLogger()
    prev_level = root_logger.level
    root_logger.setLevel(logging.ERROR)
    try:
        margin = client.get_available_margin()
        return float(margin) if margin is not None else 0.0
    except Exception:
        return 0.0
    finally:
        root_logger.setLevel(prev_level)


def close_market_position(client, symbol: str) -> bool:
    """Закрывает открытую позицию на заданном рынке рыночным ордером (reduceOnly) с ретраями при ошибках."""
    for attempt in range(1, FETCH_RETRIES + 1):
        try:
            logging.info(f"[Action] Fetching positions for {symbol} (attempt {attempt}/{FETCH_RETRIES})")
            positions = client.auth._send_request(
                "GET", "api/v1/position", "positionQuery", {}
            )
            break
        except Exception as e:
            logging.error(f"[Error] Fetching positions: {e}")
            if attempt < FETCH_RETRIES:
                time.sleep(FETCH_RETRY_DELAY)
            else:
                return False

    for pos in positions:
        if pos.get("symbol") != symbol:
            continue
        size = float(pos.get("netQuantity", 0))
        if size == 0:
            logging.info(f"[Info] No position for {symbol}")
            return True
        side = "Ask" if size > 0 else "Bid"
        for close_attempt in range(1, CLOSE_RETRIES + 2):
            try:
                logging.info(f"[Action] Closing position {symbol}, size={size}, side={side} (try {close_attempt})")
                client.auth.execute_order(
                    orderType="Market",
                    side=side,
                    symbol=symbol,
                    quantity=str(abs(size)),
                    reduceOnly=True
                )
                logging.info(f"[Success] Closed {symbol} position, size={size}")
                # задержка после закрытия
                if general_DELAY_MAX > 0:
                    delay = random.uniform(general_DELAY_MIN, general_DELAY_MAX)
                    logging.info(f"[Delay] Waiting {delay:.2f}s after closing position")
                    time.sleep(delay)
                return True
            except Exception as err:
                logging.error(f"[Error] Closing position {symbol}: {err}")
                if close_attempt <= CLOSE_RETRIES:
                    time.sleep(CLOSE_RETRY_DELAY)
                else:
                    return False
    logging.info(f"[Info] Symbol {symbol} not in positions")
    return True


def _attempt_sweep(client, main_address: str, initial_balance: float, max_attempts: int) -> bool:
    qty = round(initial_balance, 6)
    qty_str = f"{qty:.6f}"
    for attempt in range(1, max_attempts + 1):
        logging.info(f"[Action] Attempting withdrawal {attempt}/{max_attempts}: {qty_str} {USDC_SYMBOL}")
        try:
            client.auth.request_withdrawal(
                address=main_address,
                blockchain=BLOCKCHAIN,
                quantity=qty_str,
                symbol=USDC_SYMBOL
            )
            logging.info(f"[Success] Swept {qty_str} {USDC_SYMBOL} (attempt {attempt}/{max_attempts})")
            return True
        except Exception as e:
            logging.warning(f"[Warn] Withdrawal failed: {e}")
            if "Insufficient collateral" in str(e):
                qty *= 0.5
                if qty < MIN_BALANCE:
                    logging.error("[Error] Remaining USDC too low to continue sweep")
                    return False
                qty_str = f"{qty:.6f}"
                logging.info(f"[Adjust] New withdrawal amount: {qty_str}")
            delay = min(2 ** (attempt - 1), 30) * random.uniform(1, 1.5)
            time.sleep(delay)
    logging.error(f"[Error] Failed to sweep after {max_attempts} attempts")
    return False


def sweep_usdc(client, main_address: str, max_attempts: int = 8) -> bool:
    balance = safe_get_available_margin(client)
    logging.info(f"[Balance] Available: {balance:.6f} {USDC_SYMBOL}")
    if balance <= MIN_BALANCE:
        logging.info("[Skip] Balance is zero")
        return True
    if not _attempt_sweep(client, main_address, balance, max_attempts):
        return False
    remaining = safe_get_available_margin(client)
    logging.info(f"[Check] Remaining after sweep: {remaining:.6f} {USDC_SYMBOL}")
    if remaining > MIN_BALANCE:
        logging.info("[Retry] Sweeping remaining funds")
        return _attempt_sweep(client, main_address, remaining, max_attempts)
    return True


def process_pair(pair: dict, symbol: str, main_address: str, sweep_attempts: int):
    pair_name = pair.get('name', 'Unnamed Pair')
    logging.info(f"[Start] Processing pair '{pair_name}' in {threading.current_thread().name}")
    from backpack_liquidation_bot import BackpackTrader as Client
    clients = {}
    for role in ('long', 'short'):
        acct = pair.get(f"{role}_account")
        if acct:
            try:
                clients[role] = Client(acct['api_key'], acct['api_secret'])
            except Exception as e:
                logging.error(f"[Error] Init client for {role}: {e}")
    if not clients:
        logging.warning(f"[Warning] No accounts for '{pair_name}'")
        return
    # Закрываем позиции с ретраями
    for role, client in clients.items():
        if not close_market_position(client, symbol):
            logging.error(f"[Error] Could not close positions for '{role}' in '{pair_name}', skip sweep")
            return
    # Пауза
    if general_DELAY_MAX > 0:
        time.sleep(random.uniform(general_DELAY_MIN, general_DELAY_MAX))
    # Выводим
    for role, client in clients.items():
        if not sweep_usdc(client, main_address, max_attempts=sweep_attempts):
            logging.error(f"[Error] Sweep failed for '{role}' in '{pair_name}'")
    logging.info(f"[Complete] Pair '{pair_name}' done")


def sweep_all(cfg_path: str):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler()]
    )
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = yaml.safe_load(f)
    proxy_url = init_proxy_from_config(cfg)
    patch_sdk_clients(proxy_url)
    global general_DELAY_MIN, general_DELAY_MAX
    gd = cfg.get('general_delay', {})
    general_DELAY_MIN, general_DELAY_MAX = gd.get('min', 0), gd.get('max', 0)
    pair_delay = cfg.get('pair_start_delay_max', 0)
    main_address = cfg.get('main_account', {}).get('address')
    symbol = cfg.get('symbol')
    attempts = int(cfg.get('sweep_attempts', 8))
    pairs = cfg.get('pairs', [])
    threads = []
    for i, pair in enumerate(pairs):
        t = threading.Thread(target=process_pair, args=(pair, symbol, main_address, attempts), name=f"PairThread-{i+1}")
        t.start(); threads.append(t)
        if i < len(pairs)-1 and pair_delay>0:
            time.sleep(random.uniform(0, pair_delay))
    for t in threads: t.join()
    logging.info("[Done] All pairs processed successfully")


if __name__ == '__main__':
    cfg_path = sys.argv[1] if len(sys.argv)>1 else 'config.yaml'
    sweep_all(cfg_path)


