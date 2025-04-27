#!/usr/bin/env python3
"""
sweep_subaccounts.py

Скрипт закрывает позицию на заданном рынке и выводит USDC на главный аккаунт,
используя логику из savplux/backpack_liquidation (классы BackpackTrader).
"""
import yaml
import sys
import time
import random
import logging
from proxy_handler import init_proxy_from_config, patch_sdk_clients

def sweep_all(cfg_path: str):
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Читаем конфиг
    with open(cfg_path, 'r') as f:
        cfg = yaml.safe_load(f)

    # Инициализируем и патчим SDK для работы через прокси
    proxy_url = init_proxy_from_config(cfg)
    if proxy_url:
        logging.info(f"[Proxy] enabled: {proxy_url}")
    else:
        logging.info("[Proxy] disabled")
    patch_sdk_clients(proxy_url)

    # Загрузка задержек из конфига
    general_delay_cfg = cfg.get('general_delay', {})
    global general_DELAY_MIN, general_DELAY_MAX
    general_DELAY_MIN = general_delay_cfg.get('min', 0)
    general_DELAY_MAX = general_delay_cfg.get('max', 0)

# Задержки между действиями, загружаются из конфига
general_DELAY_MIN = 0
general_DELAY_MAX = 0

# Принудительно устанавливаем кодировку вывода в utf-8 (для Windows)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

from backpack_liquidation_bot import BackpackTrader as Client

# Константы
USDC_SYMBOL = 'USDC'
BLOCKCHAIN = 'Solana'
MIN_BALANCE = 0  # Минимальный баланс для операций


def close_market_position(client: Client, symbol: str) -> bool:
    """Закрывает открытую позицию на заданном рынке рыночным ордером (reduceOnly)."""
    try:
        positions = client.auth._send_request(
            "GET", "api/v1/position", "positionQuery", {}
        )
    except Exception as e:
        logging.error(f"[Error] fetching positions: {e}")
        return False
    for pos in positions:
        if pos.get("symbol") != symbol:
            continue
        size = float(pos.get("netQuantity", 0))
        if size == 0:
            logging.info(f"[Info] no position for {symbol}")
            return True
        side = "Ask" if size > 0 else "Bid"
        try:
            client.auth.execute_order(
                orderType="Market",
                side=side,
                symbol=symbol,
                quantity=str(abs(size)),
                reduceOnly=True
            )
            logging.info(f"[Closed] {symbol}, size={size}")
            # Задержка после закрытия ордера
            time.sleep(random.uniform(general_DELAY_MIN, general_DELAY_MAX))
            return True
        except Exception as err:
            logging.error(f"[Error] closing position {symbol}: {err}")
            return False
    logging.info(f"[Info] symbol {symbol} not in positions")
    return True


def sweep_usdc(client: Client, main_address: str, max_attempts: int = 8) -> bool:
    """Выводит весь доступный USDC на главный адрес с повторными попытками."""
    balance = client.get_available_margin()
    logging.info(f"[Balance] available margin: {balance:.6f} {USDC_SYMBOL}")
    if balance <= MIN_BALANCE:
        logging.info(f"[Skip] balance <= {MIN_BALANCE} {USDC_SYMBOL}")
        return True
    
    success = _attempt_sweep(client, main_address, balance, max_attempts)
    
    # Проверяем баланс после первой попытки свипа
    if success:
        time.sleep(2)  # Даем время на обновление баланса
        remaining = client.get_available_margin()
        logging.info(f"[Check] remaining balance after sweep: {remaining:.6f} {USDC_SYMBOL}")
        
        # Если остались средства, пробуем свипнуть их снова
        if remaining > MIN_BALANCE:
            logging.info(f"[Retry] sweeping remaining {remaining:.6f} {USDC_SYMBOL}")
            success = _attempt_sweep(client, main_address, remaining, max_attempts)
            
            # Финальная проверка
            if success:
                time.sleep(2)
                final_balance = client.get_available_margin()
                logging.info(f"[Final] balance after all sweeps: {final_balance:.6f} {USDC_SYMBOL}")
                if final_balance > MIN_BALANCE:
                    logging.warning(f"[Warning] still have {final_balance:.6f} {USDC_SYMBOL} remaining")
    
    return success


def _attempt_sweep(client: Client, main_address: str, initial_balance: float, max_attempts: int) -> bool:
    """Вспомогательная функция для попыток свипа заданной суммы."""
    qty = round(initial_balance, 6)
    qty_str = f"{qty:.6f}"
    
    for attempt in range(1, max_attempts + 1):
        try:
            client.auth.request_withdrawal(
                address=main_address,
                blockchain=BLOCKCHAIN,
                quantity=qty_str,
                symbol=USDC_SYMBOL
            )
            logging.info(f"[Swept] {qty_str} {USDC_SYMBOL} to main account (attempt {attempt}/{max_attempts})")
            return True
        except Exception as e:
            logging.warning(f"[Warn] withdrawal {attempt}/{max_attempts} failed: {e}")
            if "Insufficient collateral" in str(e):
                qty *= 0.5
                if qty < MIN_BALANCE:
                    logging.error(f"[Error] remaining USDC too low (<{MIN_BALANCE}) to continue sweep")
                    return False
                qty_str = f"{qty:.6f}"
            delay = min(2 ** (attempt - 1), 30)
            time.sleep(delay * random.uniform(1, 1.5))
    
    logging.error(f"[Error] failed to sweep after {max_attempts} attempts")
    return False


def sweep_all(cfg_path: str):
    # Настройка логирования: понятный формат с ASCII-символами
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    with open(cfg_path, 'r') as f:
        cfg = yaml.safe_load(f)

    # Загрузка задержек из конфига
    general_delay_cfg = cfg.get('general_delay', {})
    global general_DELAY_MIN, general_DELAY_MAX
    general_DELAY_MIN = general_delay_cfg.get('min', 0)
    general_DELAY_MAX = general_delay_cfg.get('max', 0)

    main_address = cfg.get('main_account', {}).get('address')
    if not main_address:
        logging.error("[Error] main account address not set in config")
        sys.exit(1)

    symbol = cfg.get('symbol')
    if not symbol:
        logging.error("[Error] symbol not set in config")
        sys.exit(1)

    sweep_attempts = int(cfg.get('sweep_attempts', 8))

    subaccounts = []
    for pair in cfg.get('pairs', []):
        for role in ('short_account', 'long_account'):
            acct = pair.get(role)
            if acct:
                subaccounts.append(acct)

    for acct in subaccounts:
        name = acct.get('name', 'unknown')
        logging.info(f"[Start] processing subaccount '{name}'")
        client = Client(acct['api_key'], acct['api_secret'])

        if not close_market_position(client, symbol):
            logging.error(f"[Error] failed to close position on '{name}'")
        if not sweep_usdc(client, main_address, max_attempts=sweep_attempts):
            logging.error(f"[Error] failed to sweep USDC from '{name}'")

    logging.info("[Done] sweep completed successfully.")


if __name__ == '__main__':
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else 'config.yaml'
    sweep_all(cfg_path)

