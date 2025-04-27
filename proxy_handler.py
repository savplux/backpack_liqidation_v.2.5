import logging
import requests
import urllib3
from urllib.parse import urlparse
import base64
import json
import time

# Отключаем предупреждения SSL для тестирования прокси
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def setup_proxy_session(proxy_url=None, verify_ssl=False):
    """
    Создает и настраивает сессию requests с прокси.
    """
    session = requests.Session()
    session.verify = verify_ssl

    if not proxy_url:
        return session

    try:
        if proxy_url.startswith(('http://', 'https://', 'socks4://', 'socks5://')):
            protocol, rest = proxy_url.split('://', 1)
        else:
            protocol = 'http'
            rest = proxy_url

        if '@' in rest:
            auth_part, host_port = rest.split('@', 1)
            username, password = auth_part.split(':', 1) if ':' in auth_part else (auth_part, '')
        else:
            host_port = rest
            username = password = None

        proxy_full_url = f"{protocol}://{host_port}"
        session.proxies = {'http': proxy_full_url, 'https': proxy_full_url}

        if username and password:
            auth_obj = requests.auth.HTTPBasicAuth(username, password)
            session.auth = auth_obj
            auth_header = base64.b64encode(f"{username}:{password}".encode()).decode()
            session.headers.update({"Proxy-Authorization": f"Basic {auth_header}"})
            logging.info(f"Proxy authentication configured for user: {username}")
        else:
            logging.info("No proxy authentication provided")

        # Таймауты подключения и чтения
        session.timeout = (10, 30)

        safe_proxy = host_port
        logging.info(f"Proxy configured: {protocol}://{safe_proxy}")
    except Exception as e:
        logging.error(f"Failed to parse proxy URL: {e}")
        session = requests.Session()
        session.verify = verify_ssl

    return session

def test_proxy(session, max_retries=3):
    """
    Тестирует прокси-соединение.
    """
    test_urls = [
        "https://api.ipify.org?format=json",
        "https://httpbin.org/ip",
        "https://www.google.com",
        "https://example.com"
    ]

    for url in test_urls:
        for attempt in range(1, max_retries + 1):
            try:
                logging.info(f"Testing proxy with {url} (attempt {attempt}/{max_retries})")
                response = session.get(url, timeout=10, verify=False)
                if response.status_code == 200:
                    logging.info(f"Proxy test successful with {url}")
                    try:
                        result = response.json() if url.endswith('json') else {}
                        if 'ip' in result:
                            logging.info(f"Proxy IP: {result['ip']}")
                    except Exception:
                        pass
                    return True
                else:
                    logging.warning(f"Proxy test with {url} returned status {response.status_code}")
            except requests.exceptions.RequestException as e:
                logging.warning(f"Proxy test with {url} failed: {e}")

            if attempt < max_retries:
                time.sleep(2)

    logging.error("All proxy tests failed")
    return False

def patch_sdk_clients(proxy_url=None):
    """
    Модифицирует SDK клиенты для использования прокси
    """
    if not proxy_url:
        logging.info("No proxy URL provided, SDK clients will use direct connection")
        return False

    try:
        from backpack_exchange_sdk.authenticated import AuthenticationClient
        from backpack_exchange_sdk.public import PublicClient

        # Создаем и тестируем сессию
        test_session = setup_proxy_session(proxy_url, verify_ssl=False)
        if not test_proxy(test_session):
            logging.warning("Proxy tests failed, SDK clients will use direct connection")
            return False

        # Патчим AuthenticationClient
        original_auth_init = AuthenticationClient.__init__
        original_auth_send = getattr(AuthenticationClient, '_send_request', None)

        def patched_auth_init(self, api_key, api_secret, *args, **kwargs):
            original_auth_init(self, api_key, api_secret, *args, **kwargs)
            self.session = setup_proxy_session(proxy_url, verify_ssl=False)
            logging.info("AuthenticationClient initialized with proxy")

        AuthenticationClient.__init__ = patched_auth_init
        if original_auth_send:
            def patched_auth_send(self, *args, **kwargs):
                if not hasattr(self, 'session') or not self.session.proxies:
                    self.session = setup_proxy_session(proxy_url, verify_ssl=False)
                return original_auth_send(self, *args, **kwargs)
            AuthenticationClient._send_request = patched_auth_send
        else:
            logging.warning("AuthenticationClient._send_request not found; skipping send patch")

        # Патчим PublicClient
        original_public_init = PublicClient.__init__
        original_public_send = getattr(PublicClient, '_send_request', None)

        def patched_public_init(self, *args, **kwargs):
            original_public_init(self, *args, **kwargs)
            self.session = setup_proxy_session(proxy_url, verify_ssl=False)
            logging.info("PublicClient initialized with proxy")

        PublicClient.__init__ = patched_public_init
        if original_public_send:
            def patched_public_send(self, *args, **kwargs):
                if not hasattr(self, 'session') or not self.session.proxies:
                    self.session = setup_proxy_session(proxy_url, verify_ssl=False)
                return original_public_send(self, *args, **kwargs)
            PublicClient._send_request = patched_public_send
        else:
            logging.warning("PublicClient._send_request not found; skipping send patch")

        logging.info("SDK clients successfully patched for proxy usage")
        return True
    except Exception as e:
        logging.error(f"Failed to patch SDK clients: {e}")
        return False

def init_proxy_from_config(cfg):
    """
    Инициализирует прокси из настроек конфигурации
    """
    proxy_enabled = cfg.get("proxy", {}).get("enabled", False)
    proxy_url = cfg.get("proxy", {}).get("url", None)

    if not proxy_enabled or not proxy_url:
        logging.info("Proxy disabled in configuration")
        return None

    if '@' in proxy_url:
        _, host_port = proxy_url.split('@', 1)
    else:
        host_port = proxy_url

    if not proxy_url.startswith(('http://', 'https://', 'socks4://', 'socks5://')):
        proxy_url = f"http://{proxy_url}"
        logging.info(f"No protocol specified in proxy URL, assuming HTTP: {host_port}")

    logging.info(f"Proxy enabled: {host_port}")
    return proxy_url
