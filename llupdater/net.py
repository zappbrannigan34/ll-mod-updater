from typing import Any


def proxy_enabled(settings: dict[str, Any]) -> bool:
    return bool(settings.get("proxy_enabled", False))


def proxy_url(settings: dict[str, Any]) -> str:
    return str(settings.get("proxy_url") or "").strip()


def validate_proxy_settings(settings: dict[str, Any]) -> None:
    if proxy_enabled(settings) and not proxy_url(settings):
        raise ValueError("Proxy is enabled but proxy_url is empty")


def get_proxies(settings: dict[str, Any]) -> dict[str, str]:
    validate_proxy_settings(settings)
    if not proxy_enabled(settings):
        return {}
    url = proxy_url(settings)
    return {"http": url, "https": url}


def apply_network_to_session(session: Any, settings: dict[str, Any]) -> Any:
    if proxy_enabled(settings):
        proxies = get_proxies(settings)
        session.trust_env = False
        session.proxies.update(proxies)
    return session
