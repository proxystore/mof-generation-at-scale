import functools
from typing import Any
from typing import Callable
from typing import ParamSpec
from typing import TypeVar

T = TypeVar("T")
P = ParamSpec("P")


def patch_logging(func: Callable[P, T]) -> Callable[P, T]:
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
        import logging

        logger = logging.getLogger("worker_log")
        logger.setLevel(logging.DEBUG)
        from proxystore.store import base

        base.logger = logger

        return func(*args, **kwargs)

    return wrapper


def apply_into_owned(
    config: dict[str, Any],
    proxy_result: bool,
    ownership: bool,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            from proxystore.proxy import Proxy
            from proxystore.store.base import Store
            from proxystore.store import register_store, get_store
            from proxystore.store.ref import into_owned

            args = (into_owned(arg) if isinstance(arg, Proxy) else arg for arg in args)
            kwargs = {
                key: into_owned(value) if isinstance(value, Proxy) else value
                for key, value in kwargs.items()
            }

            result = func(*args, **kwargs)

            if proxy_result and not isinstance(result, Proxy):
                store = get_store(config["name"])
                if store is None:
                    store = Store.from_config(config)
                    register_store(store)
                if ownership:
                    result = store.owned_proxy(result)
                else:
                    result = store.proxy(result)

            return result

        return wrapper

    return decorator


def apply_into_owned_generator(
    config: dict[str, Any],
    proxy_result: bool,
    ownership: bool,
) -> Callable[[Callable[P, T]], Callable[P, T]]:
    def decorator(func: Callable[P, T]) -> Callable[P, T]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            from proxystore.proxy import Proxy
            from proxystore.store.base import Store
            from proxystore.store import register_store, get_store
            from proxystore.store.ref import into_owned

            args = (into_owned(arg) if isinstance(arg, Proxy) else arg for arg in args)
            kwargs = {
                key: into_owned(value) if isinstance(value, Proxy) else value
                for key, value in kwargs.items()
            }

            for result in func(*args, **kwargs):
                if proxy_result and not isinstance(result, Proxy):
                    store = get_store(config["name"])
                    if store is None:
                        store = Store.from_config(config)
                        register_store(store)
                    if ownership:
                        result = store.owned_proxy(result)
                    else:
                        result = store.proxy(result)
                yield result

        return wrapper

    return decorator
