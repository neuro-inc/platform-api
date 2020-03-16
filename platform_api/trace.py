import functools
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, AsyncIterator, Awaitable, Callable

import aiozipkin
from aiohttp import web
from aiozipkin.span import SpanAbc


Handler = Callable[[web.Request], Awaitable[web.StreamResponse]]


CURRENT_TRACER: ContextVar[aiozipkin.Tracer] = ContextVar("CURRENT_TRACER")
CURRENT_SPAN: ContextVar[SpanAbc] = ContextVar("CURRENT_SPAN")


@asynccontextmanager
async def tracing_cm(name: str) -> AsyncIterator[SpanAbc]:
    tracer = CURRENT_TRACER.get()
    try:
        span = CURRENT_SPAN.get()
        child = tracer.new_child(span.context)
    except LookupError:
        child = tracer.new_trace(sampled=False)
    reset_token = CURRENT_SPAN.set(child)
    try:
        with child:
            child.name(name)
            yield child
    finally:
        CURRENT_SPAN.reset(reset_token)


def trace(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @functools.wraps(func)
    async def tracer(*args: Any, **kwargs: Any) -> Any:
        async with tracing_cm(func.__name__):
            return await func(*args, **kwargs)

    return tracer


@web.middleware
async def store_span_middleware(
    request: web.Request, handler: Handler
) -> web.StreamResponse:
    tracer = aiozipkin.get_tracer(request.app)
    span = aiozipkin.request_span(request)
    CURRENT_TRACER.set(tracer)
    CURRENT_SPAN.set(span)
    return await handler(request)
