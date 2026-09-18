"""Capture and replay terminal-failure task messages."""
import weakref

from kombu.serialization import loads

from celery.exceptions import BackendGetMetaError
from celery.states import SUCCESS

__all__ = ('get_store', 'archive_request', 'replay_dead_letters')

_stores = weakref.WeakKeyDictionary()
_PUBLISH_PROPERTIES = frozenset({
    'correlation_id', 'reply_to', 'priority', 'delivery_mode', 'expiration',
    'message_id', 'timestamp', 'type', 'user_id', 'app_id', 'cluster_id',
})


def get_store(app, *, enabled_only=True):
    conf = app.conf
    if enabled_only and not conf.dead_letter_enabled:
        return None
    cache_key = (conf.dead_letter_store, conf.dead_letter_url)
    store = _stores.get(app)
    if store is not None and store[0] == cache_key:
        return store[1]
    from celery.utils.imports import symbol_by_name
    store_cls = symbol_by_name(conf.dead_letter_store)
    instance = store_cls(conf.dead_letter_url, app=app)
    _stores[app] = (cache_key, instance)
    return instance


def archive_request(request, exc, traceback, *, reason=None, store=None):
    """Archive a terminal-failure request and its original broker message."""
    from kombu.utils.encoding import safe_repr

    store = store or get_store(request.app)
    if store is None:
        return False

    message = request.message
    delivery_info = message.delivery_info or {}
    properties = message.properties or {}
    routing_key = delivery_info.get('routing_key')
    from .base import DeadLetter
    dead_letter = DeadLetter(
        task_id=request.id,
        task_name=request.type,
        args=request.args,
        kwargs=request.kwargs,
        argsrepr=request.argsrepr,
        kwargsrepr=request.kwargsrepr,
        exception_type=type(exc).__name__,
        exception_message=reason or safe_repr(exc),
        traceback=traceback or '',
        message_body=message.body,
        message_headers=dict(message.headers or {}),
        message_properties=dict(properties),
        content_type=message.content_type,
        content_encoding=message.content_encoding,
        queue=routing_key if delivery_info.get('exchange') in (None, '') else None,
        exchange=delivery_info.get('exchange'),
        routing_key=routing_key,
        hostname=request.hostname,
    )
    store.archive(dead_letter)
    return True


def replay_dead_letters(
    app,
    *,
    task_ids=None,
    task_name=None,
    exception_type=None,
    queue=None,
    hostname=None,
    since=None,
    until=None,
    limit=100,
    select_all=False,
    reset_retries=True,
    clear_schedule=True,
    lease_seconds=300,
    store=None,
    dry_run=False,
):
    """Select eligible dead letters and publish their original task messages."""
    if store is None:
        store = get_store(app)
    if store is None:
        raise RuntimeError('Dead-letter store is disabled')

    if dry_run:
        selected = store.list(
            task_id=task_ids,
            task_name=task_name,
            exception_type=exception_type,
            queue=queue,
            hostname=hostname,
            status=('FAILURE',),
            since=since,
            until=until,
            limit=limit,
        )
        return selected, []

    claimed = store.claim(
        task_ids=task_ids,
        task_name=task_name,
        exception_type=exception_type,
        queue=queue,
        hostname=hostname,
        since=since,
        until=until,
        limit=limit,
        select_all=select_all,
        lease_seconds=lease_seconds,
    )

    queued = []
    skipped = []
    for dead_letter in claimed:
        if _already_succeeded(app, store, dead_letter, skipped):
            continue
        try:
            # Mark QUEUED before publishing so another concurrent/retried
            # replay cannot enqueue this same task while the broker publish is
            # in flight. Stale QUEUED records are only reclaimable after their
            # lease expires; a publish failure immediately returns to FAILURE.
            store.mark_queued(dead_letter.task_id, dead_letter.replay_batch_id)
            _publish(
                app,
                dead_letter,
                reset_retries=reset_retries,
                clear_schedule=clear_schedule,
            )
        except Exception as exc:  # replay command must report the whole batch
            dead_letter.status = 'REPLAYING'
            dead_letter.exception_type = type(exc).__name__
            from kombu.utils.encoding import safe_repr
            dead_letter.exception_message = safe_repr(exc)
            dead_letter.traceback = ''
            store.mark_failed(dead_letter.task_id, dead_letter)
            skipped.append({'task_id': dead_letter.task_id, 'reason': str(exc), 'status': 'FAILED'})
        else:
            queued.append(dead_letter)
    return queued, skipped


def _already_succeeded(app, store, dead_letter, skipped):
    if dead_letter.status == 'SUCCESS':
        skipped.append({
            'task_id': dead_letter.task_id,
            'reason': 'Task already completed successfully',
            'status': 'SUCCESS',
        })
        return True
    result = app.AsyncResult(dead_letter.task_id)
    try:
        state = result.state
    except BackendGetMetaError:
        return False
    if state == SUCCESS:
        store.mark_succeeded(dead_letter.task_id)
        skipped.append({
            'task_id': dead_letter.task_id,
            'reason': 'Task already completed successfully',
            'status': state,
        })
        return True
    return False


def _publish(app, dead_letter, *, reset_retries, clear_schedule):
    body = dead_letter.message_body
    content_type = dead_letter.content_type or app.conf.task_serializer
    content_encoding = dead_letter.content_encoding
    headers = dict(dead_letter.message_headers or {})
    properties = dict(dead_letter.message_properties or {})

    headers['dead_letter_replay'] = True
    headers['dead_letter_attempts'] = dead_letter.attempts

    if reset_retries or clear_schedule:
        decoded = loads(body, content_type, content_encoding, force=True)
        if isinstance(decoded, dict):
            if reset_retries:
                decoded['retries'] = 0
            if clear_schedule:
                decoded['eta'] = decoded['expires'] = None
        elif isinstance(decoded, tuple):
            args, kwargs, embed = decoded
            if reset_retries:
                headers['retries'] = 0
            if clear_schedule:
                headers['eta'] = headers['expires'] = None
            decoded = (args, kwargs, embed)
        serializer = _serializer_for(content_type, app)
        content_type, content_encoding, body = _dumps(
            decoded, content_type, serializer,
        )

    publish_properties = {
        key: value for key, value in properties.items()
        if key in _PUBLISH_PROPERTIES and value is not None
    }
    with app.producer_or_acquire(None) as producer:
        with producer.connection._reraise_as_library_errors():
            producer.publish(
                body,
                exchange=dead_letter.exchange or '',
                routing_key=dead_letter.routing_key,
                headers=headers,
                content_type=content_type,
                content_encoding=content_encoding,
                serializer=None,
                declare=[],
                retry=False,
                retry_policy=app.conf.task_publish_retry_policy,
                **publish_properties,
            )


def _serializer_for(content_type, app):
    from kombu.serialization import registry
    serializer = registry._type_to_serializer.get(content_type)
    if serializer is None:
        serializer = app.conf.task_serializer
    return serializer


def _dumps(decoded, content_type, serializer):
    from kombu.serialization import registry
    new_content_type, new_content_encoding, encoder = registry._encoders[serializer]
    return new_content_type or content_type, new_content_encoding, encoder(decoded)
