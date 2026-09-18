"""Dead-letter store for finally failed tasks.

This module implements collection of tasks that failed permanently --
i.e. tasks that exhausted all of their retries, or whose message was
rejected without requeue -- so that they can be inspected later and
selectively re-enqueued (replayed).

Every dead-letter entry keeps the failure reason, the task input
arguments and the *original message* (body, headers, properties and
delivery information), which makes it possible to republish the exact
same message at a later point.

Replay is safe against duplicates:

- entries whose task is already in state ``SUCCESS`` in the result
  backend are never re-enqueued;
- a single replay batch never contains the same task id twice
  (only the most recently failed entry is considered);
- entries already requeued by a previous replay are skipped,
  so replaying the same selection twice will not enqueue the
  same batch of messages again.
"""
import json
import os
import tempfile
import threading
from base64 import b64decode, b64encode
from datetime import datetime
from time import time

from kombu.utils.encoding import safe_repr
from kombu.utils.uuid import uuid

from celery import states
from celery.backends.base import DisabledBackend
from celery.utils.functional import maybe_list
from celery.utils.log import get_logger
from celery.utils.time import maybe_iso8601

__all__ = (
    'REASON_FAILURE', 'REASON_REJECTED', 'REASON_TIMEOUT',
    'DeadLetterStore', 'entry_from_request',
)

#: Dead-letter reasons.
#: the task raised an exception and will not be retried anymore.
REASON_FAILURE = 'failed'
#: the task raised :exc:`~celery.exceptions.Reject` without requeue.
REASON_REJECTED = 'rejected'
#: the task exceeded its hard time limit.
REASON_TIMEOUT = 'timeout'

REASONS = frozenset({REASON_FAILURE, REASON_REJECTED, REASON_TIMEOUT})

logger = get_logger(__name__)

#: message properties that are not republished as-is, either because
#: they are passed explicitly or because they no longer apply.
_NON_REPLAYABLE_PROPERTIES = frozenset({
    'content_type', 'content_encoding', 'headers', 'delivery_info',
    'delivery_tag', 'compression', 'body_encoding',
})

#: message headers that are dropped or reset when an entry is replayed.
_NON_REPLAYABLE_HEADERS = frozenset({
    'compression',  # the stored body is already decompressed
    'eta',          # scheduling directives of the original delivery
    'expires',
})

_BYTES_TAG = '__celery_deadletter_bytes__'


def _json_safe(value):
    """Recursively convert *value* into something JSON serializable."""
    if isinstance(value, bytes):
        return {_BYTES_TAG: b64encode(value).decode('ascii')}
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return safe_repr(value)


def _json_restore(value):
    """Inverse of :func:`_json_safe` (only tagged values are restored)."""
    if isinstance(value, dict):
        if set(value) == {_BYTES_TAG}:
            return b64decode(value[_BYTES_TAG].encode('ascii'))
        return {key: _json_restore(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_restore(item) for item in value]
    return value


def _to_timestamp(value):
    """Convert epoch/datetime/ISO-8601 string to a unix timestamp."""
    if value is None or isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        value = maybe_iso8601(value)
    if isinstance(value, datetime):
        return value.timestamp()
    raise ValueError(f'cannot convert {value!r} to a timestamp')


def entry_from_request(request, exc, reason, traceback=None):
    """Create a dead-letter entry from a worker task request.

    Arguments:
        request (celery.worker.request.Request): The failed request.
        exc (BaseException): The exception that caused the final failure.
        reason (str): One of the ``REASON_*`` constants.
        traceback (str): Optional formatted traceback of the failure.
    """
    message = getattr(request, 'message', None)
    request_dict = getattr(request, 'request_dict', None) or {}
    return {
        'id': uuid(),
        'task_id': request.id,
        'task_name': request.type,
        'argsrepr': request.argsrepr or '',
        'kwargsrepr': request.kwargsrepr or '',
        'reason': reason,
        'exc_type': type(exc).__name__ if exc is not None else None,
        'exception': safe_repr(exc) if exc is not None else None,
        'traceback': traceback,
        'retries': request_dict.get('retries', 0),
        'failed_at': time(),
        'hostname': request.hostname,
        # the original message, used to replay the task as-is:
        'body': getattr(message, 'body', None),
        'content_type': getattr(message, 'content_type', None),
        'content_encoding': getattr(message, 'content_encoding', None),
        'headers': dict(getattr(message, 'headers', None) or {}),
        'properties': dict(getattr(message, 'properties', None) or {}),
        'delivery_info': dict(getattr(message, 'delivery_info', None) or {}),
        # replay bookkeeping:
        'requeued_at': None,
        'replay_id': None,
    }


class DeadLetterStore:
    """Queryable store of finally failed tasks (dead letters).

    The store is kept in memory and optionally persisted to a JSON file
    (see :setting:`worker_dead_letter_db`).  It is bounded by
    :setting:`worker_dead_letter_max_entries`; when the limit is
    exceeded the oldest entries are evicted first.
    """

    #: version of the on-disk format.
    persistence_version = 1

    def __init__(self, app, entries=None):
        self.app = app
        self._entries = {}
        self._mutex = threading.Lock()
        for entry in entries or ():
            self._entries[entry['id']] = entry
        self._load()

    @property
    def enabled(self):
        return self.max_entries > 0

    @property
    def max_entries(self):
        return self.app.conf.worker_dead_letter_max_entries

    @property
    def path(self):
        return self.app.conf.worker_dead_letter_db

    def __len__(self):
        return len(self._entries)

    def __iter__(self):
        return iter(self.find(include_requeued=True))

    def __contains__(self, entry_id):
        return entry_id in self._entries

    def add_from_request(self, request, exc, reason, traceback=None):
        """Create an entry from a failed request and add it to the store."""
        return self.add(entry_from_request(request, exc, reason, traceback))

    def add(self, entry):
        """Add a dead-letter entry, evicting the oldest if at capacity."""
        if not self.enabled:
            return None
        with self._mutex:
            self._entries[entry['id']] = entry
            self._evict()
            self._save()
        return entry

    def get(self, entry_id, default=None):
        """Return entry by id."""
        return self._entries.get(entry_id, default)

    def find(self, task=None, exc_type=None, reason=None,
             since=None, until=None, include_requeued=False,
             limit=None, offset=0):
        """Find entries matching the given criteria, newest first.

        Arguments:
            task (str): Only include entries of this task name.
            exc_type (str): Only include entries with this exception
                class name (e.g. ``'MaxRetriesExceededError'``).
            reason (str): Only include entries with this reason
                (``'failed'``, ``'rejected'`` or ``'timeout'``).
            since (float): Only include entries that failed at or after
                this time (epoch, ISO-8601 string or datetime).
            until (float): Only include entries that failed at or before
                this time.
            include_requeued (bool): Also include entries that were
                already requeued by a previous replay.  Disabled by
                default so that only actionable entries are returned.
            limit (int): Maximum number of entries to return.
            offset (int): Number of matching entries to skip.
        """
        since, until = _to_timestamp(since), _to_timestamp(until)
        tasks = set(maybe_list(task) or [])
        exc_types = set(maybe_list(exc_type) or [])
        reasons = set(maybe_list(reason) or [])
        with self._mutex:
            snapshot = list(self._entries.values())
        entries = sorted(
            (entry for entry in snapshot
             if (include_requeued or not entry.get('requeued_at'))
             and (not tasks or entry['task_name'] in tasks)
             and (not exc_types or entry['exc_type'] in exc_types)
             and (not reasons or entry['reason'] in reasons)
             and (since is None or entry['failed_at'] >= since)
             and (until is None or entry['failed_at'] <= until)),
            key=lambda entry: entry['failed_at'],
            reverse=True,
        )
        if offset:
            entries = entries[offset:]
        if limit is not None:
            entries = entries[:limit]
        return entries

    def mark_requeued(self, entry_ids, replay_id=None, save=True):
        """Mark entries as requeued so they are not replayed again."""
        replay_id = replay_id or uuid()
        now = time()
        with self._mutex:
            for entry_id in maybe_list(entry_ids):
                entry = self._entries.get(entry_id)
                if entry is not None:
                    entry['requeued_at'] = now
                    entry['replay_id'] = replay_id
            if save:
                self._save()
        return replay_id

    def purge(self, ids=None, task=None, exc_type=None, reason=None,
              since=None, until=None, requeued_only=False):
        """Remove entries matching the given criteria.

        With no arguments at all, every entry is removed.

        Returns:
            int: number of entries removed.
        """
        if (ids is None and task is None and exc_type is None and
                reason is None and since is None and until is None and
                not requeued_only):
            count = len(self._entries)
            self.clear()
            return count
        if ids is not None:
            wanted = set(maybe_list(ids))
            victims = [self._entries[entry_id] for entry_id in wanted
                       if entry_id in self._entries]
        else:
            victims = self.find(task=task, exc_type=exc_type, reason=reason,
                                since=since, until=until,
                                include_requeued=True)
        if requeued_only:
            victims = [entry for entry in victims if entry.get('requeued_at')]
        with self._mutex:
            for entry in victims:
                self._entries.pop(entry['id'], None)
            self._save()
        return len(victims)

    def clear(self):
        """Remove all entries."""
        with self._mutex:
            self._entries.clear()
            self._save()

    def replay(self, ids=None, task=None, exc_type=None, reason=None,
               since=None, until=None, limit=None, force=False):
        """Re-enqueue dead-lettered tasks matching the given criteria.

        The original message of every selected entry is republished
        with its original task id, exchange and routing key.

        A task is *not* re-enqueued when:

        - its state in the result backend is ``SUCCESS``;
        - a newer dead-letter entry exists for the same task id
          (only the latest failure of a task is replayed);
        - the entry was already requeued by a previous replay
          (unless *force* is enabled).

        Entries whose message cannot be republished are reported
        under the ``error`` key and do not abort the batch.

        Returns:
            dict: summary with the keys ``replay_id``, ``replayed``,
                ``skipped`` and ``total``.
        """
        replay_id = uuid()
        unknown = []
        if ids is not None:
            candidates = []
            for entry_id in maybe_list(ids):
                entry = self._entries.get(entry_id)
                if entry is None:
                    unknown.append(entry_id)
                else:
                    candidates.append(entry)
            candidates.sort(key=lambda entry: entry['failed_at'],
                            reverse=True)
        else:
            candidates = self.find(task=task, exc_type=exc_type,
                                   reason=reason, since=since, until=until,
                                   include_requeued=True, limit=limit)

        replayed, skipped = [], {
            'duplicate': [], 'requeued': [], 'succeeded': [],
            'incomplete': [], 'error': [], 'unknown': unknown,
        }
        seen_task_ids = set()
        dirty = False
        try:
            for entry in candidates:
                task_id = entry['task_id']
                if task_id in seen_task_ids:
                    skipped['duplicate'].append(self._ref(entry))
                    continue
                seen_task_ids.add(task_id)
                if entry.get('requeued_at') and not force:
                    skipped['requeued'].append(self._ref(entry))
                    continue
                if entry.get('body') is None:
                    skipped['incomplete'].append(self._ref(entry))
                    continue
                if self._already_succeeded(task_id):
                    skipped['succeeded'].append(self._ref(entry))
                    continue
                try:
                    self._republish(entry)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception(
                        'Cannot replay dead letter %s (task %s): %r',
                        entry['id'], task_id, exc)
                    skipped['error'].append(
                        dict(self._ref(entry), error=safe_repr(exc)))
                    continue
                # mark immediately so that a retried replay of the same
                # selection does not enqueue this message twice.
                self.mark_requeued(entry['id'], replay_id=replay_id,
                                   save=False)
                dirty = True
                replayed.append(entry)
        finally:
            if dirty:
                self._save()
        return {
            'replay_id': replay_id,
            'replayed': [self._ref(entry) for entry in replayed],
            'skipped': skipped,
            'total': len(candidates),
        }

    # -- internal helpers ------------------------------------------------

    @staticmethod
    def _ref(entry):
        return {'id': entry['id'], 'task_id': entry['task_id'],
                'task_name': entry['task_name']}

    def _evict(self):
        overflow = len(self._entries) - self.max_entries
        if overflow <= 0:
            return
        by_age = sorted(self._entries.values(),
                        key=lambda entry: entry['failed_at'])
        for entry in by_age[:overflow]:
            del self._entries[entry['id']]

    def _already_succeeded(self, task_id):
        backend = self.app.backend
        if isinstance(backend, DisabledBackend):
            # without a result backend there is no way to tell whether
            # the task succeeded meanwhile: assume it did not.
            return False
        try:
            return self.app.AsyncResult(
                task_id, backend=backend).state == states.SUCCESS
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning(
                'Cannot check state of task %s against the result '
                'backend: %r. Assuming it did not succeed.',
                task_id, exc)
            return False

    def _republish(self, entry):
        headers = {
            key: value for key, value in (entry.get('headers') or {}).items()
            if key not in _NON_REPLAYABLE_HEADERS
        }
        # replayed tasks get a fresh retry budget.
        headers['retries'] = 0
        delivery_info = entry.get('delivery_info') or {}
        properties = {
            key: value
            for key, value in (entry.get('properties') or {}).items()
            if key not in _NON_REPLAYABLE_PROPERTIES and value is not None
        }
        with self.app.producer_or_acquire() as producer:
            producer.publish(
                body=entry.get('body'),
                content_type=entry.get('content_type'),
                content_encoding=entry.get('content_encoding'),
                headers=headers,
                exchange=delivery_info.get('exchange') or '',
                routing_key=delivery_info.get('routing_key') or '',
                **properties)

    # -- persistence ------------------------------------------------------

    def _load(self):
        path = self.path
        if not path or not os.path.exists(path):
            return
        try:
            with open(path, encoding='utf-8') as handle:
                payload = json.load(handle)
        except (OSError, ValueError) as exc:
            logger.error('Cannot load dead-letter store %r: %r', path, exc)
            return
        for entry in payload.get('entries') or ():
            entry = _json_restore(entry)
            self._entries[entry['id']] = entry

    def _save(self):
        path = self.path
        if not path:
            return
        payload = {
            'version': self.persistence_version,
            'entries': [_json_safe(entry)
                        for entry in self._entries.values()],
        }
        directory = os.path.dirname(os.path.abspath(path))
        fd, tmp_path = tempfile.mkstemp(
            prefix='.deadletter-', suffix='.tmp', dir=directory)
        try:
            with os.fdopen(fd, 'w', encoding='utf-8') as handle:
                json.dump(payload, handle)
            os.replace(tmp_path, path)
        except OSError as exc:
            logger.error('Cannot save dead-letter store %r: %r', path, exc)
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
