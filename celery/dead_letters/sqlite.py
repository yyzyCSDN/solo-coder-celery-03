"""SQLite implementation of the terminal-failure dead-letter store."""
import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional
from uuid import uuid4

from .base import FAILED, QUEUED, REPLAYING, SUCCEEDED, DeadLetter, DeadLetterStore

__all__ = ('SqliteDeadLetterStore',)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS celery_dead_letter (
    task_id TEXT PRIMARY KEY,
    task_name TEXT NOT NULL,
    args TEXT,
    kwargs TEXT,
    argsrepr TEXT NOT NULL,
    kwargsrepr TEXT NOT NULL,
    exception_type TEXT NOT NULL,
    exception_message TEXT NOT NULL,
    traceback TEXT NOT NULL,
    message_body BLOB,
    message_headers TEXT NOT NULL,
    message_properties TEXT NOT NULL,
    content_type TEXT,
    content_encoding TEXT,
    queue TEXT,
    exchange TEXT,
    routing_key TEXT,
    hostname TEXT,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    failures TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    leased_until TEXT,
    replay_batch_id TEXT
)
"""


def utcnow():
    return datetime.now(timezone.utc)


def _dump(value):
    if value is None:
        return None
    return json.dumps(value, default=repr, ensure_ascii=False)


def _load(value, default=None):
    if value is None:
        return default
    return json.loads(value)


def _safe_json(value):
    if value is None:
        return None
    try:
        return json.dumps(value, default=repr, ensure_ascii=False)
    except (TypeError, ValueError):
        return repr(value)


def _format_time(value):
    if value is None:
        return None
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()


def _parse_time(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


class SqliteDeadLetterStore(DeadLetterStore):
    """Persist dead letters in a local SQLite database."""

    def __init__(self, url: str = 'celery-dead-letters.sqlite3', **kwargs):
        self.url = self._normalize_url(url)
        self._memory_connection = None
        self._memory_lock = threading.Lock()
        path = Path(self.url)
        if self.url != ':memory:':
            path.parent.mkdir(parents=True, exist_ok=True)
        self.options = kwargs
        with self._connect() as connection:
            connection.execute(_SCHEMA)

    @staticmethod
    def _normalize_url(url):
        if not url:
            url = 'celery-dead-letters.sqlite3'
        if url == 'sqlite://:memory:':
            return ':memory:'
        for prefix in ('sqlite:///', 'sqlite://'):
            if url.startswith(prefix):
                return url[len(prefix):]
        return url

    @contextmanager
    def _connect(self):
        if self.url == ':memory:':
            with self._memory_lock:
                if self._memory_connection is None:
                    self._memory_connection = sqlite3.connect(
                        self.url,
                        timeout=self.options.get('timeout', 30),
                        isolation_level=None,
                        check_same_thread=False,
                    )
                    self._memory_connection.row_factory = sqlite3.Row
                connection = self._memory_connection
                yield connection
                return

        connection = sqlite3.connect(
            self.url,
            timeout=self.options.get('timeout', 30),
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            connection.execute('PRAGMA busy_timeout = 30000')
            if self.url != ':memory:':
                connection.execute('PRAGMA journal_mode=WAL')
            yield connection
        finally:
            connection.close()

    def archive(self, dead_letter):
        now = utcnow()
        dead_letter.updated_at = now
        if dead_letter.created_at is None:
            dead_letter.created_at = now
        if not dead_letter.failures:
            dead_letter.failures = [self._failure(dead_letter, now)]

        values = self._values(dead_letter)
        with self._connect() as connection:
            connection.execute('BEGIN IMMEDIATE')
            existing = connection.execute(
                'SELECT status, attempts, failures FROM celery_dead_letter WHERE task_id = ?',
                (dead_letter.task_id,),
            ).fetchone()
            if existing is None:
                columns = ', '.join(values)
                placeholders = ', '.join(f':{key}' for key in values)
                connection.execute(
                    f'INSERT INTO celery_dead_letter ({columns}) VALUES ({placeholders})',
                    values,
                )
            else:
                # The same message can legitimately be archived more than once
                # (for example, it was rejected by one worker and later
                # redelivered and raised). Append the new failure but keep the
                # original terminal-failure payload and replay state.
                failures = _load(existing['failures'], [])
                failures.extend(dead_letter.failures)
                values['attempts'] = existing['attempts'] + dead_letter.attempts
                values['failures'] = _dump(failures)
                if (
                    dead_letter.message_headers.get('dead_letter_replay')
                    and existing['status'] == QUEUED
                ):
                    values['status'] = FAILED
                    values['leased_until'] = None
                    values['replay_batch_id'] = None
                else:
                    for column in ('status', 'leased_until', 'replay_batch_id'):
                        values.pop(column, None)
                values.pop('created_at', None)
                assignments = ', '.join(f'{key} = :{key}' for key in values)
                connection.execute(
                    f'UPDATE celery_dead_letter SET {assignments} WHERE task_id = :task_id',
                    values,
                )
            connection.commit()

    def list(
        self, *, task_id=None, task_name=None, exception_type=None,
        queue=None, hostname=None, status=None,
        since=None, until=None, limit=None,
    ):
        clauses, parameters = self._filters(
            task_id=task_id, task_name=task_name, exception_type=exception_type,
            queue=queue, hostname=hostname, status=status, since=since, until=until,
        )
        sql = 'SELECT * FROM celery_dead_letter'
        if clauses:
            sql += ' WHERE ' + ' AND '.join(clauses)
        sql += ' ORDER BY created_at DESC'
        if limit is not None:
            sql += ' LIMIT ?'
            parameters.append(limit)
        with self._connect() as connection:
            return [self._from_row(row) for row in connection.execute(sql, parameters)]

    def get(self, task_id):
        with self._connect() as connection:
            row = connection.execute(
                'SELECT * FROM celery_dead_letter WHERE task_id = ?',
                (task_id,),
            ).fetchone()
        return self._from_row(row) if row else None

    def claim(
        self, *, task_ids=None, task_name=None, exception_type=None,
        queue=None, hostname=None, since=None, until=None, limit=100,
        lease_seconds=300, batch_id=None, select_all=False,
    ):
        filters = {
            'task_name': task_name,
            'exception_type': exception_type,
            'queue': queue,
            'hostname': hostname,
            'since': since,
            'until': until,
        }
        if not select_all and not task_ids and not any(value is not None for value in filters.values()):
            raise ValueError('Refusing to claim every dead letter without select_all=True')

        clauses, parameters = self._filters(**filters)
        if task_ids:
            placeholders = ', '.join('?' for _ in task_ids)
            clauses.append(f'task_id IN ({placeholders})')
            parameters.extend(task_ids)
        now = utcnow()
        lease_until = datetime.fromtimestamp(now.timestamp() + lease_seconds, timezone.utc)
        batch_id = batch_id or str(uuid4())

        with self._connect() as connection:
            connection.execute('BEGIN IMMEDIATE')
            clauses.extend([
                '(status = ? OR (status = ? AND leased_until < ?) '
                'OR (status = ? AND (leased_until IS NULL OR leased_until < ?)))',
            ])
            parameters.extend([
                FAILED, REPLAYING, _format_time(now),
                QUEUED, _format_time(now),
            ])
            select_sql = 'SELECT task_id FROM celery_dead_letter'
            if clauses:
                select_sql += ' WHERE ' + ' AND '.join(clauses)
            select_sql += ' ORDER BY created_at ASC LIMIT ?'
            select_parameters = parameters + [limit]
            ids = [
                row['task_id']
                for row in connection.execute(select_sql, select_parameters)
            ]
            if ids:
                placeholders = ', '.join('?' for _ in ids)
                connection.execute(
                    f'''UPDATE celery_dead_letter
                        SET status = ?, updated_at = ?, leased_until = ?, replay_batch_id = ?
                        WHERE task_id IN ({placeholders})''',
                    [REPLAYING, _format_time(now), _format_time(lease_until), batch_id, *ids],
                )
            connection.commit()
            if not ids:
                return []
            rows = connection.execute(
                f'''SELECT * FROM celery_dead_letter
                    WHERE task_id IN ({placeholders}) ORDER BY created_at ASC''',
                ids,
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def mark_queued(self, task_id, batch_id=None):
        with self._update_claimed(task_id, QUEUED, batch_id) as connection:
            return connection

    def mark_succeeded(self, task_id):
        with self._connect() as connection:
            connection.execute(
                '''UPDATE celery_dead_letter
                   SET status = ?, updated_at = ?, leased_until = NULL
                   WHERE task_id = ?''',
                (SUCCEEDED, _format_time(utcnow()), task_id),
            )

    def mark_failed(self, task_id, dead_letter):
        now = utcnow()
        failure = self._failure(dead_letter, now)
        with self._connect() as connection:
            connection.execute('BEGIN IMMEDIATE')
            row = connection.execute(
                'SELECT attempts, failures FROM celery_dead_letter WHERE task_id = ?',
                (task_id,),
            ).fetchone()
            if row is None:
                connection.rollback()
                raise KeyError(task_id)
            failures = _load(row['failures'], [])
            failures.append(failure)
            cursor = connection.execute(
                '''UPDATE celery_dead_letter
                   SET status = ?, attempts = ?, failures = ?, exception_type = ?,
                       exception_message = ?, traceback = ?, argsrepr = ?, kwargsrepr = ?,
                       updated_at = ?, leased_until = NULL, replay_batch_id = NULL
                   WHERE task_id = ? AND (replay_batch_id = ? OR replay_batch_id IS NULL)''',
                (
                    FAILED, row['attempts'] + 1, _dump(failures),
                    dead_letter.exception_type, dead_letter.exception_message,
                    dead_letter.traceback, dead_letter.argsrepr, dead_letter.kwargsrepr,
                    _format_time(now), task_id, dead_letter.replay_batch_id,
                ),
            )
            if cursor.rowcount != 1:
                connection.rollback()
                raise KeyError(task_id)
            connection.commit()

    @contextmanager
    def _update_claimed(self, task_id, status, batch_id):
        with self._connect() as connection:
            now = _format_time(utcnow())
            if batch_id is None:
                cursor = connection.execute(
                    '''UPDATE celery_dead_letter
                       SET status = ?, updated_at = ?, leased_until = leased_until
                       WHERE task_id = ? AND status = ?''',
                    (status, now, task_id, REPLAYING),
                )
            else:
                cursor = connection.execute(
                    '''UPDATE celery_dead_letter
                       SET status = ?, updated_at = ?, leased_until = leased_until
                       WHERE task_id = ? AND status = ? AND replay_batch_id = ?''',
                    (status, now, task_id, REPLAYING, batch_id),
                )
            if cursor.rowcount != 1:
                raise KeyError(task_id)
            yield connection

    @staticmethod
    def _filters(
        task_id=None, task_name=None, exception_type=None,
        queue=None, hostname=None, status=None, since=None, until=None,
    ):
        clauses = []
        parameters = []
        if task_id is not None:
            if isinstance(task_id, str):
                clauses.append('task_id = ?')
                parameters.append(task_id)
            else:
                task_id = list(task_id)
                if task_id:
                    placeholders = ', '.join('?' for _ in task_id)
                    clauses.append(f'task_id IN ({placeholders})')
                    parameters.extend(task_id)
        if task_name is not None:
            clauses.append('task_name = ?')
            parameters.append(task_name)
        if exception_type is not None:
            clauses.append('exception_type = ?')
            parameters.append(exception_type)
        if queue is not None:
            clauses.append('queue = ?')
            parameters.append(queue)
        if hostname is not None:
            clauses.append('hostname = ?')
            parameters.append(hostname)
        if status is not None:
            status = tuple(status)
            placeholders = ', '.join('?' for _ in status)
            clauses.append(f'status IN ({placeholders})')
            parameters.extend(status)
        if since is not None:
            clauses.append('created_at >= ?')
            parameters.append(_format_time(since))
        if until is not None:
            clauses.append('created_at <= ?')
            parameters.append(_format_time(until))
        return clauses, parameters

    @staticmethod
    def _failure(dead_letter, when):
        return {
            'at': _format_time(when),
            'exception_type': dead_letter.exception_type,
            'exception_message': dead_letter.exception_message,
            'traceback': dead_letter.traceback,
            'hostname': dead_letter.hostname,
            'attempt': dead_letter.attempts,
        }

    def _values(self, dead_letter):
        return {
            'task_id': dead_letter.task_id,
            'task_name': dead_letter.task_name,
            'args': _safe_json(dead_letter.args),
            'kwargs': _safe_json(dead_letter.kwargs),
            'argsrepr': dead_letter.argsrepr,
            'kwargsrepr': dead_letter.kwargsrepr,
            'exception_type': dead_letter.exception_type,
            'exception_message': dead_letter.exception_message,
            'traceback': dead_letter.traceback,
            'message_body': dead_letter.message_body,
            'message_headers': _safe_json(dead_letter.message_headers),
            'message_properties': _safe_json(dead_letter.message_properties),
            'content_type': dead_letter.content_type,
            'content_encoding': dead_letter.content_encoding,
            'queue': dead_letter.queue,
            'exchange': dead_letter.exchange,
            'routing_key': dead_letter.routing_key,
            'hostname': dead_letter.hostname,
            'status': dead_letter.status,
            'attempts': dead_letter.attempts,
            'failures': _dump(dead_letter.failures),
            'created_at': _format_time(dead_letter.created_at),
            'updated_at': _format_time(dead_letter.updated_at),
            'leased_until': _format_time(dead_letter.leased_until),
            'replay_batch_id': dead_letter.replay_batch_id,
        }

    @staticmethod
    def _from_row(row):
        return DeadLetter(
            task_id=row['task_id'],
            task_name=row['task_name'],
            args=_load(row['args']),
            kwargs=_load(row['kwargs']),
            argsrepr=row['argsrepr'],
            kwargsrepr=row['kwargsrepr'],
            exception_type=row['exception_type'],
            exception_message=row['exception_message'],
            traceback=row['traceback'],
            message_body=row['message_body'],
            message_headers=_load(row['message_headers'], {}),
            message_properties=_load(row['message_properties'], {}),
            content_type=row['content_type'],
            content_encoding=row['content_encoding'],
            queue=row['queue'],
            exchange=row['exchange'],
            routing_key=row['routing_key'],
            hostname=row['hostname'],
            status=row['status'],
            attempts=row['attempts'],
            failures=_load(row['failures'], []),
            created_at=_parse_time(row['created_at']),
            updated_at=_parse_time(row['updated_at']),
            leased_until=_parse_time(row['leased_until']),
            replay_batch_id=row['replay_batch_id'],
        )
