from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from celery.contrib.testing.mocks import TaskMessage
from celery.dead_letters.base import QUEUED, REPLAYING, DeadLetter
from celery.dead_letters.replay import archive_request, replay_dead_letters
from celery.dead_letters.sqlite import SqliteDeadLetterStore
from celery.worker.request import Request


@pytest.fixture
def store(tmp_path):
    return SqliteDeadLetterStore(str(tmp_path / 'dead-letters.sqlite3'))


def dead_letter(task_id='task-id', **kwargs):
    defaults = {
        'task_name': 'tasks.add',
        'args': [1, 2],
        'kwargs': {'x': 'y'},
        'argsrepr': '(1, 2)',
        'kwargsrepr': "{'x': 'y'}",
        'exception_type': 'ValueError',
        'exception_message': 'boom',
        'traceback': 'traceback',
        'message_body': b'[[1, 2], {"x": "y"}, {}]',
        'message_headers': {
            'id': task_id,
            'task': 'tasks.add',
            'retries': 3,
            'eta': '2000-01-01T00:00:00+00:00',
            'expires': '2000-01-02T00:00:00+00:00',
        },
        'message_properties': {},
        'content_type': 'application/json',
        'content_encoding': 'utf-8',
        'queue': 'celery',
        'exchange': '',
        'routing_key': 'celery',
    }
    defaults.update(kwargs)
    return DeadLetter(task_id=task_id, **defaults)


class test_sqlite_dead_letter_store:

    def test_archive_appends_failure_history(self, store):
        store.archive(dead_letter())
        store.archive(dead_letter(exception_message='again'))

        archived = store.get('task-id')
        assert archived.attempts == 2
        assert [failure['exception_message'] for failure in archived.failures] == ['boom', 'again']

    def test_claim_excludes_same_task_from_second_batch(self, store):
        store.archive(dead_letter('one'))
        store.archive(dead_letter('two'))

        first = store.claim(select_all=True)
        second = store.claim(select_all=True)

        assert {message.task_id for message in first} == {'one', 'two'}
        assert all(message.status == REPLAYING for message in first)
        assert second == []

    def test_expired_claim_can_be_retaken(self, store):
        store.archive(dead_letter())
        claimed = store.claim(select_all=True, lease_seconds=300)
        claimed[0].leased_until = datetime.now(timezone.utc) - timedelta(seconds=1)
        store.mark_failed(claimed[0].task_id, claimed[0])

        reclaimed = store.claim(select_all=True)
        assert len(reclaimed) == 1

    def test_stale_queued_lease_can_be_retaken(self, store):
        store.archive(dead_letter())
        claimed = store.claim(select_all=True, lease_seconds=300)
        store.mark_queued(claimed[0].task_id, claimed[0].replay_batch_id)
        assert store.claim(select_all=True, lease_seconds=300) == []

        with store._connect() as connection:
            connection.execute(
                'UPDATE celery_dead_letter SET leased_until = ? WHERE task_id = ?',
                ('2000-01-01T00:00:00+00:00', 'task-id'),
            )
        assert len(store.claim(select_all=True, lease_seconds=300)) == 1

    def test_replay_execution_failure_reopens_queued_record(self, store):
        store.archive(dead_letter())
        claimed = store.claim(select_all=True)
        store.mark_queued(claimed[0].task_id, claimed[0].replay_batch_id)

        store.archive(dead_letter(
            exception_message='replay failed',
            message_headers={'dead_letter_replay': True},
        ))

        archived = store.get('task-id')
        assert archived.status == 'FAILURE'
        assert archived.leased_until is None
        assert archived.replay_batch_id is None
        assert len(store.claim(select_all=True)) == 1

    def test_duplicate_failure_keeps_active_replay_state(self, store):
        store.archive(dead_letter())
        claimed = store.claim(select_all=True)
        store.mark_queued(claimed[0].task_id, claimed[0].replay_batch_id)

        store.archive(dead_letter(exception_message='duplicate delivery'))

        assert store.get('task-id').status == QUEUED
        assert store.claim(select_all=True, lease_seconds=300) == []

    def test_mark_queued_requires_claimed_batch(self, store):
        store.archive(dead_letter())
        with pytest.raises(KeyError):
            store.mark_queued('task-id', batch_id='other')

        claimed = store.claim(select_all=True)
        store.mark_queued(claimed[0].task_id, claimed[0].replay_batch_id)
        assert store.get('task-id').status == QUEUED


class test_replay_dead_letters:

    def test_archive_request_keeps_args_and_original_message(self, app, store):
        message = TaskMessage(
            'tasks.add', 'task-id', args=(1, 2), kwargs={'x': 'y'},
            retries=3,
        )
        message.delivery_info = {
            'exchange': '', 'routing_key': 'celery', 'redelivered': False,
        }
        message.properties = {}
        task = Mock()
        task.ignore_result = False
        request = Request(
            message, app=app, task=task,
            on_ack=Mock(), on_reject=Mock(),
        )

        assert archive_request(request, ValueError('boom'), 'traceback', store=store)
        archived = store.get('task-id')
        assert archived.args == [1, 2]
        assert archived.kwargs == {'x': 'y'}
        assert archived.message_headers['retries'] == 3
        assert archived.message_body == message.body

    def test_replay_claims_publishes_and_resets_retry_count(self, app, store):
        message = TaskMessage(
            'tasks.add', 'task-id', args=(1, 2), kwargs={},
            retries=3, eta='2000-01-01T00:00:00+00:00',
        )
        message.properties = {}
        store.archive(dead_letter(message_body=message.body))
        producer = Mock()
        producer.connection._reraise_as_library_errors.return_value.__enter__ = Mock()
        producer.connection._reraise_as_library_errors.return_value.__exit__ = Mock(return_value=False)

        @contextmanager
        def producer_or_acquire(_producer=None):
            yield producer

        app.producer_or_acquire = producer_or_acquire

        queued, skipped = replay_dead_letters(app, store=store, select_all=True)

        assert len(queued) == 1
        assert skipped == []
        producer.publish.assert_called_once()
        published = producer.publish.call_args
        headers = published.kwargs['headers']
        assert headers['retries'] == 0
        assert headers['eta'] is None
        assert headers['expires'] is None
        assert headers['dead_letter_replay'] is True
        assert published.kwargs['content_type'] == 'application/json'
        assert store.get('task-id').status == QUEUED

    def test_publish_failure_returns_record_to_failed(self, app, store):
        store.archive(dead_letter())

        @contextmanager
        def producer_or_acquire(_producer=None):
            producer = Mock()
            producer.publish.side_effect = RuntimeError('broker unavailable')
            producer.connection._reraise_as_library_errors.return_value.__enter__ = Mock()
            producer.connection._reraise_as_library_errors.return_value.__exit__ = Mock(
                return_value=False,
            )
            yield producer

        app.producer_or_acquire = producer_or_acquire

        queued, skipped = replay_dead_letters(app, store=store, task_ids=['task-id'])

        assert queued == []
        assert skipped[0]['status'] == 'FAILURE'
        archived = store.get('task-id')
        assert archived.status == 'FAILURE'
        assert archived.exception_type == 'RuntimeError'

    def test_replay_skips_task_already_successful(self, app, store):
        store.archive(dead_letter())
        app.AsyncResult = Mock(return_value=Mock(state='SUCCESS'))

        queued, skipped = replay_dead_letters(app, store=store, task_ids=['task-id'])

        assert queued == []
        assert skipped == [{
            'task_id': 'task-id',
            'reason': 'Task already completed successfully',
            'status': 'SUCCESS',
        }]
        assert store.get('task-id').status == 'SUCCESS'

    def test_replay_requires_filter_or_all(self, app, store):
        store.archive(dead_letter())
        with pytest.raises(ValueError, match='Refusing to claim every'):
            replay_dead_letters(app, store=store)
