import json
import os
import time
from unittest.mock import MagicMock, Mock, patch

from billiard.einfo import ExceptionInfo

from celery.exceptions import Ignore, MaxRetriesExceededError, Reject, Retry, WorkerLostError
from celery.worker import deadletter
from celery.worker.deadletter import (REASON_FAILURE, REASON_REJECTED, DeadLetterStore, _json_restore, _json_safe,
                                      entry_from_request)
from celery.worker.request import Request


class DeadLetterCase:

    def setup_method(self):
        self.app.dead_letters.clear()

        @self.app.task(shared=False)
        def mytask(i, **kwargs):
            return i ** i

        self.mytask = mytask

    def teardown_method(self):
        self.app.dead_letters.clear()

    def xRequest(self, name=None, id=None, args=None, kwargs=None,
                 on_ack=None, on_reject=None, **head):
        args = [1] if args is None else args
        kwargs = {'f': 'x'} if kwargs is None else kwargs
        message = self.TaskMessage(
            name or self.mytask.name, id, args=args, kwargs=kwargs, **head)
        message.delivery_info = {
            'exchange': 'celery',
            'routing_key': 'celery',
            'redelivered': False,
        }
        message.properties = {
            'correlation_id': message.headers['id'],
            'reply_to': 'someone',
        }
        return Request(message, app=self.app,
                       on_ack=on_ack or Mock(name='on_ack'),
                       on_reject=on_reject or Mock(name='on_reject'))

    def make_entry(self, task_id=None, task_name='tasks.add',
                   failed_at=None, **kwargs):
        from kombu.utils.uuid import uuid
        task_id = task_id or uuid()
        entry = {
            'id': uuid(),
            'task_id': task_id,
            'task_name': task_name,
            'argsrepr': '(1, 2)',
            'kwargsrepr': '{}',
            'reason': REASON_FAILURE,
            'exc_type': 'KeyError',
            'exception': "KeyError('x')",
            'traceback': 'Traceback (most recent call last):\n...',
            'retries': 3,
            'failed_at': failed_at if failed_at is not None else time.time(),
            'hostname': 'worker1@example.com',
            'body': '[[1, 2], {}, {"callbacks": null}]',
            'content_type': 'application/json',
            'content_encoding': 'utf-8',
            'headers': {'id': task_id, 'task': task_name, 'retries': 3},
            'properties': {'correlation_id': task_id},
            'delivery_info': {'exchange': 'celery', 'routing_key': 'celery'},
            'requeued_at': None,
            'replay_id': None,
        }
        entry.update(kwargs)
        return entry

    def store(self, **kwargs):
        return DeadLetterStore(app=self.app, **kwargs)


class test_entry_from_request(DeadLetterCase):

    def test_collects_failure_input_and_message(self):
        job = self.xRequest(args=[1, 2], kwargs={'a': 'b'})
        exc = KeyError('boom')
        entry = entry_from_request(job, exc, REASON_FAILURE, 'tb...')
        assert entry['task_id'] == job.id
        assert entry['task_name'] == self.mytask.name
        assert entry['reason'] == REASON_FAILURE
        assert entry['exc_type'] == 'KeyError'
        assert 'boom' in entry['exception']
        assert entry['traceback'] == 'tb...'
        assert entry['argsrepr'] == job.argsrepr
        assert entry['kwargsrepr'] == job.kwargsrepr
        # the original message is kept for replay
        assert entry['body'] == job.message.body
        assert entry['content_type'] == job.message.content_type
        assert entry['content_encoding'] == job.message.content_encoding
        assert entry['headers']['id'] == job.id
        assert entry['delivery_info']['exchange'] == 'celery'
        assert entry['properties']['correlation_id'] == job.id
        assert entry['requeued_at'] is None


class test_json_safety:

    def test_bytes_roundtrip(self):
        value = {'body': b'\x00\xffbinary', 'nested': [b'abc', 1, 'x']}
        assert _json_restore(_json_safe(value)) == value

    def test_unserializable_falls_back_to_repr(self):
        class Thing:
            def __repr__(self):
                return '<Thing>'

        safe = _json_safe({'thing': Thing()})
        assert safe == {'thing': '<Thing>'}
        json.dumps(safe)


class test_DeadLetterStore(DeadLetterCase):

    def test_add_and_get(self):
        store = self.store()
        entry = store.add(self.make_entry())
        assert entry['id'] in store
        assert store.get(entry['id']) == entry
        assert len(store) == 1

    def test_add_disabled_by_max_entries(self, monkeypatch):
        monkeypatch.setattr(
            self.app.conf, 'worker_dead_letter_max_entries', 0)
        store = self.store()
        assert not store.enabled
        assert store.add(self.make_entry()) is None
        assert len(store) == 0

    def test_evicts_oldest_when_full(self, monkeypatch):
        monkeypatch.setattr(
            self.app.conf, 'worker_dead_letter_max_entries', 2)
        store = self.store()
        first = store.add(self.make_entry(failed_at=1.0))
        store.add(self.make_entry(failed_at=2.0))
        store.add(self.make_entry(failed_at=3.0))
        assert len(store) == 2
        assert first['id'] not in store
        remaining = [e['failed_at'] for e in store.find(include_requeued=True)]
        assert remaining == [3.0, 2.0]

    def test_find_newest_first(self):
        store = self.store()
        store.add(self.make_entry(failed_at=1.0))
        store.add(self.make_entry(failed_at=3.0))
        store.add(self.make_entry(failed_at=2.0))
        found = store.find()
        assert [e['failed_at'] for e in found] == [3.0, 2.0, 1.0]

    def test_find_by_task(self):
        store = self.store()
        store.add(self.make_entry(task_name='tasks.add'))
        store.add(self.make_entry(task_name='tasks.mul'))
        found = store.find(task='tasks.add')
        assert len(found) == 1
        assert found[0]['task_name'] == 'tasks.add'
        assert len(store.find(task=['tasks.add', 'tasks.mul'])) == 2

    def test_find_by_exc_type_and_reason(self):
        store = self.store()
        store.add(self.make_entry(exc_type='KeyError'))
        store.add(self.make_entry(exc_type='MaxRetriesExceededError'))
        store.add(self.make_entry(exc_type='KeyError',
                                  reason=REASON_REJECTED))
        assert len(store.find(exc_type='KeyError')) == 2
        assert len(store.find(exc_type='MaxRetriesExceededError')) == 1
        assert len(store.find(reason=REASON_REJECTED)) == 1
        assert len(store.find(exc_type='KeyError',
                              reason=REASON_REJECTED)) == 1

    def test_find_by_time_window(self):
        store = self.store()
        store.add(self.make_entry(failed_at=10.0))
        store.add(self.make_entry(failed_at=20.0))
        store.add(self.make_entry(failed_at=30.0))
        assert len(store.find(since=20.0)) == 2
        assert len(store.find(until=20.0)) == 2
        assert len(store.find(since=15.0, until=25.0)) == 1
        assert len(store.find(since='1970-01-01T00:00:00Z')) == 3

    def test_find_limit_offset(self):
        store = self.store()
        for i in range(5):
            store.add(self.make_entry(failed_at=float(i)))
        assert len(store.find(limit=2)) == 2
        page = store.find(limit=2, offset=2)
        assert [e['failed_at'] for e in page] == [2.0, 1.0]

    def test_find_excludes_requeued_by_default(self):
        store = self.store()
        entry = store.add(self.make_entry())
        assert len(store.find()) == 1
        store.mark_requeued(entry['id'])
        assert store.find() == []
        assert len(store.find(include_requeued=True)) == 1

    def test_mark_requeued(self):
        store = self.store()
        entry = store.add(self.make_entry())
        replay_id = store.mark_requeued(entry['id'])
        stored = store.get(entry['id'])
        assert stored['requeued_at']
        assert stored['replay_id'] == replay_id

    def test_purge_all(self):
        store = self.store()
        store.add(self.make_entry())
        store.add(self.make_entry())
        assert store.purge() == 2
        assert len(store) == 0

    def test_purge_by_ids_and_filters(self):
        store = self.store()
        keep = store.add(self.make_entry(task_name='tasks.keep'))
        gone = store.add(self.make_entry(task_name='tasks.drop'))
        assert store.purge(ids=[gone['id'], 'does-not-exist']) == 1
        assert store.get(keep['id']) is not None
        assert store.purge(task='tasks.keep') == 1
        assert len(store) == 0

    def test_purge_requeued_only(self):
        store = self.store()
        replayed = store.add(self.make_entry())
        store.add(self.make_entry())
        store.mark_requeued(replayed['id'])
        assert store.purge(requeued_only=True) == 1
        assert len(store) == 1

    def test_clear(self):
        store = self.store()
        store.add(self.make_entry())
        store.clear()
        assert len(store) == 0


class test_persistence(DeadLetterCase):

    def test_save_and_load(self, tmp_path, monkeypatch):
        db = str(tmp_path / 'deadletters.json')
        monkeypatch.setattr(self.app.conf, 'worker_dead_letter_db', db)
        store = self.store()
        entry = store.add(self.make_entry(body=b'\x00raw bytes'))
        store.mark_requeued(entry['id'], replay_id='batch-1')

        reloaded = self.store()
        assert len(reloaded) == 1
        loaded = reloaded.get(entry['id'])
        assert loaded['body'] == b'\x00raw bytes'
        assert loaded['replay_id'] == 'batch-1'
        assert loaded['requeued_at']
        # file is valid JSON and does not contain raw bytes
        with open(db, encoding='utf-8') as handle:
            payload = json.load(handle)
        assert payload['version'] == store.persistence_version

    def test_load_missing_file_is_ok(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            self.app.conf, 'worker_dead_letter_db',
            str(tmp_path / 'does-not-exist.json'))
        assert len(self.store()) == 0

    def test_load_broken_file_is_ok(self, tmp_path, monkeypatch):
        db = tmp_path / 'broken.json'
        db.write_text('{not json')
        monkeypatch.setattr(self.app.conf, 'worker_dead_letter_db', str(db))
        assert len(self.store()) == 0

    def test_no_db_configured_no_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr(self.app.conf, 'worker_dead_letter_db', None)
        store = self.store()
        store.add(self.make_entry())
        assert os.listdir(tmp_path) == []


class test_replay(DeadLetterCase):

    @staticmethod
    def patched_producer(app):
        producer = MagicMock(name='producer')
        acquire = MagicMock(name='producer_or_acquire')
        acquire.return_value.__enter__.return_value = producer
        return producer, patch.object(
            type(app), 'producer_or_acquire', acquire)

    def test_replay_republishes_original_message(self):
        store = self.store()
        entry = self.make_entry(
            headers={'id': 'tid', 'task': 'tasks.add', 'retries': 3,
                     'eta': '2016-01-01T00:00:00Z',
                     'expires': '2016-01-02T00:00:00Z',
                     'compression': 'gzip'},
            properties={'correlation_id': 'tid', 'reply_to': 'q',
                        'delivery_info': {'exchange': 'celery'},
                        'body_encoding': 'base64'},
        )
        store.add(entry)
        producer, patching = self.patched_producer(self.app)
        with patching:
            result = store.replay()
        assert result['total'] == 1
        assert len(result['replayed']) == 1
        producer.publish.assert_called_once()
        published = producer.publish.call_args.kwargs
        # original message is republished as-is...
        assert published['body'] == entry['body']
        assert published['content_type'] == 'application/json'
        assert published['content_encoding'] == 'utf-8'
        assert published['exchange'] == 'celery'
        assert published['routing_key'] == 'celery'
        assert published['correlation_id'] == 'tid'
        assert published['reply_to'] == 'q'
        # ...but the retry budget is reset and stale scheduling removed
        assert published['headers']['retries'] == 0
        assert 'eta' not in published['headers']
        assert 'expires' not in published['headers']
        assert 'compression' not in published['headers']
        # transport-internal properties are not passed through
        assert 'delivery_info' not in published
        assert 'body_encoding' not in published

    def test_replay_marks_entries_requeued(self):
        store = self.store()
        entry = store.add(self.make_entry())
        producer, patching = self.patched_producer(self.app)
        with patching:
            result = store.replay()
        stored = store.get(entry['id'])
        assert stored['requeued_at']
        assert stored['replay_id'] == result['replay_id']

    def test_replay_same_batch_twice_does_not_reenqueue(self):
        store = self.store()
        store.add(self.make_entry())
        store.add(self.make_entry())
        producer, patching = self.patched_producer(self.app)
        with patching:
            first = store.replay()
            assert len(first['replayed']) == 2
            assert producer.publish.call_count == 2
            # replaying the same selection again enqueues nothing.
            second = store.replay()
            assert second['replayed'] == []
            assert len(second['skipped']['requeued']) == 2
            assert producer.publish.call_count == 2

    def test_replay_force_reenqueues(self):
        store = self.store()
        store.add(self.make_entry())
        producer, patching = self.patched_producer(self.app)
        with patching:
            store.replay()
            result = store.replay(force=True)
            assert len(result['replayed']) == 1
            assert producer.publish.call_count == 2

    def test_replay_deduplicates_task_id_keeping_latest(self):
        store = self.store()
        store.add(self.make_entry(task_id='tid', failed_at=1.0,
                                  body='old-body'))
        store.add(self.make_entry(task_id='tid', failed_at=2.0,
                                  body='new-body'))
        producer, patching = self.patched_producer(self.app)
        with patching:
            result = store.replay()
        assert len(result['replayed']) == 1
        assert len(result['skipped']['duplicate']) == 1
        producer.publish.assert_called_once()
        assert producer.publish.call_args.kwargs['body'] == 'new-body'

    def test_replay_skips_already_succeeded(self):
        store = self.store()
        entry = store.add(self.make_entry())
        self.app.backend.mark_as_done(entry['task_id'], 42)
        producer, patching = self.patched_producer(self.app)
        with patching:
            result = store.replay()
        assert result['replayed'] == []
        assert result['skipped']['succeeded'] == [
            {'id': entry['id'], 'task_id': entry['task_id'],
             'task_name': entry['task_name']},
        ]
        producer.publish.assert_not_called()

    def test_replay_by_ids_and_unknown_ids(self):
        store = self.store()
        first = store.add(self.make_entry())
        second = store.add(self.make_entry())
        producer, patching = self.patched_producer(self.app)
        with patching:
            result = store.replay(ids=[first['id'], 'missing-id'])
        assert [r['id'] for r in result['replayed']] == [first['id']]
        assert result['skipped']['unknown'] == ['missing-id']
        assert producer.publish.call_count == 1
        assert store.get(second['id'])['requeued_at'] is None

    def test_replay_with_filters(self):
        store = self.store()
        store.add(self.make_entry(task_name='tasks.add'))
        store.add(self.make_entry(task_name='tasks.mul'))
        producer, patching = self.patched_producer(self.app)
        with patching:
            result = store.replay(task='tasks.add')
        assert result['total'] == 1
        assert producer.publish.call_count == 1

    def test_replay_skips_entries_without_body(self):
        store = self.store()
        store.add(self.make_entry(body=None))
        producer, patching = self.patched_producer(self.app)
        with patching:
            result = store.replay()
        assert result['replayed'] == []
        assert len(result['skipped']['incomplete']) == 1
        producer.publish.assert_not_called()

    def test_replay_publish_error_does_not_abort_batch(self):
        store = self.store()
        bad = store.add(self.make_entry(failed_at=2.0))
        good = store.add(self.make_entry(failed_at=1.0))
        producer, patching = self.patched_producer(self.app)
        producer.publish.side_effect = [KeyError('boom'), None]
        with patching:
            result = store.replay()
        assert [r['id'] for r in result['replayed']] == [good['id']]
        assert [e['id'] for e in result['skipped']['error']] == [bad['id']]
        # only the successfully published entry is marked as requeued.
        assert store.get(bad['id'])['requeued_at'] is None
        assert store.get(good['id'])['requeued_at'] is not None

    def test_replay_reenqueued_failure_can_be_replayed_again(self):
        # a task that is replayed and fails again produces a new entry,
        # which itself is replayable.
        store = self.store()
        task_id = 'tid'
        store.add(self.make_entry(task_id=task_id, failed_at=1.0))
        producer, patching = self.patched_producer(self.app)
        with patching:
            store.replay()
            # the replayed delivery fails again: new entry, same task id.
            store.add(self.make_entry(task_id=task_id, failed_at=2.0))
            result = store.replay()
        assert len(result['replayed']) == 1
        # the older, already requeued entry is not enqueued again.
        assert producer.publish.call_count == 2
        skipped = result['skipped']
        assert len(skipped['duplicate'] + skipped['requeued']) == 1


class test_request_capture(DeadLetterCase):

    def dead_letters(self):
        return self.app.dead_letters.find(include_requeued=True)

    def test_failure_is_captured(self):
        job = self.xRequest()
        try:
            raise MaxRetriesExceededError('no more retries')
        except MaxRetriesExceededError:
            exc_info = ExceptionInfo()
        job.on_failure(exc_info)
        entries = self.dead_letters()
        assert len(entries) == 1
        entry = entries[0]
        assert entry['task_id'] == job.id
        assert entry['reason'] == REASON_FAILURE
        assert entry['exc_type'] == 'MaxRetriesExceededError'
        assert entry['traceback'] == exc_info.traceback

    def test_reject_without_requeue_is_captured(self):
        job = self.xRequest()
        try:
            raise Reject('no thank you')
        except Reject:
            exc_info = ExceptionInfo()
        job.on_failure(exc_info)
        entries = self.dead_letters()
        assert len(entries) == 1
        assert entries[0]['reason'] == REASON_REJECTED
        assert entries[0]['exc_type'] == 'Reject'

    def test_reject_with_requeue_is_not_captured(self):
        job = self.xRequest()
        try:
            raise Reject('try again', requeue=True)
        except Reject:
            exc_info = ExceptionInfo()
        job.on_failure(exc_info)
        assert self.dead_letters() == []

    def test_retry_is_not_captured(self):
        job = self.xRequest()
        try:
            raise Retry('try again')
        except Retry:
            exc_info = ExceptionInfo()
        job.on_failure(exc_info)
        assert self.dead_letters() == []

    def test_ignore_is_not_captured(self):
        job = self.xRequest()
        try:
            raise Ignore()
        except Ignore:
            exc_info = ExceptionInfo()
        job.on_failure(exc_info)
        assert self.dead_letters() == []

    def test_requeued_worker_lost_is_not_captured(self):
        self.mytask.acks_late = True
        self.mytask.reject_on_worker_lost = True
        job = self.xRequest()
        try:
            raise WorkerLostError('worker died')
        except WorkerLostError:
            exc_info = ExceptionInfo()
        job.on_failure(exc_info)
        assert self.dead_letters() == []

    def test_worker_lost_without_requeue_is_captured(self):
        job = self.xRequest()
        try:
            raise WorkerLostError('worker died')
        except WorkerLostError:
            exc_info = ExceptionInfo()
        job.on_failure(exc_info)
        entries = self.dead_letters()
        assert len(entries) == 1
        assert entries[0]['reason'] == REASON_FAILURE

    def test_broken_store_does_not_break_on_failure(self):
        job = self.xRequest()
        with patch.object(type(self.app), 'dead_letters',
                          new=property(lambda app: 1 / 0)):
            try:
                raise KeyError('x')
            except KeyError:
                exc_info = ExceptionInfo()
            job.on_failure(exc_info)  # must not raise
        assert self.dead_letters() == []

    def test_capture_disabled_by_max_entries(self, monkeypatch):
        monkeypatch.setattr(
            self.app.conf, 'worker_dead_letter_max_entries', 0)
        job = self.xRequest()
        try:
            raise KeyError('x')
        except KeyError:
            exc_info = ExceptionInfo()
        job.on_failure(exc_info)
        assert self.dead_letters() == []


class test_module:

    def test_reasons_documented(self):
        assert deadletter.REASONS == {
            REASON_FAILURE, REASON_REJECTED, 'timeout'}


class test_replay_integration(DeadLetterCase):
    """End-to-end: publish -> fail -> capture -> replay -> consume."""

    def test_replayed_message_is_delivered(self):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y

        # 1. publish a real task message to the (memory) broker.
        task_id = add.apply_async((2, 3)).id

        # 2. consume it and let it fail permanently.
        with self.app.connection_for_read() as conn:
            with conn.SimpleQueue('testcelery') as queue:
                message = queue.get_nowait()
        request = Request(
            message, app=self.app,
            on_ack=Mock(name='on_ack'), on_reject=Mock(name='on_reject'))
        try:
            raise MaxRetriesExceededError('no more retries')
        except MaxRetriesExceededError:
            exc_info = ExceptionInfo()
        request.on_failure(exc_info)

        # 3. the failure is collected with the original message.
        (entry,) = self.app.dead_letters.find()
        assert entry['task_id'] == task_id
        assert entry['exc_type'] == 'MaxRetriesExceededError'

        # 4. replay puts a consumable message back on the queue.
        result = self.app.dead_letters.replay()
        assert len(result['replayed']) == 1
        with self.app.connection_for_read() as conn:
            with conn.SimpleQueue('testcelery') as queue:
                redelivered = queue.get_nowait()
        args, kwargs, _embed = redelivered.payload
        assert redelivered.headers['id'] == task_id
        assert redelivered.headers['retries'] == 0
        assert list(args) == [2, 3]
        assert kwargs == {}
