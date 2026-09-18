"""Interfaces for terminal-failure dead-letter stores."""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, List, Optional, Sequence

from celery import states

FAILED = states.FAILURE
REPLAYING = 'REPLAYING'
QUEUED = 'QUEUED'
SUCCEEDED = states.SUCCESS


@dataclass
class DeadLetter:
    """A task message which has reached a terminal failure state."""

    task_id: str
    task_name: str
    args: Any = None
    kwargs: Any = None
    argsrepr: str = ''
    kwargsrepr: str = ''
    exception_type: str = ''
    exception_message: str = ''
    traceback: str = ''
    message_body: Optional[bytes] = None
    message_headers: dict = field(default_factory=dict)
    message_properties: dict = field(default_factory=dict)
    content_type: Optional[str] = None
    content_encoding: Optional[str] = None
    queue: Optional[str] = None
    exchange: Optional[str] = None
    routing_key: Optional[str] = None
    hostname: Optional[str] = None
    status: str = FAILED
    attempts: int = 1
    failures: List[dict] = field(default_factory=list)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    leased_until: Optional[datetime] = None
    replay_batch_id: Optional[str] = None

    def to_dict(self):
        return {
            'id': self.task_id,
            'name': self.task_name,
            'args': self.argsrepr,
            'kwargs': self.kwargsrepr,
            'exception_type': self.exception_type,
            'exception_message': self.exception_message,
            'queue': self.queue,
            'exchange': self.exchange,
            'routing_key': self.routing_key,
            'hostname': self.hostname,
            'status': self.status,
            'attempts': self.attempts,
            'failures': self.failures,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
            'leased_until': self.leased_until,
            'replay_batch_id': self.replay_batch_id,
        }


class DeadLetterStore(ABC):
    """Storage interface used by the worker and the dead-letter CLI."""

    @abstractmethod
    def archive(self, dead_letter: DeadLetter) -> None:
        """Create or append a terminal failure for ``dead_letter.task_id``."""

    @abstractmethod
    def list(
        self,
        *,
        task_id: Optional[Iterable[str]] = None,
        task_name: Optional[str] = None,
        exception_type: Optional[str] = None,
        queue: Optional[str] = None,
        hostname: Optional[str] = None,
        status: Optional[Sequence[str]] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: Optional[int] = None,
    ) -> List[DeadLetter]:
        """Return dead letters matching the supplied filters."""

    @abstractmethod
    def get(self, task_id: str) -> Optional[DeadLetter]:
        """Return one dead letter by original task ID."""

    @abstractmethod
    def claim(
        self,
        *,
        task_ids: Optional[Iterable[str]] = None,
        task_name: Optional[str] = None,
        exception_type: Optional[str] = None,
        queue: Optional[str] = None,
        hostname: Optional[str] = None,
        since: Optional[datetime] = None,
        until: Optional[datetime] = None,
        limit: int = 100,
        lease_seconds: int = 300,
        batch_id: Optional[str] = None,
        select_all: bool = False,
    ) -> List[DeadLetter]:
        """Atomically change open failures to :data:`REPLAYING`."""

    @abstractmethod
    def mark_queued(self, task_id: str, batch_id: Optional[str] = None) -> None:
        """Record that a claimed message has been published."""

    @abstractmethod
    def mark_succeeded(self, task_id: str) -> None:
        """Record that a replay completed successfully."""

    @abstractmethod
    def mark_failed(self, task_id: str, dead_letter: DeadLetter) -> None:
        """Return a claimed dead letter to the failed state with a new failure."""
