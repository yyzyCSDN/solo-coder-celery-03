"""Persistent storage for tasks that reached their terminal failure state."""
from .base import DeadLetter, DeadLetterStore
from .sqlite import SqliteDeadLetterStore

__all__ = ('DeadLetter', 'DeadLetterStore', 'SqliteDeadLetterStore')
