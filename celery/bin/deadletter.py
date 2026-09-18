"""Inspect and replay messages in the terminal-failure dead-letter store."""
import json as jsonlib

import click

from celery.bin.base import ISO8601, CeleryCommand, CeleryOption, handle_preload_options
from celery.dead_letters.replay import get_store, replay_dead_letters


def _filters(
    task_id, task, exception, queue, hostname, status, since, until,
):
    return {
        'task_id': task_id,
        'task_name': task,
        'exception_type': exception,
        'queue': queue,
        'hostname': hostname,
        'status': status,
        'since': since,
        'until': until,
    }


def _replay_filters(task_id, task, exception, queue, hostname, since, until):
    return {
        'task_ids': task_id,
        'task_name': task,
        'exception_type': exception,
        'queue': queue,
        'hostname': hostname,
        'since': since,
        'until': until,
    }


def _print_json(context, value):
    context.echo(jsonlib.dumps(value, default=str, indent=2, ensure_ascii=False))


@click.group(name='dead-letter', cls=CeleryCommand)
@click.pass_context
@handle_preload_options
def deadletter(ctx):
    """Inspect and replay terminally failed task messages."""


@deadletter.command(name='list', cls=CeleryCommand)
@click.option('-i', '--task-id', multiple=True, cls=CeleryOption, help_group='Filters')
@click.option('-t', '--task', cls=CeleryOption, help_group='Filters')
@click.option('-e', '--exception', cls=CeleryOption, help_group='Filters')
@click.option('-q', '--queue', cls=CeleryOption, help_group='Filters')
@click.option('-w', '--hostname', cls=CeleryOption, help_group='Filters')
@click.option('--status', multiple=True, cls=CeleryOption, help_group='Filters')
@click.option('--since', cls=CeleryOption, type=ISO8601, help_group='Filters')
@click.option('--until', cls=CeleryOption, type=ISO8601, help_group='Filters')
@click.option('--limit', default=100, type=click.IntRange(1), cls=CeleryOption, help_group='Output')
@click.option('--json', 'as_json', is_flag=True, cls=CeleryOption, help_group='Output')
@click.pass_context
def list_deadletters(
    ctx, task_id, task, exception, queue, hostname, status, since, until,
    limit, as_json,
):
    """List final failures and their replay state."""
    store = get_store(ctx.obj.app, enabled_only=False)
    filters = _filters(
        list(task_id) or None, task, exception, queue, hostname,
        tuple(status) or None, since, until,
    )
    messages = store.list(limit=limit, **filters)

    if as_json:
        _print_json(ctx.obj, [message.to_dict() for message in messages])
    else:
        for message in messages:
            ctx.obj.echo(
                f'{message.task_id}\t{message.status}\t{message.task_name}\t'
                f'{message.exception_type}: {message.exception_message}'
            )


@deadletter.command(cls=CeleryCommand)
@click.argument('task_id')
@click.option('--json', 'as_json', is_flag=True, cls=CeleryOption)
@click.pass_context
def show(ctx, task_id, as_json):
    """Show one dead letter, including arguments and the original message."""
    message = get_store(ctx.obj.app, enabled_only=False).get(task_id)
    if message is None:
        ctx.fail(f'No dead letter found for task {task_id}')
    payload = message.to_dict()
    payload.update({
        'message_body': message.message_body,
        'message_headers': message.message_headers,
        'message_properties': message.message_properties,
        'content_type': message.content_type,
        'content_encoding': message.content_encoding,
        'args': message.args,
        'kwargs': message.kwargs,
        'traceback': message.traceback,
    })
    if as_json:
        _print_json(ctx.obj, payload)
    else:
        ctx.obj.echo(jsonlib.dumps(payload, default=str, indent=2, ensure_ascii=False))


@deadletter.command(name='replay', cls=CeleryCommand)
@click.option('-i', '--task-id', multiple=True, cls=CeleryOption, help_group='Selection')
@click.option('-t', '--task', cls=CeleryOption, help_group='Selection')
@click.option('-e', '--exception', cls=CeleryOption, help_group='Selection')
@click.option('-q', '--queue', cls=CeleryOption, help_group='Selection')
@click.option('-w', '--hostname', cls=CeleryOption, help_group='Selection')
@click.option('--since', cls=CeleryOption, type=ISO8601, help_group='Selection')
@click.option('--until', cls=CeleryOption, type=ISO8601, help_group='Selection')
@click.option('--limit', default=100, type=click.IntRange(1), cls=CeleryOption, help_group='Selection')
@click.option('--all', 'select_all', is_flag=True, cls=CeleryOption, help_group='Selection')
@click.option('--keep-retries/--reset-retries', default=False, cls=CeleryOption, help_group='Replay')
@click.option('--keep-schedule', is_flag=True, cls=CeleryOption, help_group='Replay')
@click.option('--dry-run', is_flag=True, cls=CeleryOption, help_group='Replay')
@click.option('--lease-seconds', cls=CeleryOption, type=click.IntRange(1),
              default=None, help_group='Replay')
@click.option('--json', 'as_json', is_flag=True, cls=CeleryOption, help_group='Output')
@click.pass_context
def replay(
    ctx, task_id, task, exception, queue, hostname, since, until, limit,
    select_all, keep_retries, keep_schedule, dry_run, lease_seconds, as_json,
):
    """Claim selected failures and republish their original messages."""
    app = ctx.obj.app
    kwargs = _replay_filters(
        list(task_id) or None, task, exception, queue, hostname, since, until,
    )
    kwargs.update({
        'limit': limit,
        'select_all': select_all,
        'reset_retries': not keep_retries,
        'clear_schedule': not keep_schedule,
        'dry_run': dry_run,
    })
    if lease_seconds is not None:
        kwargs['lease_seconds'] = lease_seconds
    else:
        kwargs['lease_seconds'] = app.conf.dead_letter_lease_seconds

    queued, skipped = replay_dead_letters(app, store=get_store(app, enabled_only=False), **kwargs)
    if as_json:
        _print_json(ctx.obj, {
            'queued': [message.to_dict() for message in queued],
            'skipped': skipped,
            'dry_run': dry_run,
        })
    else:
        ctx.obj.echo(f'{len(queued)} {"selected" if dry_run else "queued"}, {len(skipped)} skipped')
        for message in queued:
            ctx.obj.echo(f'QUEUED\t{message.task_id}\t{message.task_name}')
        for item in skipped:
            ctx.obj.echo(f'SKIPPED\t{item["task_id"]}\t{item["reason"]}')
