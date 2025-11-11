"""
Celery tasks for testing Go publisher compatibility.
These tasks verify that messages published by the Go client
can be consumed and executed by Python Celery workers.
"""

import json
import os
from celery import Celery

# Get broker URL from environment variable
BROKER_URL = os.getenv('BROKER_URL', 'amqp://guest:guest@rabbitmq:5672/')

# Initialize Celery app
app = Celery('test_tasks', broker=BROKER_URL)

# Configure Celery
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
)


@app.task(name='tasks.add')
def add(*args, **kwargs):
    """
    Test task that adds numbers from args.
    Example: add(1, 2, 3) -> 6
    """
    print(f"[ADD] Received args: {args}, kwargs: {kwargs}")

    # Write result to file for test verification
    result = sum(args) if args else 0
    result_file = '/tmp/task_results.json'

    with open(result_file, 'a') as f:
        f.write(json.dumps({
            'task': 'tasks.add',
            'args': args,
            'kwargs': kwargs,
            'result': result
        }) + '\n')

    return result


@app.task(name='tasks.process_data')
def process_data(*args, **kwargs):
    """
    Test task that processes data from kwargs.
    Example: process_data(user_id=123, action='update')
    """
    print(f"[PROCESS_DATA] Received args: {args}, kwargs: {kwargs}")

    # Write result to file for test verification
    result_file = '/tmp/task_results.json'

    with open(result_file, 'a') as f:
        f.write(json.dumps({
            'task': 'tasks.process_data',
            'args': args,
            'kwargs': kwargs,
            'result': 'processed'
        }) + '\n')

    return 'processed'


@app.task(name='tasks.complex_operation')
def complex_operation(*args, **kwargs):
    """
    Test task that handles both args and kwargs.
    Example: complex_operation('data', 42, priority=5, retry=False)
    """
    print(f"[COMPLEX_OPERATION] Received args: {args}, kwargs: {kwargs}")

    # Write result to file for test verification
    result = {
        'args_count': len(args),
        'kwargs_count': len(kwargs),
        'processed': True
    }
    result_file = '/tmp/task_results.json'

    with open(result_file, 'a') as f:
        f.write(json.dumps({
            'task': 'tasks.complex_operation',
            'args': args,
            'kwargs': kwargs,
            'result': result
        }) + '\n')

    return result


@app.task(name='tasks.batch_process')
def batch_process(*args, **kwargs):
    """
    Test task for batch processing.
    """
    print(f"[BATCH_PROCESS] Received args: {args}, kwargs: {kwargs}")

    # Write result to file for test verification
    result_file = '/tmp/task_results.json'

    with open(result_file, 'a') as f:
        f.write(json.dumps({
            'task': 'tasks.batch_process',
            'args': args,
            'kwargs': kwargs,
            'result': 'batch_processed'
        }) + '\n')

    return 'batch_processed'


if __name__ == '__main__':
    # Start the Celery worker
    app.worker_main([
        'worker',
        '--loglevel=info',
        '--concurrency=2',
        '--queues=test-queue-args-only,test-queue-kwargs-only,test-queue-both,test-queue-multiple'
    ])
