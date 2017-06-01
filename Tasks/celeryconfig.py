## Broker settings.
broker_url = 'redis://bj*@10.25.115.53:25801/15'

# List of modules to import when the Celery worker starts.
# imports = ('Tasks.tasks',)

## Using the database to store task state and results.
result_backend = 'redis://bj*@10.25.115.53:25801/14'

# task_annotations = {'tasks.add': {'rate_limit': '10/s'}}