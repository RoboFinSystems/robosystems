"""Infrastructure monitoring and maintenance tasks."""

# Import all infrastructure tasks to register them with Celery
from . import auth_cleanup  # noqa
# Note: instance_monitor has been migrated to Lambda (bin/lambda/lbug_instance_monitor.py)
