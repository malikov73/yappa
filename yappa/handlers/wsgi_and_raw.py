import logging
import os
from importlib import import_module
from pathlib import Path

import httpx

from .common import (
    DEFAULT_CONFIG_FILENAME, body_to_bytes, load_yaml, set_access_token,
    patch_response,
)
from .wsgi import call_app, app, config

logger = logging.getLogger(__name__)


def load_raw(import_path):
    if not import_path:
        raise ValueError("import_path should not be empty")
    *submodules, app_name = import_path.split(".")
    module = import_module(".".join(submodules))
    raw_handler = getattr(module, app_name)
    return raw_handler


raw_handler = load_raw(config.get('raw_handler'))


def handle(event, context):
    set_access_token()
    if not event:
        return {
            'statusCode': 500,
            'body': "got empty event",
        }
    try:
        if 'messages' in event and event['messages'][0]['event_metadata']['event_type'] in (
        'yandex.cloud.events.serverless.triggers.TimerMessage', 'yandex.cloud.events.messagequeue.QueueMessage'):
            return raw_handler(event, context)
        else:
            response = call_app(app, event)
            return patch_response(response)
    except Exception as e:
        logger.error("unhandled error", exc_info=True)
        return {
            "statusCode": 500,
            "body": f"got unhandled exception ({e}). Most likely on "
                    f"Yappa side. See clouds logs for traceback"
        }
