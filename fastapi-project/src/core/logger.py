from logging import config

from src.core.api_settings import settings

LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {'format': settings.log_format},
        'default': {
            '()': 'uvicorn.logging.DefaultFormatter',
            'fmt': '%(levelprefix)s %(message)s',
            'use_colors': None,
        },
        'access': {
            '()': 'uvicorn.logging.AccessFormatter',
            'fmt': "%(asctime)s - uvicorn - %(client_addr)s - %(levelname)s - '%(request_line)s' %(status_code)s",
        },
    },
    'handlers': {
        'console': {
            'level': settings.console_log_lvl,
            'class': 'logging.StreamHandler',
            'formatter': 'verbose',
        },
        'default': {
            'formatter': 'default',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
        'access': {
            'formatter': 'access',
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
    },
    'loggers': {
        '': {
            'handlers': settings.log_default_handlers,
            'level': settings.loggers_handlers_log_lvl,
        },
        'uvicorn.error': {
            'level': settings.unicorn_error_log_lvl,
        },
        'uvicorn.access': {
            'handlers': ['access'],
            'level': settings.unicorn_access_log_lvl,
            'propagate': False,
        },
    },
    'root': {
        'level': settings.root_log_lvl,
        'formatter': 'verbose',
        'handlers': settings.log_default_handlers,
    },
}

def setup_logging():
    config.dictConfig(LOGGING)
