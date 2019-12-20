import logging.config
from structlog import configure, processors, stdlib, threadlocal

# Found in https://blog.sneawo.com/blog/2017/07/28/json-logging-in-python/

logging.config.dictConfig({
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'json': {
            'format': '%(message)s %(lineno)d %(pathname)s',
            'class': 'pythonjsonlogger.jsonlogger.JsonFormatter'
        }
    },
    'handlers': {
        'json': {
            'class': 'logging.StreamHandler',
            'formatter': 'json'
        }
    },
    'loggers': {
        '': {
            'handlers': ['json'],
            'level': "INFO"
        },
        "requests": {
            "level": "DEBUG",
        },
        "elasticsearch": {
            "level": "WARNING"
        },
        "urllib3": {
            "level": "CRITICAL"
        },
        "timy": {
            "level": "ERROR"
        },
    }
})

configure(
    context_class=threadlocal.wrap_dict(dict),
    logger_factory=stdlib.LoggerFactory(),
    wrapper_class=stdlib.BoundLogger,
    processors=[
        stdlib.filter_by_level,
        stdlib.add_logger_name,
        stdlib.add_log_level,
        stdlib.PositionalArgumentsFormatter(),
        processors.TimeStamper(fmt="iso"),
        processors.StackInfoRenderer(),
        processors.format_exc_info,
        processors.UnicodeDecoder(),
        stdlib.render_to_log_kwargs]
)
