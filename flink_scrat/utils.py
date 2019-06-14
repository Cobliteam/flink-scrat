import os
import yaml
import logging

from envparse import env
from logging import CRITICAL, ERROR, DEBUG, INFO, WARN


def setup_logging():
    """ Setup logging configuration """
    default_level = INFO
    log_level = env('SCRAT_LOG_LEVEL', default=None)
    if log_level:
        level_parser = {
            'CRITICAL': CRITICAL,
            'ERROR': ERROR,
            'WARN': WARN,
            'DEBUG': DEBUG,
            'INFO': INFO}
        default_level = level_parser[log_level]

    path = 'logging.yaml'
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = yaml.safe_load(f.read())
            if log_level:
                loggers = config.setdefault('loggers', {})
                flink_scrat_logger = loggers.setdefault('flink_scrat', {})
                flink_scrat_logger['level'] = default_level
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)
