import os
import yaml
import logging
import requests

from envparse import env
from logging import CRITICAL, ERROR, DEBUG, INFO, WARN
from flink_scrat.exceptions import (NoAppsFoundException, MultipleAppsFoundException)


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


def handle_response(req_response):
        req_response.raise_for_status()
        return req_response.json()


def find_manager_address(yarn_address, yarn_port, app_id):
    running_app = _get_running_app(yarn_address, yarn_port, app_id)

    rpc_addres_info = _build_rpc_address_info(running_app)

    return rpc_addres_info


def _get_running_app(yarn_address, yarn_port, app_id):
    route = 'http://{}:{}/ws/v1/cluster/apps'.format(yarn_address, yarn_port)

    params = {
        'states': ["RUNNING"],
        'applicationTags': ["{}".format(app_id)],
        'limit': 2
    }

    result = handle_response(requests.get(route, params=params))
    running_apps_info = result['apps']['app']

    num_running_apps = len(running_apps_info)

    if num_running_apps != 1:
        if num_running_apps == 0:
            raise NoAppsFoundException("No app found with state=<RUNNING> and tag=<{}>".format(app_id))
        else:
            raise MultipleAppsFoundException("More then one app found with state=<RUNNING> and tag=<{}>".format(app_id))

    running_app = running_apps_info[0]

    return running_app


def _build_rpc_address_info(running_app):
    rpc_address, rpc_port = running_app['amRPCAddress'].split(":")

    rpc_info = {
        "rpc_address": rpc_address,
        "rpc_port": rpc_port
    }

    return rpc_info
