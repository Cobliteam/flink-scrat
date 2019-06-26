import os
import yaml
import logging
import requests

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


def handle_response(req_response):
        req_response.raise_for_status()
        return req_response.json()


def find_manager_address(yarn_address, yarn_port, app_name):
    apps_info = _get_apps_info(yarn_address, yarn_port)

    running_app = _get_running_app(apps_info, app_name)

    rpc_addres_info = _build_rpc_address_info(running_app)

    return rpc_addres_info


def _get_apps_info(yarn_address, yarn_port):
    route = "http://{}:{}/ws/v1/cluster/apps".format(yarn_address, yarn_port)

    result = handle_response(requests.get(route))

    apps_info = result['apps']['app']

    return apps_info


def _get_running_app(apps_info, app_name):
    running_apps = list(filter(lambda x: x['state'] == 'RUNNING' and x['name'] == app_name, apps_info))

    if len(running_apps) == 0:
        raise Exception("No app found with state=<RUNNING> and name=<{}>".format(app_name))
    elif len(running_apps) > 1:
        raise Exception("More then one app found with state=<RUNNING> and name=<{}>".format(app_name))

    running_app = running_apps[0]

    return running_app


def _build_rpc_address_info(running_app):
    rpc_address, rpc_port = running_app['amRPCAddress'].split(":")

    rpc_info = {
        "rpc_address": rpc_address,
        "rpc_port": rpc_port
    }

    return rpc_info
