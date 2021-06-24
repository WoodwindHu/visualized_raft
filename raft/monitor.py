import os

import grequests
from enum import Enum
from .NodeState import NodeState
from .client import Client
import logging

MONITOR_URL_STATE_UPDATE = 'http://127.0.0.1:7000/monitor/state'
MONITOR_URL_HEARTBEAT = 'http://127.0.0.1:7000/monitor/heartbeat'

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)


class TimeUnit(Enum):
    SECOND = 1
    MS = 2


def send_state_update(node_state: NodeState, election_timeout, value, time_unit=TimeUnit.SECOND):
    client = Client()
    timeout = election_timeout
    if time_unit == TimeUnit.SECOND:
        timeout = timeout * 1000
    state = {
        "pid": os.getpid(),
        "id": node_state.id,
        "term": node_state.current_term,
        "state": type(node_state).__name__.lower(),
        "value": value,
        "index": node_state.last_applied_index,
        "commit": node_state.commit_index
    }
    try:
        with client as session:
            logging.info(f'send state update to monitor: {state}')
            posts = [grequests.post(MONITOR_URL_STATE_UPDATE, json=state, session=session, timeout=1.0)]
            for response in grequests.imap(posts):
                result = response.json()
                logging.info(f'get response from monitor: {result}')
    except:
        logging.info(f'cannot connect to monitor: {MONITOR_URL_STATE_UPDATE}')


def send_heartbeat(node_state: NodeState, election_timeout, value, time_unit=TimeUnit.SECOND):
    client = Client()
    timeout = int(election_timeout)
    if time_unit == TimeUnit.SECOND:
        timeout = timeout * 1000
    state = {
        "pid": os.getpid(),
        "id": node_state.id,
        "term": node_state.current_term,
        "state": type(node_state).__name__.lower(),
        "value": value,
        "timeout": timeout
    }
    try:
        with client as session:
            logging.info(f'send heartbeat to monitor: {state}')
            posts = [grequests.post(MONITOR_URL_HEARTBEAT, json=state, session=session, timeout=1.0)]
            for response in grequests.imap(posts):
                result = response.json()
                logging.info(f'get response from monitor: {result}')
    except:
        logging.info(f'cannot connect to monitor: {MONITOR_URL_STATE_UPDATE}')