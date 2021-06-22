import json

from gevent import monkey

from flask import render_template
monkey.patch_all()
import logging
import os
from flask import Flask, jsonify, request
from raft.cluster import Cluster
from raft.timer_thread import TimerThread
from raft.AppendEntries import AppendEntries
from raft.LogEntry import LogEntry
from raft.command import Command

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)

NODE_ID = int(os.environ.get('NODE_ID'))
cluster = Cluster()
node = cluster[NODE_ID]
timer_thread = TimerThread(NODE_ID)


def create_app():
    raft = Flask(__name__)
    timer_thread.start()
    return raft


app = create_app()

def json2AppendEntries(s):
    entries = LogEntry(s['entries']['index'], s['entries']['term'], s['entries']['payload']) \
        if s['entries'] != None else None
    return AppendEntries(s['term'], s['leader_id'], s['prev_log_index'], s['prev_log_term'], entries,
                         s['leader_commit'])

def json2Command(s):
    command = Command(s[0], s[1])
    return command


@app.route('/raft/vote', methods=['POST'])
def request_vote():
    vote_request = request.get_json()
    result = timer_thread.vote(json.loads(vote_request))
    return jsonify(result)


@app.route('/raft/heartbeat', methods=['POST'])
def heartbeat():
    heartbeat_request = request.get_json()
    append_entries = json.loads(heartbeat_request)
    append_entries = json2AppendEntries(append_entries)
    success, term = timer_thread.append_entries_reponse(append_entries)
    d = {"success": success, "term": term, "node": timer_thread.node.id}
    return jsonify(d)

@app.route('/raft/receive_client_command', methods=['POST'])
def receive_client_command():
    command = request.get_json()
    command = json2Command(json.loads(command))
    success = timer_thread.receive_client_command(command)
    d = {'success':success}
    return jsonify(d)


@app.route('/')
def hello_raft():
    return f'raft cluster: {cluster}!'

if __name__ == '__main__':
    create_app()
    app.run()