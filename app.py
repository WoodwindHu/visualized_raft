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
    entries = LogEntry(s['entries']['index'], s['entries']['term'], s['entries']['payload']) if s['entries'] != None else None
    return AppendEntries(s['term'], s['leader_id'], s['prev_log_index'], s['prev_log_term'], entries,
                         s['leader_commit'])


@app.route('/raft/vote', methods=['POST'])
def request_vote():
    vote_request = request.get_json()
    result = timer_thread.vote(json.loads(vote_request))
    return jsonify(result)


@app.route('/raft/heartbeat', methods=['POST'])
def heartbeat():
    ae_json = request.get_json()
    append_entries = json2AppendEntries(ae_json)
    logging.info(f'{timer_thread} got heartbeat from leader: {append_entries.leader_id}')
    success = True
    # Reply false if term < currentTerm
    if append_entries.term < timer_thread.node_state.current_term:
        success = False
    # Reply false if log doesn’t contain an entry at prevLogIndex
    # whose term matches prevLogTerm
    if append_entries.prev_log_index > 0 and timer_thread.node_state.entries[append_entries.prev_log_index].term != append_entries.prev_log_term:
        success = False
    if append_entries.entries != None:
        # If an existing entry conflicts with a new one (same index
        # but different terms), delete the existing entry and all that
        # follow it
        if append_entries.entries.index < len(timer_thread.node_state.entries) \
                and append_entries.entries.term != timer_thread.node_state.entries[append_entries.entries.index].term:
            timer_thread.node_state.entries = timer_thread.node_state.entries[:append_entries.entries.term]
        # Append any new entries not already in the log
        timer_thread.node_state.entries.append(append_entries.entries)
    # If leaderCommit > commitIndex, set commitIndex =
    # min(leaderCommit, index of last new entry)
    if append_entries.leader_commit > timer_thread.node_state.commit_index:
        timer_thread.node_state.commit_index = min(append_entries.leader_commit, append_entries.entries.index)
    d = {"success": success, "term": timer_thread.node_state.current_term}
    timer_thread.become_follower()
    return jsonify(d)


@app.route('/')
def hello_raft():
    return f'raft cluster: {cluster}!'

if __name__ == '__main__':
    create_app()
    app.run()