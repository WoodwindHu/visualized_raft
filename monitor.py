import json

from gevent import monkey

from flask import render_template
monkey.patch_all()
from flask import Flask, jsonify, request
from raft.cluster import CLUSTER_SIZE
import os
import logging
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

app = Flask(__name__)

class State:
    def __init__(self, pid=0, id=0, term=0, state='F', value=0, index=0, commit=0):
        self.pid = pid
        self.id = id
        self.term = term
        self.state = state
        self.value = value
        self.index = index
        self.commit = commit

states = [State(id=i) for i in range(CLUSTER_SIZE)]

@app.route('/monitor/state', methods=['POST'])
def update_state():
    r = request.get_json()

    id = r['id']
    states[id].pid = r['pid']
    states[id].term = r['term']
    if r['state'] == 'follower':
        states[id].state = 'F'
    elif r['state'] == 'candidate':
        states[id].state = 'C'
    else:
        states[id].state = 'L'
    states[id].value = r['value']
    states[id].commit = r['commit']
    states[id].index = r['index']
    print('\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n=========================================================================')
    print('| id \t\t| {} \t\t| {} \t\t| {} \t\t| {} \t\t| {} \t\t|'.format(0, 1, 2, 3, 4))
    print('| pid \t\t| {} \t| {} \t| {} \t| {} \t| {} \t|'.format(
        states[0].pid, states[1].pid, states[2].pid, states[3].pid, states[4].pid))
    print('| state \t| {} \t\t| {} \t\t| {} \t\t| {} \t\t| {} \t\t|'.format(
        states[0].state, states[1].state, states[2].state, states[3].state, states[4].state))
    print('| term \t\t| {} \t\t| {} \t\t| {} \t\t| {} \t\t| {} \t\t|'.format(
        states[0].term, states[1].term, states[2].term, states[3].term, states[4].term))
    print('| apply \t| {} \t\t| {} \t\t| {} \t\t| {} \t\t| {} \t\t|'.format(
        states[0].index, states[1].index, states[2].index, states[3].index, states[4].index))
    print('| commit \t| {} \t\t| {} \t\t| {} \t\t| {} \t\t| {} \t\t|'.format(
        states[0].commit, states[1].commit, states[2].commit, states[3].commit, states[4].commit))
    print('| value \t| {} \t\t| {} \t\t| {} \t\t| {} \t\t| {} \t\t|'.format(
        states[0].value, states[1].value, states[2].value, states[3].value, states[4].value))
    print('=========================================================================')

    d = {'success': True}
    return jsonify(d)


@app.route('/monitor/heartbeat', methods=['POST'])
def heartbeat():
    d = {'success': True}
    return jsonify(d)


@app.route('/')
def hello_raft():
    return render_template('index.html')

if __name__ == '__main__':
    app.run()