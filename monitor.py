import json

from gevent import monkey

from flask import render_template
monkey.patch_all()
import logging
from flask import Flask, jsonify, request

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)

app = Flask(__name__)


@app.route('/monitor/state', methods=['POST'])
def update_state():
    pass


@app.route('/monitor/heartbeat', methods=['POST'])
def heartbeat():
    pass


@app.route('/')
def hello_raft():
    return render_template('index.html')

if __name__ == '__main__':
    app.run()