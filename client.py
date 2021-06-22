from raft.command import Command
import requests
import json
import argparse
from raft.client import Client

parser = argparse.ArgumentParser()
parser.add_argument('-p', type=int, default=5000, help='port')
parser.add_argument('-c', type=str, default='set', help='command, set or add')
parser.add_argument('-n', type=int, default=0, help='num to set or to add')
args = parser.parse_args()

client = Client()
with client as session:
    command = Command(args.c, args.n)
    r = requests.post('http://localhost:500{}/raft/receive_client_command'.format(args.p), json=json.dumps(command))
    print(r.json())
