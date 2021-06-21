import time
from random import randrange

import grequests
from .NodeState import NodeState
from .client import Client
from .cluster import HEART_BEAT_INTERVAL, ELECTION_TIMEOUT_MAX
from .AppendEntries import AppendEntries
import logging
import json



logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)


class Leader(NodeState):
    def __init__(self, candidate):
        super(Leader, self).__init__(candidate.node)
        self.current_term = candidate.current_term
        self.commit_index = candidate.commit_index
        self.last_applied_index = candidate.last_applied_index
        self.entries = candidate.entries
        self.stopped = False
        self.followers = [peer for peer in self.cluster if peer != self.node]
        self.election_timeout = float(randrange(ELECTION_TIMEOUT_MAX / 2, ELECTION_TIMEOUT_MAX))
        self.next_index = [self.last_applied_index+1 for i in range(len(self.cluster))]
        self.match_index = [0 for i in range(len(self.cluster))]


    def __repr__(self):
        return f'{type(self).__name__, self.node.id, self.current_term}'
