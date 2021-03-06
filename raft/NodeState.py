import collections

from .cluster import Cluster
from .LogEntry import LogEntry
import logging
import json

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)

class VoteResult:
    def __init__(self, vote_granted, term, id):
        self.vote_granted = vote_granted
        self.term = term
        self.id = id

    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class NodeState:
    def __init__(self, node=None, current_term=0):
        self.cluster = Cluster()
        self.node = node

        self.id = node.id
        self.current_term = current_term

        self.vote_for = None  # node.id of the voted candidate

    # input: candidate (id, current_term, lastLogIndex, lastLogTerm)
    # output: vote_granted (true/false), term (current_term, for candidate to update itself)
    # rule:
    #   1. return false if candidate.term < current_term
    #   2. return true if (voteFor is None or voteFor==candidate.id) and candidate's log is newer than receiver's
    def vote(self, vote_request):
        term = vote_request.term
        candidate_id = vote_request.candidate_id
        last_log_index = vote_request.last_log_index
        if term > self.current_term:
            logging.info(f'{self} approves vote request since term: {term} > {self.current_term}')
            self.vote_for = candidate_id
            self.current_term = term
            return VoteResult(True, self.current_term, self.id)
        if term < self.current_term:
            logging.info(f'{self} rejects vote request since term: {term} < {self.current_term}')
            return VoteResult(False, self.current_term, self.id)
        # vote_request.term == self.current_term
        if (self.vote_for is None or self.vote_for == candidate_id) and self.last_applied_index <= last_log_index:
            self.vote_for = candidate_id
            return VoteResult(True, self.current_term, self.id)
        logging.info(f'{self} rejects vote request since vote_for: {self.vote_for} != {candidate_id}')
        return VoteResult(False, self.current_term, self.id)

    # another thread might change the state into Follower when got heartbeat
    # only candidate could return True
    # it returns False for both Leader and Follower
    def win(self):
        return False
