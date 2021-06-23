import json

import grequests
from .NodeState import NodeState
from .client import Client
import logging

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)


class VoteRequest:
    def __init__(self, *args):
        if len(args) == 1:
            candidate = args[0]
            self.candidate_id = candidate.id
            self.term = candidate.current_term
            self.last_log_index = candidate.last_applied_index
            self.last_log_term = candidate.entries[candidate.last_applied_index].term
        else:
            self.candidate_id = args[0]
            self.term = args[1]
            self.last_log_index = args[2]
            self.last_log_term = args[3]


    def to_json(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)


class Candidate(NodeState):
    """ The state of candidate

    The service will become candidate if no heartbeat is heard from leader
    after the election timeout comes.

    Candidate will start a new leader election with current_term+1.
    If the candidate can get quorum votes before the election timeout comes,
    it becomes the leader and send heartbeat to followers immediately.
    Otherwise, start a new election with current_term+1.
    TODO add pre election later
    """

    def __init__(self, follower):
        super(Candidate, self).__init__(follower.node)
        self.current_term = follower.current_term
        self.commit_index = follower.commit_index
        self.last_applied_index = follower.last_applied_index
        self.votes = []
        self.entries = follower.entries
        self.followers = [peer for peer in self.cluster if peer.id != self.node.id]
        self.vote_for = self.id  # candidate always votes itself

    def elect(self):
        """ When become to candidate and start to elect:
            1. term + 1
            2. vote itself
            3. send the vote request (VR) to each peer in parallel
            4. return when it is timeout
        """
        self.current_term = self.current_term + 1
        logging.info(f'{self} sends vote request to peers ')
        # vote itself
        self.votes.append(self.node)
        client = Client()
        with client as session:
            posts = []
            # VoteRequest(self)
            for peer in self.followers:
                logging.info(f'{self} sends request to {peer}')
                posts.append(grequests.post(f'http://{peer.uri}/raft/vote', json=VoteRequest(self).to_json(), session=session))
            for response in grequests.imap(posts):
                logging.info(f'{self} got vote result: {response.status_code}: {response.json()}')
                result = response.json()
                if result['vote_granted']:  # vote_granted
                    self.votes.append(result['id'])  # id

    def win(self):
        return len(self.votes) > len(self.cluster) / 2

    def __repr__(self):
        return f'{type(self).__name__, self.node.id, self.current_term}'
