import sys
import threading
from random import randrange
import logging

from .monitor import send_state_update

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)

from .Candidate import Candidate, VoteRequest
from .AppendEntries import AppendEntries
from .Follower import Follower
from .Leader import Leader
from .cluster import Cluster, ELECTION_TIMEOUT_MAX
from .cluster import HEART_BEAT_INTERVAL, ELECTION_TIMEOUT_MAX
import time

from .monitor import send_state_update, send_heartbeat
from .client import Client

import grequests
import json
cluster = Cluster()


class TimerThread(threading.Thread):
    def __init__(self, node_id):
        threading.Thread.__init__(self)
        self.node = cluster[node_id]
        self.node_state = Follower(self.node)
        self.election_timeout = float(randrange(ELECTION_TIMEOUT_MAX / 2, ELECTION_TIMEOUT_MAX))
        self.election_timer = threading.Timer(self.election_timeout, self.become_candidate)

    def heartbeat(self):
        while not self.node_state.stopped:
            logging.info(f'{self} send heartbeat to followers')
            logging.info('========================================================================')
            send_heartbeat(self.node_state, HEART_BEAT_INTERVAL)
            client = Client()
            prev_log_term = self.node_state.entries[self.node_state.last_applied_index].term \
                                if self.node_state.last_applied_index > 0 else 0
            log_entry = AppendEntries(self.node_state.current_term,
                                      self.node_state.node, self.node_state.last_applied_index,
                                      prev_log_term, self.node_state.entry, self.node_state.commit_index)
            with client as session:
                posts = [
                    grequests.post(f'http://{peer.uri}/raft/heartbeat',
                                   json=json.dumps(log_entry, default=lambda obj: obj.__dict__,
                                                   sort_keys=True, indent=4),
                                   session=session)
                    for peer in self.node_state.followers
                ]
                for response in grequests.map(posts, gtimeout=HEART_BEAT_INTERVAL):
                    if response is not None:
                        logging.info(f'{self.node_state} got heartbeat from follower: {response.json()}')
                        r = response.json()
                        success, term = r['success'], r['term']
                        if term > self.node_state.current_term:
                            self.become_follower()
                    else:
                        logging.info(f'{self} got heartbeat from follower: None')
            logging.info('========================================================================')
            time.sleep(HEART_BEAT_INTERVAL)
            self.entry = None

    def become_leader(self):
        logging.info(f'{self} become leader and start to send heartbeat ... ')
        send_state_update(self.node_state, self.election_timeout)
        self.node_state = Leader(self.node_state)
        self.heartbeat()

    def become_candidate(self):
        logging.warning(f'heartbeat is timeout: {int(self.election_timeout)} s')
        logging.info(f'{self} become candidate and start to request vote ... ')
        send_state_update(self.node_state, self.election_timeout)
        self.node_state = Candidate(self.node_state)
        self.node_state.elect()
        if self.node_state.win():
            self.become_leader()
        else:
            self.become_follower()

    # input: candidate (id, term, lastLogIndex, lastLogTerm)
    # output: term, vote_granted
    # rule:
    #   1. return false if candidate.term < current_term
    #   2. return true if (voteFor is None or voteFor==candidate.id) and candidate's log is newer than receiver's
    def vote(self, vote_request: VoteRequest):
        logging.info(f'{self} got vote request: {vote_request} ')
        vote_result = self.node_state.vote(vote_request)
        if vote_result[0]:
            self.become_follower()
        logging.info(f'{self} return vote result: {vote_result} ')
        return vote_result

    def append_entries_reponse(self, append_entries: AppendEntries):
        logging.info(f'{self} got heartbeat from leader: {append_entries.leader_id}')
        success = True
        # Reply false if term < currentTerm
        if append_entries.term < self.node_state.current_term:
            success = False
            return success, self.node_state.current_term
        # Reply false if log doesn’t contain an entry at prevLogIndex
        # whose term matches prevLogTerm
        if append_entries.prev_log_index > 0 and self.node_state.entries[
            append_entries.prev_log_index].term != append_entries.prev_log_term:
            success = False
        if append_entries.entries != None:
            # If an existing entry conflicts with a new one (same index
            # but different terms), delete the existing entry and all that
            # follow it
            if append_entries.entries.index < len(self.node_state.entries) \
                    and append_entries.entries.term != self.node_state.entries[
                append_entries.entries.index].term:
                self.node_state.entries = self.node_state.entries[:append_entries.entries.term]
            # Append any new entries not already in the log
            self.node_state.entries.append(append_entries.entries)
        # If leaderCommit > commitIndex, set commitIndex =
        # min(leaderCommit, index of last new entry)
        if append_entries.leader_commit > self.node_state.commit_index:
            self.node_state.commit_index = min(append_entries.leader_commit, append_entries.entries.index)
        self.become_follower()
        return success, self.node_state.current_term

    def become_follower(self):
        timeout = float(randrange(ELECTION_TIMEOUT_MAX / 2, ELECTION_TIMEOUT_MAX))
        if type(self.node_state) != Follower:
            logging.info(f'{self} become follower ... ')
            self.node_state = Follower(self.node)
        logging.info(f'{self} reset election timer {timeout} s ... ')
        send_state_update(self.node_state, timeout)
        self.election_timer.cancel()
        self.election_timer = threading.Timer(timeout, self.become_candidate)
        self.election_timer.start()

    def run(self):
        self.become_follower()

    @property
    def term(self):
        return self.node_state.current_term

    def __repr__(self):
        return f'{type(self).__name__, self.node_state}'


if __name__ == '__main__':
    timerThread = TimerThread(int(sys.argv[1]))
    timerThread.start()
