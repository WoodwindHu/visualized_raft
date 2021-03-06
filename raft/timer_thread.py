import sys
import threading
from random import randrange, random
import logging

from .monitor import send_state_update

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)

from .Candidate import Candidate, VoteRequest
from .AppendEntries import AppendEntries
from .LogEntry import LogEntry
from .Follower import Follower
from .Leader import Leader
from .cluster import Cluster, ELECTION_TIMEOUT_MAX, HEART_BEAT_INTERVAL
import time

from .monitor import send_state_update, send_heartbeat
from .client import Client

import grequests
import json
from .command import Command
import os
cluster = Cluster()


class TimerThread(threading.Thread):
    def __init__(self, node_id):
        threading.Thread.__init__(self)
        self.node = cluster[node_id]
        self.node_state = Follower(self.node)
        self.state = 0
        self.election_timeout = float(randrange(ELECTION_TIMEOUT_MAX / 2, ELECTION_TIMEOUT_MAX))
        self.election_timer = threading.Timer(self.election_timeout, self.become_candidate)
        self.heartbeat_timer = None
        logging.info('{}'.format(os.getpid()))

    def receive_client_command(self, command: Command):
        logging.info(f'{self} got command: {json.dumps(command)}')
        if command.command != 'set' and command.command != 'add':
            return False
        self.node_state.last_applied_index += 1
        self.node_state.entries[self.node_state.last_applied_index]=LogEntry(self.node_state.last_applied_index,
                                                self.node_state.current_term, command)
        self.node_state.match_index[self.node.id] = self.node_state.last_applied_index
        self.node_state.next_index[self.node.id] = self.node_state.match_index[self.node.id] + 1
        send_state_update(self.node_state, 0, self.state)
        # self.heartbeat_timer.cancel()
        # self.heartbeat_timer = threading.Timer(float(HEART_BEAT_INTERVAL), self.heartbeat)
        # self.heartbeat_timer.start()
        return True

    def update_state(self, command: Command):
        if command.command == 'set':
            self.state = command.num
        elif command.command == 'add':
            self.state += command.num
        logging.info(f'{self} update state: {self.state}')
        send_state_update(self.node_state, self.election_timeout, self.state)

    def heartbeat(self):
        logging.info(f'{self} send heartbeat to followers')
        logging.info('========================================================================')
        send_heartbeat(self.node_state, HEART_BEAT_INTERVAL, self.state)
        client = Client()
        with client as session:
            posts = []
            for i, peer in enumerate(self.node_state.followers):
                entry = None
                # If last log index ??? nextIndex for a follower: send
                # AppendEntries RPC with log entries starting at nextIndex
                if self.node_state.last_applied_index >= self.node_state.next_index[peer.id]:
                    entry = self.node_state.entries[self.node_state.next_index[peer.id]]
                prev_log_index = self.node_state.next_index[peer.id] - 1
                prev_log_term = self.node_state.entries[prev_log_index].term
                log_entry = AppendEntries(self.node_state.current_term,
                                          self.node_state.node, prev_log_index,
                                          prev_log_term, entry, self.node_state.commit_index)
                posts.append(grequests.post(f'http://{peer.uri}/raft/heartbeat',
                               json=json.dumps(log_entry, default=lambda obj: obj.__dict__,
                                               sort_keys=True, indent=4),
                               session=session, timeout=1.0))

            for response in grequests.map(posts, gtimeout=HEART_BEAT_INTERVAL):
                if response is not None:
                    logging.info(f'{self.node_state} got heartbeat from follower: {response.json()}')
                    r = response.json()
                    success, term, node_id = r['success'], r['term'], r['node']
                    if term > self.node_state.current_term:
                        self.become_follower()
                    if self.node_state.last_applied_index >= self.node_state.next_index[node_id]:
                        # If successful: update nextIndex and matchIndex for
                        # follower
                        if success:
                            self.node_state.match_index[node_id] = self.node_state.next_index[node_id]
                            self.node_state.next_index[node_id] += 1
                            logging.info(f'update next_index({node_id}) {self.node_state.next_index[node_id]}')
                        # If AppendEntries fails because of log inconsistency:
                        # decrement nextIndex and retry
                        else:
                            self.node_state.next_index[node_id] -= 1
                            logging.info(f'update next_index({node_id}) {self.node_state.next_index[node_id]}')
                else:
                    logging.info(f'{self} got heartbeat from follower: None')

        # If there exists an N such that N > commitIndex, a majority
        # of matchIndex[i] ??? N, and log[N].term == currentTerm:
        # set commitIndex = N
        while self.node_state.commit_index < self.node_state.last_applied_index \
            and self.node_state.entries[self.node_state.commit_index+1].term == self.node_state.current_term:
                count = 0
                for peer in self.node_state.cluster:
                    count += 1 if self.node_state.match_index[peer.id] >= self.node_state.commit_index+1 \
                            else 0
                if count > len(self.node_state.cluster)/2:
                    self.node_state.commit_index += 1
                    self.update_state(self.node_state.entries[self.node_state.commit_index].payload)
                else:
                    break

        logging.info('========================================================================')

        self.heartbeat_timer = threading.Timer(float(HEART_BEAT_INTERVAL), self.heartbeat)
        self.heartbeat_timer.start()

    def become_leader(self):
        logging.info(f'{self} become leader and start to send heartbeat ... ')
        self.election_timer.cancel()
        self.node_state = Leader(self.node_state)
        send_state_update(self.node_state, self.election_timeout, self.state)
        self.heartbeat()

    def become_candidate(self):
        logging.warning(f'heartbeat is timeout: {self.election_timeout} s')
        logging.info(f'{self} become candidate and start to request vote ... ')
        self.election_timer.cancel()
        self.node_state = Candidate(self.node_state)
        send_state_update(self.node_state, self.election_timeout, self.state)
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
        logging.info(f'{self} got vote request: {vote_request.to_json()} ')
        vote_result = self.node_state.vote(vote_request)
        if vote_result.vote_granted:
            self.become_follower()
        logging.info(f'{self} return vote result: {vote_result.to_json()} ')
        return vote_result

    def append_entries_reponse(self, append_entries: AppendEntries):
        logging.info(f'{self} got heartbeat from leader: {append_entries.leader_id}')
        success = True
        # Reply false if term < currentTerm
        if append_entries.term < self.node_state.current_term:
            success = False
            return success, self.node_state.current_term
        # Reply false if log doesn???t contain an entry at prevLogIndex
        # whose term matches prevLogTerm
        if append_entries.prev_log_index > 0 and self.node_state.entries[
            append_entries.prev_log_index].term != append_entries.prev_log_term:
            success = False
        if append_entries.entries is not None:
            # If an existing entry conflicts with a new one (same index
            # but different terms), delete the existing entry and all that
            # follow it
            if append_entries.entries.index < self.node_state.last_applied_index \
                    and append_entries.entries.term != self.node_state.entries[
                append_entries.entries.index].term:
                self.node_state.entries[append_entries.entries.index:] = LogEntry(0,0,None)
            self.node_state.last_applied_index = append_entries.entries.index
            # Append any new entries not already in the log
            self.node_state.entries[append_entries.entries.index] = append_entries.entries
            send_state_update(self.node_state, 0, self.state)
        # If leaderCommit > commitIndex, set commitIndex =
        # min(leaderCommit, index of last new entry)
        if append_entries.leader_commit > self.node_state.commit_index:
            if append_entries.entries:
                commit_index = min(append_entries.leader_commit, append_entries.entries.index)
            else:
                commit_index = append_entries.leader_commit
            while self.node_state.commit_index < commit_index and \
                    self.node_state.commit_index < self.node_state.last_applied_index:
                self.node_state.commit_index += 1
                command = self.node_state.entries[self.node_state.commit_index].payload
                command = Command(command[0], command[1])
                self.update_state(command)
        self.become_follower()
        return success, self.node_state.current_term

    def become_follower(self):
        timeout = ELECTION_TIMEOUT_MAX // 2 + ELECTION_TIMEOUT_MAX // 2 * random()
        self.election_timeout=timeout
        if type(self.node_state) != Follower:
            logging.info(f'{self} become follower ... ')
            self.node_state = Follower(self.node,
                                       current_term=self.node_state.current_term,
                                       commit_index=self.node_state.commit_index,
                                       last_applied_index=self.node_state.last_applied_index,
                                       entries=self.node_state.entries)
            self.node_state.leader = None
        logging.info(f'{self} reset election timer {timeout} s ... ')
        send_state_update(self.node_state, timeout, self.state)
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
