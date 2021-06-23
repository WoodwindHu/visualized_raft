from .NodeState import NodeState
from .cluster import Node
import logging
from .LogEntry import LogEntry
import numpy as np

logging.basicConfig(format='%(asctime)s - %(levelname)s: %(message)s', datefmt='%H:%M:%S', level=logging.INFO)


class Follower(NodeState):
    def __init__(self, node: Node):
        super(Follower, self).__init__(node)
        self.leader = None
        self.commit_index = 0
        self.last_applied_index = 0
        # next log entry to be sent by leader
        # self.nextIndex = 0
        # index of highest log entry known to be replicated on server
        # self.matchIndex = 0
        self.entries = np.array([LogEntry(0,0,None) for i in range(101)])

    # def __init__(self, nodestate: NodeState):
    #     super(Follower, self).__init__(nodestate.node)
    #     self.leader = None
    #     self.commit_index = nodestate.commit_index
    #     self.last_applied_index = nodestate.last_applied_index
    #     self.entries = nodestate.entries
    #     self.current_term = nodestate.current_term

    def __repr__(self):
        return f'{type(self).__name__, self.node.id, self.current_term}'
