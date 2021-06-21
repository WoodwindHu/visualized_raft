class AppendEntries:
    def __init__(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries
        self.leader_commit = leader_commit