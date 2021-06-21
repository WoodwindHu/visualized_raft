class LogEntry:
    def __init__(self, index, term, payload):
        self.index = index
        self.term = term
        self.payload = payload
