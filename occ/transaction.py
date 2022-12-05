import datetime

class Transaction:

    def __init__(self, id, path):
        self.id = id
        self.statements = []
        self.affected_record_names = []
        self.read_set = set()
        self.write_set = set()
        self.startTS = None
        self.validationTS = None
        self.finishTS = None
        lines = []
        with open(path, "r") as f:
            for line in f:
                s = list(line.rstrip("\n"))
                lines.append(s)
        for line in lines:
            if ((line[0].upper() == "R") or (line[0].upper() == "W")) and (line[1] == "(") and (line[-1] == ")"):
                self.statements.append((line[0].upper(), "".join(line[2:-1])))
            else:
                raise SystemExit(IOError("\"" + str(line) + "\" tidak sesuai format"))
        for statement in self.statements:
            if statement[1] not in self.affected_record_names:
                self.affected_record_names.append(statement[1])
            if statement[0] == "R":
                self.read_set.add(statement[1])
            else:
                self.write_set.add(statement[1])
    
    def get_id(self):
        return self.id
    
    def get_statements(self):
        return self.statements

    def get_affected_record_names(self):
        return self.affected_record_names
    
    def get_read_set(self):
        return self.read_set

    def get_write_set(self):
        return self.write_set

    def get_startTS(self):
        return self.startTS
    
    def get_validationTS(self):
        return self.validationTS
    
    def get_finishTS(self):
        return self.finishTS

    def set_startTS(self):
        self.startTS = datetime.datetime.now()
    
    def set_validationTS(self):
        self.validationTS = datetime.datetime.now()
    
    def set_finishTS(self):
        self.finishTS = datetime.datetime.now()
    
    def clear_TS(self):
        self.startTS = None
        self.validationTS = None
        self.finishTS = None