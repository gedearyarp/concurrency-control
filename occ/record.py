import datetime

class Record:

    def __init__(self, value):
        self.value = value
    
    def read(self):
        return datetime.datetime.now(), self.value

    def write(self, increment):
        self.value += increment
        return datetime.datetime.now()