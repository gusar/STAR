class NoneString(Exception):
    def __init__(self):
        self.message = 'String expected, None found.'
