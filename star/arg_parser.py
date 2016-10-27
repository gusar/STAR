import argparse

class ArgParser:
    def __init__(self):
        self.arg_parser = argparse.ArgumentParser()

    def add_original_repo(self):
        self.arg_parser.add_argument('-or', '--original-repo',
                                     required=True,
                                     help='Full dir path to the original large JSON files')
