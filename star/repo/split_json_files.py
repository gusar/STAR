import os
import logging
from glob import glob
import smart_open as so

from star import utils


def get_args():
    arg_parser = utils.ArgParser()
    arg_parser.add_original_repo(True)
    arg_parser.add_partitioned_repo(True)
    arg_parser.add_split_size(True)
    return arg_parser.parse()


def split_stocktwits_files():
    args = get_args()
    logging.info('Split size: ' + str(args.split_size))
    logging.info('Input DIR: ' + args.original_repo)
    logging.info('Output DIR: ' + args.partitioned_repo)

    splitter = FileSplitter(args.original_repo, args.partitioned_repo, args.split_size)
    return splitter.split_compressed_files()


class FileSplitter:

    def __init__(self, from_dir, to_dir, lines_per_file):
        self.from_dir = from_dir
        self.to_dir = to_dir
        self.lines_per_file = lines_per_file

    def split_compressed_files(self):
        utils.safe_create_path(self.to_dir)
        files_to_split = glob(os.path.join(self.from_dir, '*.json.gz'))
        logging.info('Files to split: ' + str(len(files_to_split)))

        parts_count = 0
        for json_file in files_to_split:
            parts_count += self._split_json_file(json_file)

        logging.info('File wrote: ' + str(parts_count))
        return parts_count

    def _split_json_file(self, json_file_path):
        part_count = 0
        with so.smart_open(json_file_path) as fin:
            read_next = True
            while read_next:
                file_date_str = self._extract_file_date_str(json_file_path)
                new_file_path = os.path.join(self.to_dir, '.'.join([file_date_str, str(part_count), 'json.gz']))
                json_lines_list = map(lambda x: fin.readline(), range(self.lines_per_file))
                read_next = self._write_json_file(new_file_path, json_lines_list)
                part_count += 1
            fin.close()
        return part_count

    @staticmethod
    def _write_json_file(new_file_path, json_lines_list):
        with so.smart_open(new_file_path, 'wb') as fout:
            [fout.write(ele) for ele in json_lines_list if ele != '']
            fout.close()
            logging.info('Finished writing: ' + new_file_path)
            if json_lines_list[-1] == '':
                return False
            del json_lines_list
            return True

    @staticmethod
    def _extract_file_date_str(stocktwits_file_path):
        return '_'.join(stocktwits_file_path.split('_')[-2:]).split('.')[0]
