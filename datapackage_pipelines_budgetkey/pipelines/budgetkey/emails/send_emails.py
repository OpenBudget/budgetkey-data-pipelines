from datapackage_pipelines.wrapper import process

import logging
import requests


def process_row(row, *_):
    logging.info('ROW: %r', row)

if __name__ == '__main__':
    process(process_row=process_row)


