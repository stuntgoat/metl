#!/usr/bin/python

import errno
import os
import sys
import uuid
from collections import defaultdict
from datetime import datetime

DATA_DIR = './data'
DATETIME_METRICS = defaultdict(list)


def mkdir(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise


def make_bucket_directory(bucket, path=''):
    p = os.path.join(DATA_DIR, bucket, path)
    mkdir(p)
    return p


def time_sort_row(ts_str, metric):
    """
    `ts_str` is a string representation of a Unix timestamp.
    `data` is a metric string.

    Partition the metrics by hour within DATETIME_METRICS.

    """
    # NOTE: we send millisecond Unix timestamp but since we expect seconds only
    # we divide by 1000.0
    d = datetime.utcfromtimestamp(int(ts) / 1000.0)
    timestamp = datetime(d.year, d.month, d.day, d.hour).isoformat()
    # TEST: partision by minute.
    # timestamp = datetime(d.year, d.month, d.day, d.hour, d.minute).isoformat()

    DATETIME_METRICS[timestamp].append("%s %s" % (ts_str, metric))


def getrows():
    for line in sys.stdin.readlines():
        try:
            sline = line.strip().split()
            yield (sline[0], ''.join(sline[1:]))
        except Exception as e:
            sys.stderr.write(str(e))
            continue
    sys.stderr.flush()


def filename_from_path(path):
    return os.path.join(path, str(uuid.uuid4()) + '.txt')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        raise Exception("did not recieve the correct number of arguments")
    bucket = sys.argv[1]

    for ts, metric in getrows():
        time_sort_row(ts, metric)

    for dt_chunk, metrics in DATETIME_METRICS.iteritems():
        p = make_bucket_directory(bucket, dt_chunk)

        # TODO: gzip and ship to data warehouse, S3, etc.
        with open(filename_from_path(p), 'wb') as f:
            f.writelines('\n'.join(sorted(metrics)))
