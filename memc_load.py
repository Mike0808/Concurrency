#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import logging
import collections
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
from queue import Queue

import appsinstalled_pb2
# pip install python-memcached
import memcache
from threading import Thread, Lock as TLock
from contextlib import contextmanager
from multiprocessing import Process, Lock as MpLock, JoinableQueue as JQueue, cpu_count, Value

NORMAL_ERR_RATE = 0.01
CON_RETRY = 3
TIMEOUT = 1
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])
PROCESSES_NUM = 2
THREAD_NUM = 3

# https://stackoverflow.com/questions/19665235/memcache-client-with-connection-pool-for-python
memcache.Client = type('Client', (object,), dict(memcache.Client.__dict__))


class PoolClient(object):
    """Pool of memcache clients that has the same API as memcache.Client"""

    def __init__(self, pool_size, pool_timeout, *args, **kwargs):
        self.pool_timeout = pool_timeout
        self.queue = Queue()
        if "memc_addr" in args:
            self.memc_addr = args['memc_addr']
        for _i in range(pool_size):
            self.queue.put(memcache.Client(*args, **kwargs))

    @contextmanager
    def reserve(self):
        """ Reference: http://sendapatch.se/projects/pylibmc/pooling.html#pylibmc.ClientPool"""
        client = self.queue.get(timeout=self.pool_timeout)
        try:
            yield client
        finally:
            self.queue.put(client)


def dot_rename(path):
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def insert_appsinstalled(appsinstalled, memc_addr, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    if dry_run:
        logging.debug("%s - %s -> %s" % (memc_addr, key,
                                         str(ua).replace("\n", " ")))
        return key, ua
    else:
        return key, packed


def parse_appsinstalled(line):
    if isinstance(line, bytes):
        line = line.decode("utf-8")
    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return
    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return
    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info("Not all user apps are digits: `%s`" % line)
    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info("Invalid geo coords: `%s`" % line)
    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


class Processor_files(Process):

    def __init__(self, files, errors, jqueue=None, lock=None, lines=None):
        Process.__init__(self)
        self.files = files
        self.queue = jqueue
        self.lock = lock
        self.errors = errors
        self.lines = lines

    def run(self):
        try:
            for file in self.files:
                logging.info(f'Processing {file}')
                with self.lock, gzip.open(file) as fd:
                    for line in fd:
                        if not line:
                            continue
                        parsed_line = parse_appsinstalled(line)
                        if not parsed_line:
                            self.errors.value += 1
                            continue
                        self.queue.put(parsed_line)
                        if self.lines:
                            self.lines.value += 1
                    dot_rename(file)

        except Exception as E:
            logging.error(f"Something went wrong: {E}")


class Consumer_appsinstalled(Process):

    def __init__(self, options, task_queue, result_queue, lock, errors):
        super(Consumer_appsinstalled, self).__init__()
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.options = options
        self.lock = lock
        self.errors = errors
        self.device_memc = {
            "idfa": self.options.idfa,
            "gaid": self.options.gaid,
            "adid": self.options.adid,
            "dvid": self.options.dvid,
        }

    def run(self):
        pname = self.name
        while True:
            try:
                logging.info(f'Processing appsinstalled data')
                item_appinstalled = self.task_queue.get()
                if item_appinstalled is None:
                    logging.error(f'Exiting {pname}. Queue Empty')
                    self.task_queue.task_done()
                    break
                memc_addr = self.device_memc.get(item_appinstalled.dev_type)
                if not memc_addr:
                    with self.lock:
                        self.errors.value += 1
                        logging.error("Unknow device type: %s" % item_appinstalled.dev_type)
                        continue
                item_for_memcache = insert_appsinstalled(item_appinstalled, memc_addr, self.options.dry)
                self.task_queue.task_done()
                self.result_queue.put((item_for_memcache, memc_addr))
            except Exception as E:
                logging.error(f"Something went wrong: {E}")


class Memcache_Filler(Thread):

    def __init__(self, queue, lock, errors, processed):
        Thread.__init__(self)
        self.memc_addr = None
        self.queue = queue
        self.lock = lock
        self.errors = errors
        self.processed = processed

    def run(self):
        tname = self.name
        while self.queue._notempty:
            item_for_memcache, memc_addr = self.queue.get()
            self.memc_addr = memc_addr
            if item_for_memcache is None or memc_addr is None:
                logging.error(f'Exiting {tname}. Queue Empty')
                self.queue.task_done()
                break
            try:
                with self.lock:
                    mc_client_pool = PoolClient(1, TIMEOUT, [memc_addr])
                    ok = self.do_work(item_for_memcache, mc_client_pool)
                    if not ok:
                        self.errors.value += 1
                    else:
                        self.processed.value += 1
            except Exception as e:
                logging.error(f"Something went wrong: {e}")

    def do_work(self, item_for_memcache, mc_client_pool):
        try:
            key, packed = item_for_memcache
            self.queue.task_done()
            with mc_client_pool.reserve() as mc_client:
                mc_client.set(key, packed)
        except Exception as e:
            logging.exception("Cannot write to memc %s: %s" % (self.memc_addr, e))
            return False
        return True


def build_processor_worker_pool_files(files, errors, jqueue, mlock, n_processors, lines=None):
    workers = []
    for n in range(n_processors):
        worker = Processor_files(files, errors, jqueue, mlock, lines)
        worker.start()
        workers.append(worker)
    return workers


def build_consumer_worker_pool_files(options, task_queue, result_queue, lock, errors, size=PROCESSES_NUM):
    workers = []
    for _ in range(size):
        worker = Consumer_appsinstalled(options, task_queue, result_queue, lock, errors)
        worker.start()
        workers.append(worker)
    return workers


def build_thread_worker_pool_consumer(queue, lock, errors, processed, size=THREAD_NUM):
    workers = []
    for _ in range(size):
        worker = Memcache_Filler(queue, lock, errors, processed)
        worker.start()
        workers.append(worker)
    return workers


def main(files, options):
    tlock = TLock()
    mlock = MpLock()
    mqueue = JQueue()
    jq = JQueue()
    n_processors = cpu_count()
    errors = Value('i')
    processed = Value('i')

    worker_process_files = build_processor_worker_pool_files(files, errors, jq,
                                                             mlock, n_processors)
    jq.join()
    worker_processor_consumer = build_consumer_worker_pool_files(opts, jq, mqueue, mlock, errors,
                                                                 n_processors)
    mqueue.join()
    worker_memcache_filler = build_thread_worker_pool_consumer(mqueue, tlock, errors, processed)

    for process in worker_process_files:
        process.join()

    for _ in worker_process_files:
        jq.put(None)

    for process in worker_processor_consumer:
        process.join()

    for _ in worker_memcache_filler:
        mqueue.put(None)

    for thread in worker_memcache_filler:
        thread.join()

    err_rate = float(errors.value) / processed.value
    if err_rate < NORMAL_ERR_RATE:
        logging.info("Acceptable error rate (%s). Successfull load" % err_rate)
    else:
        logging.error("High error rate (%s > %s). Failed load" % (err_rate, NORMAL_ERR_RATE))


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="./data/appsinstalled/*.tsv.gz")
    op.add_option("--idfa", action="store", default="127.0.0.1:33013")
    op.add_option("--gaid", action="store", default="127.0.0.1:33014")
    op.add_option("--adid", action="store", default="127.0.0.1:33015")
    op.add_option("--dvid", action="store", default="127.0.0.1:33016")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info("Memc loader started with options: %s" % opts)
    try:
        main(opts)
    except Exception as e:
        logging.exception("Unexpected error: %s" % e)
        sys.exit(1)
