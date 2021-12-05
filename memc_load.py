#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import gzip
import sys
import glob
import logging
import collections
import time
from optparse import OptionParser
# brew install protobuf
# protoc  --python_out=. ./appsinstalled.proto
# pip install protobuf
import appsinstalled_pb2
# pip install python-memcached
import memcache
from threading import Thread, Lock
from queue import Queue
from contextlib import contextmanager

NORMAL_ERR_RATE = 0.01
CON_RETRY = 3
TIMEOUT = 1
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])

# https://stackoverflow.com/questions/19665235/memcache-client-with-connection-pool-for-python
memcache.Client = type('Client', (object,), dict(memcache.Client.__dict__))
#
#
# # Client.__init__ references local, so need to replace that, too
# class Local(object):
#     pass
#
#
# memcache.local = Local


class PoolClient(object):
    """Pool of memcache clients that has the same API as memcache.Client"""

    def __init__(self, pool_size, pool_timeout, *args, **kwargs):
        self.pool_timeout = pool_timeout
        self.queue = Queue()
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


def insert_appsinstalled(mc_client_pool, memc_addr, appsinstalled, dry_run=False):
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    connection = 0
    key = "%s:%s" % (appsinstalled.dev_type, appsinstalled.dev_id)
    ua.apps.extend(appsinstalled.apps)
    packed = ua.SerializeToString()
    # @TODO persistent connection
    # @TODO retry and timeouts!
    try:
        if dry_run:
            logging.debug("%s - %s -> %s" % (memc_addr, key, str(ua).replace("\n", " ")))
        else:
            with mc_client_pool.reserve() as mc_client:
                mc_client.set(key, packed)

    except Exception as e:
        logging.exception("Cannot write to memc %s: %s" % (memc_addr, e))
        return False
    return True


def parse_appsinstalled(line):
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


class Proccess_File(Thread):

    def __init__(self, queue, options):
        Thread.__init__(self)
        self.processed = self.errors = 0
        self.queue = queue
        self.options = options
        self.device_memc = {
            "idfa": options.idfa,
            "gaid": options.gaid,
            "adid": options.adid,
            "dvid": options.dvid,
        }

    def run(self):
        while True:
            item = self.queue.get()
            try:
                self.do_work(self.options, item)
            except Exception as e:
                logging.error(f"Something went wrong: {e}")
            finally:
                self.queue.task_done()

    def do_work(self, options, fn):
        logging.info(f'Processing {fn}')
        with gzip.open(fn) as fd:
            for line in fd:
                line = line.strip()
                if not line:
                    continue
                appsinstalled = parse_appsinstalled(line)
                if not appsinstalled:
                    self.errors += 1
                    continue
                memc_addr = self.device_memc.get(appsinstalled.dev_type)
                mc_client_pool = PoolClient(1, TIMEOUT, [memc_addr])
                if not memc_addr:
                    self.errors += 1
                    logging.error("Unknow device type: %s" % appsinstalled.dev_type)
                    continue
                ok = insert_appsinstalled(mc_client_pool, memc_addr, appsinstalled, options.dry)
                if not ok:
                    self.errors += 1
                else:
                    self.processed += 1
        err_rate = float(self.errors) / self.processed
        if err_rate < NORMAL_ERR_RATE:
            logging.info(f"Acceptable error rate ({err_rate}). Successfull load")
        else:
            logging.error(f"High error rate ({err_rate} > {NORMAL_ERR_RATE}). Failed load")
        dot_rename(fn)


def main(options):
    q = Queue()
    for _ in glob.iglob(options.pattern):
        t = Proccess_File(q, options)
        t.daemon = True
        t.start()

    for fn in glob.iglob(options.pattern):
        q.put(fn)

    q.join()


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
