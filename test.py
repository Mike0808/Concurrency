import collections
import functools
import glob
import gzip
import os
import threading
import unittest
from queue import Queue
from threading import Lock as TLock
from multiprocessing import Queue as MpQueue, Lock as MpLock, JoinableQueue as MpJQueue, cpu_count, Value

import memc_load

Opts = collections.namedtuple("Opts", ["idfa", "gaid", "adid", "dvid", "pattern", "dry"])
opts = Opts("127.0.0.1:33013",
            "127.0.0.1:33014",
            "127.0.0.1:33015",
            "127.0.0.1:33016",
            "test/*.tsv.gz",
            False)


def cases(cases):
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args):
            for c in cases:
                new_args = args + (c if isinstance(c, tuple) else (c,))
                try:
                    f(*new_args)
                except Exception as e:
                    print(f'ERROR OCCURRED: args {new_args} -- with exception {e}')

        return wrapper

    return decorator


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        dirglob = "test/" + glob.escape(".") + "*.tsv.gz"
        for it in glob.iglob(dirglob):
            fnit = it.split(".")
            fn = "test/" + fnit[-3] + ".tsv.gz"
            os.rename(it, fn)

    def tearDown(self) -> None:
        pass

    def test_parse_file(self):
        sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
        for line in sample.splitlines():
            df = memc_load.parse_appsinstalled(line)
            dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
            apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
            lat, lon = float(lat), float(lon)
            af = memc_load.AppsInstalled(dev_type, dev_id, lat, lon, apps)
            self.assertEqual(af, df)

    @cases([
        ['test/20170929000000.tsv.gz', 'test/20170929000100.tsv.gz', 'test/20170929000200.tsv.gz']
    ])
    def test_thread_open_file(self, files):
        tlock = TLock()
        mlock = MpLock()
        mqueue = MpJQueue()
        jq = MpJQueue()
        n_processors = cpu_count()
        errors = Value('i')
        processed = Value('i')
        lines = Value('i')

        worker_process_files = memc_load.build_processor_worker_pool_files(files, errors, jq,
                                                                           mlock, n_processors, lines)
        jq.join()
        worker_processor_consumer = memc_load.build_consumer_worker_pool_files(opts, jq, mqueue, mlock, errors,
                                                                               n_processors)
        mqueue.join()
        worker_memcache_filler = memc_load.build_thread_worker_pool_consumer(mqueue, tlock, errors, processed)

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
        dirglob = "test/" + glob.escape(".") + "*.tsv.gz"
        self.assertTrue(err_rate < memc_load.NORMAL_ERR_RATE)
        lenfn = len([it for it in glob.iglob(dirglob)])
        self.assertEqual(3, lenfn)
        self.assertEqual(processed.value, lines.value)


if __name__ == '__main__':
    unittest.main()
