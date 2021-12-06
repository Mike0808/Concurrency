import collections
import glob
import os
import unittest
from queue import Queue

import memc_load


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        pass

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

    def test_do_work(self):
        q = Queue()

        Opts = collections.namedtuple("Opts", ["idfa", "gaid", "adid", "dvid", "pattern", "dry"])
        opts = Opts("127.0.0.1:33013",
                    "127.0.0.1:33014",
                    "127.0.0.1:33015",
                    "127.0.0.1:33016",
                    "test/*.tsv.gz",
                    False)
        dirglob = "test/" + glob.escape(".") + "*.tsv.gz"
        for it in glob.iglob(dirglob):
            fnit = it.split(".")
            fn = "test/" + fnit[-3] + ".tsv.gz"
            os.rename(it, fn)
        for _ in glob.iglob(opts.pattern):
            t = memc_load.Proccess_File(q, opts)
            t.daemon = True
            t.start()

        for fn in glob.iglob(opts.pattern):
            q.put(fn)
        q.join()

        lenfn = len([it for it in glob.iglob(dirglob)])
        self.assertEqual(3, lenfn)


if __name__ == '__main__':
    unittest.main()
