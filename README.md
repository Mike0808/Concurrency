MemcLoad

This is script for concurrency memchaching lines from log tracker files in tsv.gz archive.
After, then lines will be processed, files renaming with dot prefix in name of file.

Parameters for run script:
    "-t", "--test", default=False - for simple protobuf test without memcache
    "-l", "--log", default=None  - log file name with path
    "--dry", default=False - produce parsed line to stdout without memcached and protobuf serializing
    "--pattern", default="./data/appsinstalled/*.tsv.gz" - path for tsv.gz files
    "--idfa", default="127.0.0.1:33013" - idfa memcached ipaddress with port
    "--gaid", default="127.0.0.1:33014" - gaid memcached ipaddress with port
    "--adid", default="127.0.0.1:33015" - adid memcached ipaddress with port
    "--dvid", default="127.0.0.1:33016" - dvid memcached ipaddress with port

MemcLoad run example:

python memc_load.py - run with default settings with process logging to stdout
