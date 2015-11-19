#!/usr/bin/env python

import argparse
import datetime
import json
import logging
import subprocess
import sys
import time

try:
    from subprocess import DEVNULL #python3
except ImportError:
    import os
    DEVNULL = open(os.devnull, 'r+b')

logging.basicConfig(format='%(message)s', stream=sys.stdout)
LOG = logging.getLogger(__name__)


def cli_parser():
    parser = argparse.ArgumentParser(description='Ceph Scrub Manager')
    parser.add_argument('-d', '--daemon',
                        help='Run in daemon mode',
                        action='store_true')
    parser.add_argument('--daemon-log',
                        help='Specify log file rather than syslog',
                        type=str)
    parser.add_argument('--debug',
                        help='Turn on debug mode',
                        action='store_const',
                        dest='loglevel',
                        const=logging.DEBUG,
                        default=logging.WARNING)
    parser.add_argument('--deep-scrub-interval',
                        help='Interval in days since last pg deep-scrub',
                        default=7,
                        type=int,
                        dest='ds_interval')
    parser.add_argument('--scrub-interval',
                        help='Interval in days since last pg scrub',
                        default=3,
                        type=int,
                        dest='s_interval')
    parser.add_argument('-s', '--status',
                        help='Returns status information about PGs',
                        action='store_true')
    parser.add_argument('-p', '--parallel',
                        help='Maximum number of unhealthy PGs',
                        type=int,
                        default=8)
    parser.add_argument('-v', '--verbose',
                        help='Add logging verbosity',
                        action='store_const',
                        dest='loglevel',
                        const=logging.INFO)
    return parser.parse_args()


class CephScrubManager():

    def __init__(self, config):
        self.config = config

    def state_check(self):
        count = 0
        for pg in json.loads(self.dump())['pg_stats']:
            if pg['state'] != 'active+clean':
                count = count + 1
        return count

    def date_check(self, pg):
        fmt="%Y-%m-%d %H:%M:%S.%f"
        ds_days = self.config.ds_interval
        s_days = self.config.s_interval

        ds_comp = datetime.datetime.now() - datetime.timedelta(days=ds_days)
        s_comp = datetime.datetime.now() - datetime.timedelta(days=s_days)
        ds_time = datetime.datetime.strptime(pg['last_deep_scrub_stamp'], fmt)
        s_time = datetime.datetime.strptime(pg['last_scrub_stamp'], fmt)

        deep_scrub = ds_time > ds_comp
        scrub = s_time > s_comp
        return deep_scrub, scrub

    def dump(self):
        p = subprocess.Popen(["ceph", "pg", "dump", "--format=json"],
                             stdout=subprocess.PIPE,
                             stderr=DEVNULL)
        return p.communicate()[0].decode('UTF-8')

    def sorted_dump(self):
        osds = dict()
        pgs = dict()
        raw = json.loads(self.dump())

        for pg in raw['pg_stats']:
            pgs.update({pg['pgid']: {}})
            for osd in pg['acting']:
                try:
                    osds[osd].append(pg['pgid'])
                except KeyError:
                    osds[osd] = [pg['pgid']]

                pgs[pg['pgid']].update({osd: osds[osd]})

        return pgs, osds

    def deep_scrub(self):
        for pg in json.loads(self.dump())['pg_stats']:
            deep_scrub, scrub = self.date_check(pg)
            if deep_scrub:
                # pg has been scrubed in the specified timeframe
                continue

            while self.state_check() > self.config.parallel:
                LOG.info('sleeping 30 seconds')
                time.sleep(30)

            LOG.warn('Performing deep-scrubbing on PG {}'.format(pg['pgid']))
            p = subprocess.Popen(["ceph", "pg", "deep-scrub", pg['pgid']],
                                 stdout=subprocess.PIPE,
                                 stderr=DEVNULL)
            LOG.info('{}'.format(p.communicate()[0].decode('UTF-8')))
            time.sleep(15)

    def status(self):
        ds_count = 0
        dse_count = 0
        s_count = 0
        se_count = 0

        for pg in json.loads(self.dump())['pg_stats']:
            deep_scrub, scrub = self.date_check(pg)

            if not scrub:
                s_count = s_count + 1
                LOG.info('{} has not been scrubbed since {}'.format(
                    pg['pgid'], pg['last_scrub_stamp']
                ))
            if not deep_scrub:
                ds_count = ds_count + 1
                LOG.info('{} has not been deep-scrubbed since {}'.format(
                    pg['pgid'], pg['last_deep_scrub_stamp']
                ))
            if pg['stat_sum']['num_scrub_errors'] > 0:
                se_count = se_count + 1
                LOG.info('{} has {} scrub error(s)'.format(
                    pg['pgid'], pg['stat_sum']['num_scrub_errors']
                ))
            if pg['stat_sum']['num_deep_scrub_errors'] > 0:
                dse_count = dse_count + 1
                LOG.info('{} has {} deep-scrub error(s)'.format(
                    pg['pgid'], pg['stat_sum']['num_deep_scrub_errors']
                ))

        LOG.warn('Number of PGs the need scrubbing: {}'.format(s_count))
        LOG.warn('Number of PGs the need deep-scrubbing: {}'.format(ds_count))
        LOG.warn('Number of PGs scrubbing errors: {}'.format(se_count))
        LOG.warn('Number of PGs deep-scrubbing errors: {}'.format(dse_count))


def main():
    config = cli_parser()
    LOG.setLevel(config.loglevel)

    csm = CephScrubManager(config)

    if config.status:
        return csm.status()

    csm.deep_scrub()


if __name__ == '__main__':
    sys.exit(main())
