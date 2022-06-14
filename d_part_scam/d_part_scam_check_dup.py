#!/usr/bin/env python3
"""Quick job to check for addresses involves in several category of scam.
Since this could impact our final result aggregating by transaction volume
Input: hdfs user/mdn01/scams.txt (sep=',')
    fields[0]: address
    fields[1]: status
    fields[2]: category
Output: Local text file (sep='\t')
    fields[0]:  address as key for status aggregation
                address+'~' as key for category aggregation
    fields[1]:  count distinct types of status/category
"""
from mrjob.job import MRJob


class check(MRJob):

    def mapper(self, _, line):
        # Mapper to get the address (with ~ at the end for category value) as the key, and value is status/category
        try:
            fields = line.split(',')
            yield (fields[0], fields[1])
            yield (fields[0] + '~', fields[2])
        except:
            pass

    def reducer(self, key, val):
        # Reduce to distinct value of status/category
        yield (key, len(set(val)))


if __name__ == '__main__':
    check.run()
