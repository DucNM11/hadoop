#!/usr/bin/env python3
"""Evaluate the top 10 smart contracts by total Ether received.
An outline of the subtasks required to extract this information is provided below,
focusing on a MRJob based approach. This is, however, is not the only way to
complete the task, as there are several other viable ways of completing this assignment."""
from mrjob.job import MRJob


class top_amt(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            if len(fields) == 5:
                symbol = 'C'
                yield (fields[0], [symbol, None])
            else:
                symbol = 'T'
                yield (fields[2], [symbol, int(fields[3])])
        except:
            pass

    def reducer(self, key, values):
        contracts = []
        for value in values:
            if value[0] == 'C':
                contracts.append(key)

            if value[0] == 'T' & key in contracts:
                yield (key, sum(value[1]))

    # def reducer_sort(self, key, values):
    #     sorted_values = sorted(values, reverse=True, key=lambda tup: tup[1])
    #     i = 0

    #     for val in sorted_values:
    #         yield (val[0], val[1])
    #         i += 1
    #         if i >= 10:
    #             break


if __name__ == '__main__':
    top_amt.run()
