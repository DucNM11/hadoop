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
            if len(fields) == 9:
                date = fields[2]
                amt = float(fields[7]) * float(fields[6])
                symbol = fields[1]
                yield (None, (date, symbol, amt))
        except:
            pass

    def combiner(self, _, values):
        sorted_val = sorted(values, reverse=True, key=lambda tup: tup[2])
        i = 0

        for val in sorted_val:
            yield ("top", val)
            i += 1
            if i >= 10:
                break

    def reducer(self, _, values):
        sorted_values = sorted(values, reverse=True, key=lambda tup: tup[2])
        i = 0

        for value in sorted_values:
            yield (f"{value[0]} {value[1]} {value[2]}", None)
            i += 1
            if i >= 10:
                break


if __name__ == '__main__':
    top_amt.run()
