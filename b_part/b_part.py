#!/usr/bin/env python3
"""Evaluate the top 10 smart contracts by total Ether received.
An outline of the subtasks required to extract this information is provided below,
focusing on a MRJob based approach. This is, however, is not the only way to
complete the task, as there are several other viable ways of completing this assignment.

JOB ID:
    http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_3685/
    http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_3786/"""
from mrjob.job import MRJob
from mrjob.step import MRStep
from time import time
import sys


class top_amt(MRJob):

    def mapper(self, _, line):
        # Mapper to differentiate between contracts and transactions data set
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
        # Reducer to aggregate total transactions, at the same time, reshifting key to None for sorting in the next reducer
        list_values = list(values)
        check = [True for value in list_values if value[0] == 'C']
        flag = any(check)

        if flag:
            total_value = 0
            for value in list_values:
                if value[0] == 'T':
                    total_value += value[1]

            if (total_value != 0) and (flag):
                yield (None, (key, total_value))

    def reducer_sort(self, key, values):
        # Reducer with sorted values to get top 10 largest values
        sorted_values = sorted(values, reverse=True, key=lambda tup: tup[1])

        i = 0
        for val in sorted_values:
            yield (val[0], val[1])
            i += 1
            if i >= 10:
                break

    def steps(self):
        # Define multi-steps job
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer_sort)
        ]


if __name__ == '__main__':
    timer_start = time()
    top_amt.run()
    sys.stderr.write(str(time() - timer_start))
