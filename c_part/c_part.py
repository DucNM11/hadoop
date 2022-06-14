#!/usr/bin/env python3
"""Evaluate the top 10 miners by the size of the blocks mined. This is simpler
as it does not require a join. You will first have to aggregate blocks to see
how much each miner has been involved in. You will want to aggregate size for
addresses in the miner field. This will be similar to the wordcount that we
saw in Lab 1 and Lab 2. You can add each value from the reducer to a list and
then sort the list to obtain the most active miners."""
from mrjob.job import MRJob, MRStep


class top_amt(MRJob):

    def mapper(self, _, line):
        # Mapper to get needed fields as variables' names
        try:
            fields = line.split(',')
            if len(fields) == 9:
                miner_add = fields[2]
                size = int(fields[4])
                yield (miner_add, size)
        except:
            pass

    def combiner(self, key, values):
        # Combiner to optimize network traffic
        yield (key, sum(values))

    def reducer(self, key, values):
        # Reduncer to reshift the key to None for sorting in the next reducer step
        yield (None, (key, sum(values)))

    def reducer_top(self, key, values):
        # Reducer to get the top 10 largest values
        sorted_values = sorted(values, reverse=True, key=lambda tup: tup[2])
        i = 0

        for val in sorted_values:
            yield (val[0], val[1])
            i += 1
            if i >= 10:
                break

    def steps(self):
        # Define multi-steps job
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_top)
        ]


if __name__ == '__main__':
    top_amt.run()
