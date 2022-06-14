#!/usr/bin/env python3
"""
Create a bar plot showing the number of transactions occurring every month between the start and end
of the dataset. Create a bar plot showing the average value of transactions in each month between 
the start and end of the dataset. Note: As the dataset spans multiple years and you are aggregating
together all transactions in the same month, make sure to include the year in your analysis.

Note: Once the raw results have been processed within Hadoop/Spark you may create your bar plot in 
any software of your choice (excel, python, R, etc.)

JOB ID: http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_5371/
"""
from mrjob.job import MRJob
from datetime import datetime as dt


class part_a(MRJob):

    def mapper(self, _, line):
        # Map and filter lines
        fields = line.split(",")

        try:
            if len(fields) == 7:
                timestamp = dt.fromtimestamp(int(fields[6]))
                mth_yr = f"{timestamp.month}/{timestamp.year}"
                yield (mth_yr, (int(fields[3]), 1))
        except:
            pass

    def combiner(self, key, values):
        # Combiner to optimize network traffic
        count = 0
        total = 0

        for val in values:
            total += val[0]
            count += val[1]
        yield (key, (total, count))

    def reducer(self, key, values):
        # Reducer aggregates total and count to calculate average
        count = 0
        total = 0

        for val in values:
            total += val[0]
            count += val[1]
        yield (key, (total, count))


if __name__ == '__main__':
    part_a.run()
