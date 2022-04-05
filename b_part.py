#!/usr/bin/env python3
"""
Create a bar plot showing the number of transactions occurring every month between the start and end
of the dataset. Create a bar plot showing the average value of transactions in each month between 
the start and end of the dataset. Note: As the dataset spans multiple years and you are aggregating
together all transactions in the same month, make sure to include the year in your analysis.

Note: Once the raw results have been processed within Hadoop/Spark you may create your bar plot in 
any software of your choice (excel, python, R, etc.)
"""
from mrjob.job import MRJob


class part_b(MRJob):

    def mapper(self, _, line):

        fields = line.split(",")

        try:
            if len(fields) == 5:
                yield (fields[0], None)

        except:
            pass


if __name__ == '__main__':
    part_b.run()
