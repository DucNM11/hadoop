"""
Create a bar plot showing the number of transactions occurring every month between the start and end
of the dataset. Create a bar plot showing the average value of transactions in each month between 
the start and end of the dataset. Note: As the dataset spans multiple years and you are aggregating
together all transactions in the same month, make sure to include the year in your analysis.

Note: Once the raw results have been processed within Hadoop/Spark you may create your bar plot in 
any software of your choice (excel, python, R, etc.)
"""
from mrjob.job import MRJob
from datetime import datetime as dt


class lab3(MRJob):
    def mapper(self, _, line):

        fields = line.split(",")

        try:
            if len(fields) == 7:
                timestamp = dt.fromtimestamp(int(fields[6]))
                mth_yr = f"{timestamp.month}/{timestamp.year}"
                yield(mth_yr, (int(fields[3]), 1))
        except:
            pass

    def combiner(self, key, values):
        count = 0
        total = 0
        for val in values:
            total += val[0]
            count += val[1]
        yield(key, (total, count))

    def reducer(self, key, values):
        count = 0
        total = 0
        for val in values:
            total += val[0]
            count += val[1]
        yield(key, (total, count))

# this class will define two additional methods: the mapper method goes here
# and the reducer method goes after this line
# this part of the python script tells to actually run the defined MapReduce job. Note that lab3 is the name of the class

if __name__ == '__main__':
    lab3.run()
