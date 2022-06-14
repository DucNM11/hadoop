#!/usr/bin/env python3
"""Reimplement Part B in Spark (if your original was MRJob, or vice versa).
How does it run in comparison? Keep in mind that to get representative
results you will have to run the job multiple times, and report
median/average results. Can you explain the reason for these results?
What framework seems more appropriate for this task?"""

# Save resources by importing the needed module only
from pyspark import SparkContext
from time import time

sc = SparkContext()


def is_good_line(line):
    try:
        fields = line.split(',')

        # Saving computing resources by doing len once
        len_fields = len(fields)
        if len_fields not in [5, 7]:
            return False
        elif len_fields == 7:
            if int(fields[3]) == 0:
                return False
        return True
    except:
        return False


timer_start = time()

# Mapper step for each data set
contracts = sc.textFile('/data/ethereum/contracts/000000000000').filter(
    is_good_line)
transactions = sc.textFile(
    '/data/ethereum/transactions/part-01088-5ef0a9bd-d53d-4fd8-9a96-e33e04c37eed-c000.csv'
).filter(is_good_line)

contracts_map = contracts.map(lambda line: (line.split(',')[0], None))
transactions_map = transactions.map(lambda line:
                                    (line.split(',')[2], line.split(',')[3]))

# Join between data sets to get smart contracts
join = contracts_map.join(transactions_map)
mapper_2 = join.map(lambda x: (x[0], int(x[1][1])))

# Recude to get total transaction value by the key value
reducer = mapper_2.reduceByKey(lambda x, y: x + y)

# Get the top 10 largest total transactions for smart contracts
top = reducer.takeOrdered(10, key=lambda x: -x[1])

# Get the result
for record in top:
    print(record)

# Print the runtime for benchmarking
timer_stop = time()
print('Elapsed time: {}'.format(timer_stop - timer_start))
