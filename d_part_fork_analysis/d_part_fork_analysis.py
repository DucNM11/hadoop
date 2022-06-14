#!/usr/bin/env python3
"""
There have been several forks of Ethereum in the past. Identify one or more of these and see
what effect it had on price and general usage. For example, did a price surge/plummet occur,
and who profited most from this?
"""

# Save resources by importing the needed module only
from pyspark import SparkContext
from time import gmtime, strftime

sc = SparkContext()


def is_good_line(line):
    # Function to check the data quality of a line
    try:
        fields = line.split(',')

        # Saving computing resources by doing len once
        len_fields = len(fields)
        if len_fields not in [2, 7]:
            return False
        elif len_fields == 7:
            if int(fields[3]) == 0 or int(fields[6]) == 0:
                return False
        return True
    except:
        return False


def txn_map(line):
    # Function to map a line from transactions data set to each fork event
    to_date = lambda timestamp: strftime('%d/%m/%Y', gmtime(timestamp))

    fields = line.split(',')
    block = int(fields[0])

    # Conditions threshold from Wikipedia (Ethereum)
    if 0 <= block < 200000:
        fork_event = 'Frontier'
    elif 200000 <= block < 1150000:
        fork_event = 'Ice Age'
    elif 1150000 <= block < 1920000:
        fork_event = 'Homestead'
    elif 1920000 <= block < 2463000:
        fork_event = 'DAO Fork'
    elif 2463000 <= block < 2675000:
        fork_event = 'Tangerine Whistle'
    elif 2675000 <= block < 4370000:
        fork_event = 'Spurious Dragon'
    elif 4370000 <= block < 7280000:
        fork_event = 'Byzantium'
    elif block >= 7280000:
        fork_event = 'Constantinople/St. Petersburg'

    date = to_date(int(fields[6]))
    txn_value = int(fields[3])

    return ((fork_event, date), txn_value)


def txn_reshift(field):
    # Reshift for aggregation
    date = field[0][1]
    fork_event = field[0][0]
    txn_value = field[1]

    return (date, (fork_event, txn_value))


def txn_reshift_2(field):
    # Reshift for aggregation
    date = field[0]
    fork_event = field[1][0][0]
    txn_value = field[1][0][1]
    eth_price = field[1][1]

    return ((fork_event, date), (eth_price, txn_value))


def eth_map(line):
    # Map function for ethereum price (a data set scraped from Coinmarketcap)
    fields = line.split(',')
    price = float(fields[1])

    return (fields[0], price)


# Import 2 input files
txn = sc.textFile('/data/ethereum/transactions').filter(is_good_line)
eth_price = sc.textFile('/user/mdn01/eth_price.txt').filter(is_good_line)

# Define an add lambda for ease of use
add = lambda x, y: x + y

# Map steps
txn = txn.map(txn_map)

eth_price = eth_price.map(eth_map)
txn = txn.reduceByKey(add)
# Reshift to join with eth price by the date field
txn = txn.map(txn_reshift)

# Join to the daily price data set to analyse later
txn = txn.leftOuterJoin(eth_price)
eth_price.unpersist()

# Reshift index for ease of use
txn = txn.map(txn_reshift_2)
for record in txn.collect():
    print(record)
