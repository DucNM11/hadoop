#!/usr/bin/env python3
"""
Gas Guzzlers: For any transaction on Ethereum a user must supply gas.

Break down the required metrics for each questions:
    How has gas price changed over time?
        Average gas price per transaction per month

    Have contracts become more complicated, requiring more gas, or less so?
        Average gas price per transaction of contract type address per month

    Also, could you correlate the complexity for some of the top-10 contracts
    found in Part-B by observing the change over their transactions?
        Average gas price per transaction of top contracts per month

Input:
- hdfs data/ethereum/transactions dataset (sep=',')
    fields[0]: block_number
    fields[1]: from_address
    fields[2]: to_address
    fields[3]: value
    fields[4]: gas
    fields[5]: gas_price 
    fields[6]: block_timestamp
- hdfs data/ethereum/contracts dataset (sep=',')
    fields[0]: contract address
    fields[1]: is ERC20
    fields[2]: is ERC721
    fields[3]: block number
- hdfs user/mdn01/top_contracts.txt (sep=',') output from part B
    fields[0]: address
    fields[1]: total transaction volume
Output:
- hdfs user/mdn01/avg_gas (sep=',')
    fields[0]: month/year
    fields[1]: average gas price per transaction
- hdfs user/mdn01/contract_avg_gas (sep=',')
    fields[0]: month/year
    fields[1]: average gas price per transaction of contract
- hdfs user/mdn01/top_contract_avg_gas (sep=',')
    fields[0]: month/year
    fields[1]: average gas price per transaction of top contract
"""

# Save resources by importing the needed module only
from pyspark import SparkContext
from datetime import datetime as dt

sc = SparkContext()


def is_good_line(line):

    try:
        fields = line.split(',')

        # Saving computing resources by doing len once
        len_fields = len(fields)
        if len_fields not in [2, 5, 7]:
            return False
        elif len_fields == 7:
            if int(fields[5]) == 0 or int(fields[6]) == 0:
                return False
        return True
    except:
        return False


def process_date(timestamp_str):
    """Convert unix timestamp string to month/year format"""

    timestamp = dt.fromtimestamp(int(timestamp_str))
    return '{}/{}'.format(timestamp.month, timestamp.year)


def process_contract_type(field):
    """Process contract type as a dimension to slice and dice when doing analysis"""

    # Since top contract is also a contract, so we check this condition first
    if field[1][1] == 2:
        return 'Top Contract'
    elif field[1][0][1] == 1:
        return 'Contract'
    else:
        return 'Wallet'


def cal_avg_gas(rdd_obj, dim=None):

    if dim is None:
        announce = 'All addresses'
        mapper_monthly_gas_price = rdd_obj.map(lambda x: (x[0][1],
                                                          (x[1][0], x[1][1])))
    elif dim == 'Contract':
        announce = 'All contracts'
        mapper_monthly_gas_price = rdd_obj.filter(lambda x: x[0][0][
            -8:] == 'Contract').map(lambda x: (x[0][1], (x[1][0], x[1][1])))
    elif dim == 'Top Contract':
        announce = 'Top contracts'
        mapper_monthly_gas_price = rdd_obj.filter(lambda x: x[0][
            0] == 'Top Contract').map(lambda x: (x[0][1], (x[1][0], x[1][1])))

    print('Done the mapper')
    print('Calculating monthly average for:', announce)

    reducer_monthly_gas_price = mapper_monthly_gas_price.reduceByKey(add)
    mapper_monthly_gas_price.unpersist()
    print('Done the reducer')

    monthly_avg_gas_price = reducer_monthly_gas_price.mapValues(
        lambda x: x[0] / x[1])
    reducer_monthly_gas_price.unpersist()
    print('Done the avg')

    monthly_avg_gas_price.saveAsTextFile('avg_gas_{}'.format(
        announce.replace(' ', '_')))
    print('Exported')


def map_transaction(line):
    fields = line.split(',')

    address = fields[2]
    date = process_date(fields[6])
    gas_price = int(fields[5])

    return ((address, date), (gas_price, 1))


def reshift_join(field):
    # Differentiate address type with function process_contract_type
    contract_type = process_contract_type(field)
    date = field[1][0][0][0]
    gas_price = field[1][0][0][1]
    count = field[1][0][0][2]

    return ((contract_type, date), (gas_price, count))


add = lambda x, y: (x[0] + y[0], x[1] + y[1])

print("Start")
# Import 3 input files
transactions = sc.textFile('/data/ethereum/transactions').filter(is_good_line)
contracts = sc.textFile('/data/ethereum/contracts').filter(is_good_line)
top_contracts = sc.textFile('/user/mdn01/top_contracts.txt').filter(
    is_good_line)

# Mapping step to get the needed fields only
transactions_map = transactions.map(map_transaction)
# Reduce data volume by aggregating sum gas_price and count before joining
transactions_reduce = transactions_map.reduceByKey(add)
# Reshift the key value after reduce data volume
transactions_map = transactions_reduce.map(
    lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1])))

contracts_map = contracts.map(lambda line: (line.split(',')[0], 1))
top_contracts_map = top_contracts.map(lambda line: (line.split(',')[0], 2))

# Free up the used RDDs for optimal RAM usage
transactions_reduce.unpersist()
transactions.unpersist()
contracts.unpersist()
top_contracts.unpersist()
print("Mapped, freed the RAM")

# Using left outer join to differentiate address type
join = transactions_map.leftOuterJoin(contracts_map)
join = join.leftOuterJoin(top_contracts_map)

# Free up the used RDDs for optimal RAM usage
transactions_map.unpersist()
contracts_map.unpersist()
top_contracts_map.unpersist()

# Reshift key value for aggregation
join = join.map(reshift_join)
join = join.reduceByKey(add)
for record in join.take(10):
    print(record)

# Calculate total gas price and its number of occurrences by each wallet type for further analysis
cal_avg_gas(join)
cal_avg_gas(join, dim='Contract')
cal_avg_gas(join, dim='Top Contract')