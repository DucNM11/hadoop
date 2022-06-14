#!/usr/bin/env python3
"""Popular Scams: Utilising the provided scam dataset, what is the most lucrative
form of scam? Does this correlate with certainly known scams going offline/
inactive? For the correlation, you could produce the count of how many scams
for each category are active/inactive/offline/online/etc and try to correlate
it with volume (value) to make conclusions on whether state plays a factor in
making some scams more lucrative. Therefore, getting the volume and state of
each scam, you can make a conclusion whether the most lucrative ones are ones
that are online or offline or active or inactive. So for that purpose, you
need to just produce a table with SCAM TYPE, STATE, VOLUME which would be enough."""

# Import necessary package
from mrjob.job import MRJob
from mrjob.step import MRStep


class scam_report(MRJob):
    """Two seps:
1. Join data between transactions and processed scams.txt data set.
2. Change aggregation dimension from address to status and category, aggregate
sum transactions by the new dimension.

Input:
- hdfs data/ethereum/transactions dataset (sep=',')
    fields[0]: block_number
    fields[1]: from_address
    fields[2]: to_address
    fields[3]: value
    fields[4]: gas
    fields[5]: gas_price 
    fields[6]: block_timestamp
- hdfs user/mdn01/scams.txt (sep=',')
    fields[0]: address
    fields[1]: status
    fields[2]: category
Output:
- d_part_scam_rs.txt (sep='\t')
    fields[0]: (category, status)
    fields[2]: sum transaction volume
    """

    def mapper(self, _, line):
        """Add symbol value to differentiate between two data sets"""
        try:
            fields = line.split(',')
            if len(fields) == 3:
                symbol = 'S'
                yield (fields[0], [symbol, fields[1], fields[2], None])
            else:
                symbol = 'T'
                yield (fields[2], [symbol, None, None, int(fields[3])])
        except:
            pass

    def reducer_change_dim(self, key, values):
        """Filter to get scam addresses only, change the dimension from addresses
        to (category, status), aggregate transaction volume accordingly"""

        # Since values of MRJob is a generator, we change it to a list for multiple
        # consumptions using list comprehensions.
        # Statistically speaking, list comprehension has around 5 times faster
        # than for loop, we use list comprehension.
        list_val = list(values)

        # Indexing for scam type in the values list
        scam_type_idx_list = [i for i, x in enumerate(list_val) if x[0] == 'S']

        # Only proceed with transaction of scam address to save computing resources
        if len(scam_type_idx_list) > 0:
            # Total transaction number
            txn_number = sum(1 for value in list_val if value[0] == 'T')

            # Only proceed if there is scam transaction
            if txn_number != 0:

                # Get total transaction volume
                total_txn_vol = sum(value[3] for value in list_val
                                    if value[0] == 'T')

                # Yield type of scame along with sum, count metrics
                for scam_type_idx in scam_type_idx_list:
                    yield ((list_val[scam_type_idx][2],
                            list_val[scam_type_idx][1]), ('sum',
                                                          total_txn_vol))
                    yield ((list_val[scam_type_idx][2],
                            list_val[scam_type_idx][1]), ('count', txn_number))

    def reducer(self, key, values):
        """Aggregate to three metrics sum, count, average for each category & status"""
        list_val = list(values)

        total_txn_vol = sum(value[1] for value in list_val
                            if value[0] == 'sum')
        total_txn_number = sum(value[1] for value in list_val
                               if value[0] == 'count')

        yield ((key, 'sum'), total_txn_vol)
        yield ((key, 'count'), total_txn_number)
        yield ((key, 'avg'), f'{total_txn_vol/total_txn_number:.3f}')

    def steps(self):

        return [
            MRStep(mapper=self.mapper, reducer=self.reducer_change_dim),
            MRStep(reducer=self.reducer)
        ]


if __name__ == '__main__':
    scam_report.run()
