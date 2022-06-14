#!/usr/bin/env python3
"""Since the scam file size is small, process it locally would be
beneficial in term of computing resources than distributed processing
it in every node on hadoop/spark
Input: hdfs data/ethereum/scams.json
Output: scams.txt
    fields[0]: address
    fields[1]: status
    fields[2]: category
"""
import json

with open('scams_dep.json', 'r') as f:
    data = json.loads(f.read())

# Extract from key result only, since success key does not contain
# information we are looking for
data = data['result']

scam_list = []

for address in data.keys():
    # Map every related addresses to a scam address to their category
    # and status
    scam_list = scam_list + list(
        map(
            lambda e: [
                # Standardize scam data field since it is not standardized
                # (Scam/Scamming)
                e,
                data[address]['status'],
                'Scamming' if data[address]['category'] == 'Scam' else data[
                    address]['category']
            ],
            data[address]['addresses']))

# Return output to a file to put to hdfs
with open('scams.txt', 'w') as f:
    for line in scam_list:
        f.write(','.join(line))
        f.write('\n')