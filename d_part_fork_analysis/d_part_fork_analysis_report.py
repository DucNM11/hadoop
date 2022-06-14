#!/usr/bin/env python3
# Import needed packages
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Declare an empty dataframe
df = pd.DataFrame({}, columns=['event', 'date', 'price', 'txn_amt'])

# Read data from output file to the above dataframe to analyse
with open('d_part_fork_analysis.txt', 'r') as f:
    lines = f.readlines()

    for line in lines:
        fields = line.replace('u', '')\
                     .replace('\'', '')\
                     .replace('(', '')\
                     .replace(')', '')\
                     .replace('\n', '')\
                     .replace(' ', '')\
                     .replace('L','').split(',')
        df.loc[len(df)] = [fields[0], fields[1], fields[2], fields[3]]

df[['price', 'txn_amt']] = df[['price', 'txn_amt']].astype('float64')
df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

df = df.sort_values('date')

# If the fork happened within the day, there would be duplicated dates with two different fork event
# The code below add txn_amout of the fork date to the next day for consitency before deleting duplicates
index = df.index
list_dup = index[(df.date == df.date.shift(1))
                 & (df.event != df.event.shift(1))]
df.loc[list_dup + 1,
       'txn_amt'] = df.loc[list_dup + 1,
                           'txn_amt'].values + df.loc[list_dup,
                                                      'txn_amt'].values
# Remove duplicated date rows
df = df.loc[~df.date.duplicated(), :]

# Calculating features
df['avg_price_l2w'] = df['price'].rolling(14).mean()
df['avg_price_n2w'] = df['avg_price_l2w'].shift(-14)
df['total_txn_amt_l2w'] = df['txn_amt'].rolling(14).sum()
df['total_txn_amt_n2w'] = df['total_txn_amt_l2w'].shift(-14)

# Plot to analyse data
df['date_plot'] = df['date'].dt.strftime('%m/%Y')

fig, ax = plt.subplots(2, 1, figsize=(10, 8))
sns.lineplot(x='date_plot', y='price', data=df, hue='event', ax=ax[0])
sns.lineplot(x='date_plot', y='txn_amt', data=df, hue='event', ax=ax[1])
plt.tight_layout()
plt.savefig('d_part_fork_analysis_chart.png')

# Join data with each fork's date to get the key events
fork_date = pd.DataFrame(
    [['Ice Age', '8 September 2015'], ['Homestead', '15 March 2016'],
     ['DAO Fork', '20 July 2016'], ['Tangerine Whistle', '18 October 2016'],
     ['Spurious Dragon', '23 November 2016'], ['Byzantium', '16 October 2017'],
     ['Constantinople/St. Petersburg', '28 February 2019']],
    columns=['event', 'date'])

fork_date['date'] = pd.to_datetime(fork_date['date'], format='%d %B %Y')
data = pd.merge(df, fork_date, on='date')
data = data.rename(columns={'event_y': 'event'})

# Print data for reporting
print(data[[
    'date', 'event', 'avg_price_l2w', 'avg_price_n2w', 'total_txn_amt_l2w',
    'total_txn_amt_n2w'
]])
