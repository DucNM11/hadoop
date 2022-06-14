#!/usr/bin/env python3
# Import needed packages
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Define a dict to get data from Spark query's output files
input_dict = {
    'All': 'd_part_gas_guzzler_avg_gas_all.txt',
    'All contracts': 'd_part_gas_guzzler_avg_gas_contracts.txt',
    'Top contracts': 'd_part_gas_guzzler_avg_gas_top_contracts.txt'
}

# Define an empty dataframe
df = pd.DataFrame({}, columns=['type', 'date', 'avg_gas_price'])

# Load data into the above dataframe
for idx, file in input_dict.items():
    with open(file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            fields = line.replace('\'', '').replace('(', '').replace(
                ')', '').replace('\n', '').split(',')
            df.loc[len(df)] = [idx, fields[0], fields[1]]

# Pre-processing the data
df['date'] = pd.to_datetime(df['date'], format='%m/%Y')
df['avg_gas_price'] = df.avg_gas_price.astype('float64')

df = df.sort_values('date')

# Plot a chart to analyse data
df['date'] = df.date.dt.strftime('%m/%Y')

fig = plt.figure(figsize=(10, 8))
sns.barplot(x='date', y='avg_gas_price', data=df, hue='type')
plt.xticks(rotation=70)

fig.suptitle('Average gas price analysis by address type',
             fontsize=20,
             fontweight='bold')
plt.xlabel('Month/Year', fontsize=18)
plt.ylabel('Average gas price per transactions', fontsize=16)

plt.tight_layout()

# Save figure for reporting
plt.savefig('d_part_gas_guzzler_chart.png')