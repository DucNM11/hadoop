#!/usr/bin/env python3

# Import neccessary packages
import matplotlib.pyplot as plt
import pandas as pd

# Import data into pandas object
df = pd.read_csv('a_out.txt', sep=",", names=['date', 'value', 'tnx'])

# Process df data types
df['value'] = df['value'].astype('float')
df['date'] = df['date'].astype('datetime64')
df = df.set_index('date')
df = df.sort_index()

# Define title and x/y labels for the first chart for number of transactions per month
plt.title('Number of transactions per month\n', fontsize=20, fontweight='bold')
plt.xlabel('Year-Month', fontsize=16)
plt.ylabel('Number of transactions', fontsize=16)

# Plot bar chart and show
plt.bar(df.index, df.tnx, width=10)
plt.show()

# Define title and x/y labels for the second chart for average value of transactions per month
plt.title('Average value of transactions per month\n',
          fontsize=20,
          fontweight='bold')
plt.xlabel('Year-Month', fontsize=16)
plt.ylabel('Average value of transactions', fontsize=16)

# Plot bar chart and show
plt.bar(df.index, df.value / df.tnx, width=10)
plt.show()
