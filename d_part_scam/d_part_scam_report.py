#!/usr/bin/env python3
# Import needed packages
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Declare an empty dataframe
df = pd.DataFrame({}, columns=['scam_type', 'status', 'metric', 'volume'])

# Load data to the above dataframe from the Spark query's output file
with open('d_part_scam_data_rs.txt', 'r') as f:
    lines = f.readlines()
    for line in lines:
        fields = line.replace(' ',
                              '').replace('\n', '').replace('\"', '').replace(
                                  '[', '').replace(']', '').split(',')
        df.loc[len(df)] = [fields[0], fields[1], fields[2], fields[3]]

# Pre-processing dataframe fields
df['volume'] = df['volume'].astype('float64')

print(df.loc[df.metric != 'avg'].groupby(['scam_type', 'metric']).sum())

# Plot a chart to further analyse the data
fig, ax = plt.subplots(nrows=2, ncols=1, figsize=(10, 8))

df_sum = df.loc[df.metric == 'sum']
sns.barplot(x='scam_type', y='volume', hue='status', data=df_sum, ax=ax[0])
ax[0].title.set_text('Transaction volume by scam type-status')
plt.setp(ax[0], ylabel='Transaction volume')

df_count = df.loc[df.metric == 'count']
sns.barplot(x='scam_type', y='volume', hue='status', data=df_count, ax=ax[1])
ax[1].title.set_text('Number of occurence by scam type-status')
plt.setp(ax[1], ylabel='Number of occurence')

# Tuning the chart layout
plt.tight_layout()

# Save the chart for reporting
plt.savefig('d_part_scam_report.png')
