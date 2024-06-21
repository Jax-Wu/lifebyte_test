# COCLUSION
# COMMENT 1: Duplicated records exist in users table. 1000 records remain 666 after dropping duplicates.
# COMMENT 2: In users table, server == '54203B42716FE7C40138AE6C4913EBBC', only contains 3 users, which may be an issue.
# COMMENT 3: Like what was discovered in users table, in trades table, server == '54203B42716FE7C40138AE6C4913EBBC', only contains 571 records, which may be an issue.
# COMMENT 4: In trades table, there exist 5026 users that dont't belong to users table.
# COMMENT 5: In trades table, one abnormal record with volume==0 exists.
# COMMENT 6: In trades table, serveral records with open_time == close_time exist, which is abnormal.
# COMMENT 7: A number of records' date difference between close_time and open_time are very large, more than 365 days, which may be an issue.


import psycopg2
import pandas as pd

db_config = {
    'user': '*',
    'password': '*',
    'host': '*',
    'port': '*',
    'database': '*'
}



query = "SELECT * FROM users"
users = pd.read_sql(query, connection)
users.head()

len(users)


users[(users.login_hash=='18D4C2E739573770F9DF198F0E51C1B9') & (users.server_hash=='3D1F7E00251C43107EF39F55300781DB')]

# any duplicates of login_hash
# 666 < original length 1000, meaning that duplicates exist in users table
print(len(users.drop_duplicates()))


# login_hash belongs to server_hash
# users_distinct[['login_hash', 'server_hash']].value_counts().reset_index(name='count').count().unique()
unique_counts = users.groupby('login_hash')['server_hash'].nunique()
# check if nunique > 1
violations = unique_counts[unique_counts > 1]
print(violations)

# login_hash belongs to country_hash
unique_counts = users.groupby('login_hash')['country_hash'].nunique()
# check if nunique > 1
violations = unique_counts[unique_counts > 1]
print(violations)


# length of each type of hash

users['login_hash_len'] = users['login_hash'].apply(lambda x:len(x))
users['server_hash_len'] = users['server_hash'].apply(lambda x:len(x))
users['country_hash_len'] = users['country_hash'].apply(lambda x:len(x))
print(users[['login_hash_len', 'server_hash_len', 'country_hash_len']].nunique())



users_distinct = users.drop_duplicates()
# the distribution of server_hash
print(users_distinct['login_hash'].nunique())

# COMMENT: server == '54203B42716FE7C40138AE6C4913EBBC', only contains 3 users, which may be an issue
print(users_distinct.groupby('server_hash').count().reset_index()[['server_hash', 'login_hash']])
print(users_distinct.groupby('country_hash').count().reset_index()[['country_hash', 'login_hash']])



# # hash should be made of alphametic letters and numbers
pattern = r'[^a-zA-Z0-9]'
print(users_distinct[users_distinct['login_hash'].str.contains(pattern)])
print(users_distinct[users_distinct['server_hash'].str.contains(pattern)])
print(users_distinct[users_distinct['country_hash'].str.contains(pattern)])

connection = psycopg2.connect(**db_config)
cursor = connection.cursor()
query = "SELECT * FROM trades"
trades = pd.read_sql(query, connection)
trades.head()

# any duplicates of login_hash and ticket_hash
trades_distinct = trades.drop_duplicates()
print(len(trades_distinct))

# login_hash belongs to server_hash
unique_counts = trades.groupby('login_hash')['server_hash'].nunique()
# check if nunique > 1
violations = unique_counts[unique_counts > 1]
print(violations)


# ticket_hash belongs to server_hash
unique_counts = trades.groupby('ticket_hash')['server_hash'].nunique()
# check if nunique > 1
violations = unique_counts[unique_counts > 1]
print(violations)


# length of each type of hash

trades['login_hash_len'] = trades['login_hash'].apply(lambda x:len(x))
trades['server_hash_len'] = trades['server_hash'].apply(lambda x:len(x))
trades['ticket_hash_len'] = trades['ticket_hash'].apply(lambda x:len(x))
print(trades[['login_hash_len', 'server_hash_len', 'ticket_hash_len']].nunique())


# the distribution of server_hash
# COMMENT: like what was discovered in users table, server == '54203B42716FE7C40138AE6C4913EBBC', only contains 571 records, which may be an issue
print(trades.groupby('server_hash').count().reset_index()[['server_hash', 'ticket_hash']])





# hash should be made of alphametic letters and numbers
pattern = r'[^a-zA-Z0-9]'
print(trades[trades['login_hash'].str.contains(pattern)])
print(trades[trades['server_hash'].str.contains(pattern)])
print(trades[trades['ticket_hash'].str.contains(pattern)])

# 666 distinct users' login_hash in users table
l = users_distinct['login_hash'].unique().tolist()
# COMMENT: In trades table, there exist 5026 users that dont't belong to users table
len(trades[~trades.login_hash.isin(l)]['login_hash'].unique())



# enum values of symbol
print(trades['symbol'].unique())
# the distribution of symbol
print(trades.groupby('symbol').count().reset_index()[['symbol', 'login_hash']])




# the distribution of digits
print(trades.groupby('digits').count().reset_index()[['digits', 'login_hash']])




# the distribution of cmd
print(trades.groupby('cmd').count().reset_index()[['cmd', 'login_hash']])


# the distribution of volume
print(trades.groupby('volume').count().reset_index()[['volume', 'login_hash']])
print(trades['volume'].mean(), trades['volume'].median(), trades['volume'].min(), trades['volume'].max())
# volume should be > 0
# COMMENT: one abnormal data whose volume==0 exists
print(trades[trades['volume']<=0])


# the distribution of open_price
print(trades['open_price'].mean(), trades['open_price'].median(), trades['open_price'].min(), trades['open_price'].max())
# volume should be > 0
print(trades[trades['open_price']<=0])




# the distribution of open_time and close_time
print(trades['open_time'].min(), trades['open_time'].max())
print(trades['close_time'].min(), trades['close_time'].max())




# open_time should be earlier than close_time
print(trades[trades.open_time>trades.close_time])
# COMMENT: open_time == close_time, which is abnormal
print(trades[trades.open_time==trades.close_time])


# the distribution of contractsize
print(trades['contractsize'].mean(), trades['contractsize'].median(), trades['contractsize'].min(), trades['contractsize'].max())


trades['date_diff'] = trades['close_time'] - trades['open_time']
trades['date_diff'] = trades['date_diff'].dt.days
# COMMENT: a number of records' date difference between close_time and open_time are very large, more than 365 days, which may be an issue
print(trades['date_diff'].mean(), trades['date_diff'].median(), trades['date_diff'].min(), trades['date_diff'].max())
print(trades.groupby('date_diff').count().reset_index()[['date_diff', 'login_hash']])
print(trades[trades['date_diff'] > 365])
