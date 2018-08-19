"""
Linx Impulse Challange
Matheus Henrique Wagner 18/08/2018

This script contains all the detailed code used to answer the proposed questions
"""

# First we need to load our data

def load():

	# Loads the data
	# Returns a list of dictionaries, each dictionary being a log event

	print("Loading data...")

	import ndjson

	with open('ecommerce-events.ndjson') as f: # Name of the file
    		dataframe = ndjson.load(f)

	print("Done")

	return dataframe


# Now we need to deal with the duplicates

def treat_duplicates(dataframe, verbose=0):

	# dataframe = the data as loaded by load()
	# verbose = show progress
	# Returns the dataframe with the duplicates removed

	print("Removing duplicates...")

	import pandas as pd

	count = 0 # We'll count how many duplicates there are
	a = pd.DataFrame.from_dict(dataframe) # Working with a pandas DataFrame is a lot easier

	for column in a:
		a[column] = a[column].astype(str) # Turn everything into string

	a = pd.DataFrame.duplicated(a) # Returns a list with True at the duplicated values indexes

	for item in a:
                if item: # If duplicated row...
                        count += 1 # Add to the count

	if count: # If there are duplicates...
		for index in range(a.size-1,0,-1): # We have to do it backwards in order to not mess up the indices
			if a[index]: # If the row with the index is duplicated...
				del dataframe[index] # Remove that row
				if verbose:
					i += 1
					print(i,"/",count) # Show progress
	print("Removed", count, "duplicates")

	return dataframe # Sanitized data, there are 3908 duplicated row on the original data


# Now let's get a sense of how the data is organized

def entries(dataframe, title='eventType'):

	# dataframe = the data as loaded by load()
	# title = what column of the dataframe to check
	# Returns all the different entries in "title" (a),
	# and the indexes where they are (b)

        import numpy as np

        a = np.array([])
        b = []

        for row in range(len(dataframe)): # For each row of the dataframe
                c = dataframe[row-1][title]
                if dataframe[row][title] != c: # Check if the next entry differs from the last one
                        a = np.append(a, dataframe[row][title])	# If so, store its name
                        b = b+[row] # And say where it is

        return a, b # Now we now where are the page, search, product and transaction events


# Question 1

def revenue(dataframe):

	# dataframe = the data as loaded by load()
	# Prints the revenue

	revenue = 0

	# We'll only have to look at transaction log events
	# We know where they are from entires()

	for row in range(687593,len(dataframe)): # For each row on the transaction logs
		a = dataframe[row]['orderItems'] # Get the order
		for index in range(len(a)):
			revenue += a[index]['price']*a[index]['quantity'] # Add up the quantity and price of the orders

	# At this point we should double-check to see if there aren't any orders duplicated
	# For instance, we could match order id and price, but unfortunately I won't have time for that :(

	print(revenue) # The answer I got was (rounded on the cents) 212022.27


# Question 2

# I would find it useful to know the percentage of visitors who
# search for products seperately from those who actually buy products

def percentages_devices(dataframe):

	# dataframe = the data as loaded by load()
	# Returns the percentages of users who search and shop with mobile devices

	count = 0 # We'll count the logs for each device

	for row in range(687593): # Number of logs that are not transactions (687593)
		if(dataframe[row]['deviceType'] == 'mobile'):
			count += 1 # Counts how many logs where mobile

	percentage_search = (count*100)/687593 # Yields the percentage

	count = 0 # Reset

	for row in range(687593, len(dataframe)): # All the logs that are transactions
                if(dataframe[row]['deviceType'] == 'mobile'):
                        count += 1 # Counts them

	percentage_purchase = (count*100)/(len(dataframe)-687593) # Yields the percentage

	return percentage_search, percentage_purchase

	# The percentage of users who search on mobile = 41.68%
	# The percentage of users who shop on mobile = 32.31%


# I couldn't finish question 3, so we'll skip it for now

# Question 4

def traffic(dataframe):

	# dataframe = the data as loaded by load()
	# Returns daily and hourly traffic

	import pandas as pd

	dataframe = pd.DataFrame(dataframe) # Turn it into a pandas DataFrame
	dataframe = pd.DataFrame(dataframe['date']) # Get only the dates column
	dataframe = dataframe.set_index(dataframe['date']) # Get the dates as index (makes it easier to work with them)
	dataframe = dataframe.sort_index() # Get them in order
	dataframe = pd.to_datetime(dataframe['date'].index,format='%Y-%m-%d %H:%M:%S') # Turn them into pandas date type objects
	dataframe = pd.DataFrame(dataframe, index=dataframe) # Turn it back into a pandas dataframe
	daily_traffic = dataframe.groupby(dataframe.date.dt.dayofweek).count() # Group by day of week and count

	b = pd.Series(dataframe.index).dt.floor("H") # Returns a pandas Series with rounded down hours
	b = pd.Series(pd.RangeIndex(start=0, stop=688147, step=1), index=b.values) # Get the dates as index
	hourly_traffic = b.groupby(b.index.hour).count() # Group by hour and count

	return daily_traffic, hourly_traffic
"""
The results are:

daily_traffic:
0     122483
1     106447
2     102131
3      99550
4     105806
5      74747
6      76983

hourly_traffic:
0     15668
1      7547
2      4598
3      2553
4      2247
5      2347
6      4910
7     11269
8     23821
9     36804
10    42969
11    44705
12    39115
13    41180
14    44567
15    44469
16    43828
17    39176
18    41187
19    46954
20    44388
21    42469
22    35614
23    25762
"""

# As you can see, the indices aren't very explanatory, so I ran the following code to see if I could get the right dates

def aux_traffic(dataframe):

	# This part is almost identical to the traffic function

	dataframe = pd.DataFrame(dataframe)
        dataframe = pd.DataFrame(dataframe['date'])
        dataframe = dataframe.set_index(dataframe['date'])
        dataframe = pd.to_datetime(dataframe['date'].index,format='%Y-%m-%d %H:%M:%S')
        dataframe = pd.DataFrame(dataframe, index=dataframe)

	b = pd.Series(dataframe.index).dt.floor("D") # Returns a pandas Series rounded down to days
        b = pd.Series(pd.RangeIndex(start=0, stop=688147, step=1), index=b.values)
	b = pd.DataFrame(b)

	dataframe = b

	b = []
	a = np.array([])

	for i in range(688147): # This is similar to the code on entries()
		c = dataframe.index[i-1]
		if dataframe.index[i] != c: # Check if the next day differs from the last one
			a = np.append(a, dataframe.index[i]) # If so, store it
			b = b+[i] # And say where it is

	b[28] = 688147 # We'll need this information on the size of the Series of dates for the next step
	a = np.append(a, ['2017-06-05']) # This is also necessary, for the for loop, below

	c = pd.DataFrame(0, index=np.arange(1), columns=['2017-06-01','2017-06-02','2017-06-03','2017-06-04','2017-06-05','2017-06-06','2017-06-07'])

	for i in range(len(b)-1):
		c[a[i]]+=(b[i+1]-b[i]) # Calculate and store how many times each date appears

	return c

# And then I figured out the order:

"""
01/06/2017 - 99550
02/06/2017 - 105806
03/06/2017 - 74747
04/06/2017 - 76983
05/06/2017 - 122483
06/06/2017 - 106447
07/06/2017 - 102131

"""

# But I didn't have time to do something like that with the hour of the week :(


# Question 5

def campaign_results(dataframe):

	# dataframe = the data as loaded by load()

	import pandas as pd

	# Considering last touch attribuition, in my understanding we have to consider
	# the purchases that came directly from Campaing_2

	dataframe = dataframe[687593:688147] # Get only the transaction logs
	dataframe = pd.DataFrame(dataframe) # Turn it into a pandas DataFrame
	a = dataframe.groupby('utm_campaign').count() # How many buyers where brought by each campaign

	# This code returned zero for Campaign_2, which says Campaign_2 brought zero revenue
	# It might be that, or I interpreted the question wrong
	# So I checked a few more stuff that could be interesting...

	b = dataframe.groupby('utm_source').count()
	c = dataframe.groupby('utm_medium').count()

	# But couldn't really find anything to change my answer

	return a, b, c


# And finally, there was no time to tackle question 6

# But here's how far I got in question 3:

def searching_visitors(dataframe):

	# dataframe = the data as loaded by load()

        import pandas as pd
        import numpy as np

        unique_queries = pd.DataFrame.from_dict(dataframe[630561:687593]) # Get the search logs
        unique_queries = unique_queries['query'] # Get their queries

        frequencies = unique_queries.value_counts() # Count the instances of each query
        frequencies = frequencies[frequencies > 14] # Discard those that appear less than 15 times

        unique_queries = 0 # Free up some memory

        queries = pd.DataFrame.from_dict(dataframe[630561:687593]) # Get the search logs and the product logs
        product_pages = pd.DataFrame.from_dict(dataframe[524062:630561])

        frequencies = frequencies.to_dict()
        query_product = pd.DataFrame(0, index=np.arange(frequencies[0]), columns=frequencies.keys())
	# query_product should store where each visitor went after their search

	i = 0

        for query in frequencies.keys():
		i += 1
		print(i, "/", len(frequencies)) # Print progress
                clicks = 0
                a = queries.loc[(queries['query'] == query)] # Returns the instances of one query
                index2 = 0
		for index in a.index:
                        b = np.where(a[:]['visitor'][index] == product_pages[:]['visitor']) # Where is the visitor the same?
                        query_product[query][index2] = b # Store for each query, where the searchers and visitors of products were the same
			index2 += 1

	# The main problem here is that this code takes forever to run, so I couldn't really continue...

        return query_product


# I tried a few more things, without success

def clicks_through(t):

        # So I got this list t that has in each entry all of the queries of a user
        # and all of the product pages visited by them
        # But I actually lost the code I used to make it

	clicks_through = pd.DataFrame(0, index=np.arange(1), columns=[t[:][0]['query']]) # Didn't test this, but hopefully works

        for i in range(13150): # For each visitor
                print(i+1, "/", 13150) # Print progress
                for index1 in t[i][0]['searchItems'].index: # For each search
                        if(t[i][0]['searchItems'][index1] != None): # Avoid errors
                                for index2 in range(len(t[i][0]['searchItems'][index1])): # For each search result
                                        for index3 in t[i][1]['product'].index: # For each produc id
                                                if(t[i][0]['searchItems'][index1][index2]==t[i][1]['product'][index3]):
							# Check if product is in search results and product page
                                                        clicks_through[t[i][0]['query']] += 1 # This is a click-through associated with the query

	# This also takes way too long and I couldn't run it all the way through

        return clicks_through

# I tried a few more things, but got no success
