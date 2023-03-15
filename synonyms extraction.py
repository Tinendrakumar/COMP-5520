#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().system('pip install mysql-connector-python')


# In[12]:


import csv
import mysql.connector

# Connect to the UMLS database
cnx = mysql.connector.connect(host='172.16.34.1', port='3307',
                            user='umls', password='umls', database='umls2020')

# Create a cursor
cur = cnx.cursor()

# Define the query to extract CHV synonyms
query = '''
    SELECT CUI, GROUP_CONCAT(DISTINCT STR SEPARATOR ',') AS synonyms FROM MRCONSO
    WHERE SAB = 'CHV'
    AND TTY = 'SY'
    GROUP BY CUI;

'''

# Execute the query
cur.execute(query)

# Fetch the results
results = cur.fetchall()

# Define the filename for the CSV file
filename = 'chv_synonyms5.csv'

# Write the results to a CSV file
with open(filename, mode='w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['CHV', 'synonym'])
    for row in results:
        writer.writerow(row)

# Close the cursor and the database connection
cur.close()
cnx.close()


# In[ ]:




