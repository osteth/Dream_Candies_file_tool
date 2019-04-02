
# coding: utf-8

# In[8]:


#Setup
import pandas as pd
import os, csv, pathlib
import datetime
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

#get path to current project repository.
ROOT_P = os.path.abspath(os.curdir)

#Get all of the file path from the config file.
Customer_P = ROOT_P + config['PATHS']['Customer']
Invoice_P = ROOT_P + config['PATHS']['Invoice']
Item_P = ROOT_P +config['PATHS']['Item']
Output_P = config['PATHS']['Output']

#Get all of the file names from the config file.
Unique_customer_N = config['FILENAMES']['Unique_customers']
Full_N = config['FILENAMES']['Full']

print('---------   Reading Input files from: -------------')
print('Customer_P = ' + str(Customer_P))
print('Invoice_P = ' + str(Invoice_P))
print('Item_P = ' + str(Item_P))

print('--------- Loading Input files into dataframes -------------')
Customer = pd.read_csv(Customer_P.replace('~$','')).replace('"', '')
Invoice = pd.read_csv(Invoice_P.replace('~$','')).replace('"', '')
Item = pd.read_csv(Item_P.replace('~$','')).replace('"', '')

print('--------- Merging Inputs -------------')
'''Taking all three customer provided datasets and merging them together to create a single
master dataframe that can be used to easily reformat, filter, or process to attain any dataset
the customer could possibly want extracted from the data.  This takes a bit longer up front in terms
of processor time to build this dataframe but makes things much easer in the long run.'''
df = pd.merge(Customer, Invoice)
merged = pd.merge(df, Item, on=['INVOICE_CODE'])

#dropping the invoice totals column because we have line item prices and dont need to store that data.
merged.rename(columns = {'AMOUNT_y':'AMOUNT'}, inplace = True)
merged = merged.drop(['AMOUNT_x'], axis=1)

#splitting off the customer sample data to its own dataframe to save it off for delivery
Unique_customers = merged.drop_duplicates(subset=['CUSTOMER_CODE'])
Unique_customers = pd.DataFrame(Unique_customers, columns = ['CUSTOMER_CODE'])

#Ensuring filepath exists to prevent errors
path = pathlib.Path(ROOT_P + Output_P)
path.parent.mkdir(parents=True, exist_ok=True)

#Ensuring output filepath exists to prevent errors
path = pathlib.Path(ROOT_P + Output_P + Unique_customer_N)
path.parent.mkdir(parents=True, exist_ok=True)

print('--------- Writing to disk.... -------------')
#Saving off the customer sample data. 
Unique_customers.to_csv(ROOT_P + Output_P + Unique_customer_N, index=False, quotechar='"',
                      quoting=csv.QUOTE_NONNUMERIC)

'''Saving off the full merged dataframe.This will allow us to run this script to build and deliver the test set 
and then we only have to load and filter the data to generate any deliverable instead of reprocessing anything. '''


# Create today's date, to append to file
todaysdatestring = str(datetime.datetime.today().strftime('%Y-%m-%d'))

merged.to_csv(ROOT_P + Output_P + Full_N + todaysdatestring + '.csv.gz',
      header=True,
      index=False,
      compression='gzip',)
print("------------------------Action Completed!------------------------")

