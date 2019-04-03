
# coding: utf-8

# In[1]:


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
Customer_sample_P = ROOT_P +config['PATHS']['Customer_sample']
Output_P = config['PATHS']['Output']

#Get all of the file names from the config file.
Customer_N = config['FILENAMES']['Customer']
Invoice_N = config['FILENAMES']['Invoice']
Item_N = config['FILENAMES']['Item']


# In[2]:

print('---------   Reading Input files from: -------------')
print('Customer_P = ' + str(Customer_P))
print('Invoice_P = ' + str(Invoice_P))
print('Item_P = ' + str(Item_P))
print('Customer_sample_P = ' + str(Customer_sample_P))


# In[3]:

print('--------- Loading Input files into dataframes -------------')
Customer = pd.read_csv(Customer_P.replace('~$','')).replace('"', '')
Invoice = pd.read_csv(Invoice_P.replace('~$','')).replace('"', '')
Item = pd.read_csv(Item_P.replace('~$','')).replace('"', '')
Customer_sample = pd.read_csv(Customer_sample_P.replace('~$','')).replace('"', '')


# In[4]:

Customer


# In[5]:

Invoice


# In[6]:

Item


# In[7]:

Customer_sample


# In[8]:

Customer_sample['CUSTOMER_CODE'].values


# In[9]:

sub_customer = Customer.loc[Customer['CUSTOMER_CODE'].isin(Customer_sample['CUSTOMER_CODE'].values)]


# In[10]:

sub_customer
df = pd.merge(sub_customer, Invoice)
sub_Invoice = df.drop(['FIRSTNAME', 'LASTNAME'], axis=1)
df = sub_Invoice.drop(['CUSTOMER_CODE', 'AMOUNT', 'DATE'], axis=1)
sub_Item = pd.merge(df, Item)


# In[11]:

print('--------- Writing to disk.... -------------')
#Ensuring output filepath exists to prevent errors
def Validatd_path(file_path):
    path = pathlib.Path(file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return(path)
#Saving off the customer sample data. 
sub_customer.to_csv(Validatd_path(ROOT_P + Output_P + Customer_N), index=False, quotechar='"',
                      quoting=csv.QUOTE_ALL)

sub_Invoice.to_csv(Validatd_path(ROOT_P + Output_P + Invoice_N), index=False, quotechar='"',
                      quoting=csv.QUOTE_ALL)

sub_Item.to_csv(Validatd_path(ROOT_P + Output_P + Item_N), index=False, quotechar='"',
                      quoting=csv.QUOTE_ALL)

print("------------------------Action Completed!------------------------")

