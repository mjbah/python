import requests
import json
from bs4 import BeautifulSoup
import pandas as pd
from itertools import chain

table = []

# 20 pages = 456 index
joined_range = chain(range(1,2),range(24,121,24))

for page in joined_range:
   
    url = f'https://www.rightmove.co.uk/property-for-sale/find.html?searchType=SALE&locationIdentifier=REGION%5E61307&insId={page}&radius=0.0&minPrice=&maxPrice=&minBedrooms=&maxBedrooms=&displayPropertyType=&maxDaysSinceAdded=&_includeSSTC=on&sortByPriceDescending=&primaryDisplayPropertyType=&secondaryDisplayPropertyType=&oldDisplayPropertyType=&oldPrimaryDisplayPropertyType=&newHome=&auction=false'
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')

    for element in soup.find_all('div', { 'class': 'propertyCard'}):
        
        row = []
        #row.append(element.find('div', { 'class': 'propertyCard-details' }))    
        row.append(element.find('h2', { 'class': 'propertyCard-title' }).text)
        row.append(element.find('meta', { 'itemprop': 'streetAddress' })['content'])
        row.append(element.find('span', { 'itemprop': 'description' }).text)
        row.append(element.find('a', { 'class': 'propertyCard-contactsPhoneNumber'}).text)
        row.append(element.find('span', { 'class': 'propertyCard-branchSummary-addedOrReduced' }).text)
        row.append(element.find('div', { 'class': 'propertyCard-priceValue'}).text)

        table.append(row)
 
    #data.append(table)
df = pd.DataFrame(data = table, columns = ['Property','Address','Description','Agent Phone','Date Added','Price'])
#print(df)
df = df.replace('\n','',regex=True)
df['Price'] = df['Price'].str.replace('£','')
df['Property'] = df['Property'].str.strip()
#print(df['title'])
df.to_csv(r'C:\Users\mjbah\OneDrive\Desktop\houses.csv', index= False, header= True)