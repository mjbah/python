
# from IPython.display import FileLink
# from google.colab import files
import json
from datetime import datetime
import pandas as pd
import requests
import asyncio
import aiohttp
# import my_module_v3 as mm # My module with functions
# Dealing with Jupyter download
# df.to_csv('locations.csv')
# FileLink('locations.csv')
# Dealing with Google Colab download files
# files.download('example.txt') 

def get_urls():
    #filters = {'localAuthority': 'City of London'}
    filters = {'localAuthority': ['Isles of Scilly','Flintshire']}
    #filters = {}
    base_url = 'https://api.cqc.org.uk/public/v1/locations?page=1'
    base_response = requests.get(base_url, params=filters).json()    
    total_pages = int(base_response['totalPages']) + 1
    length = int(base_response['total'])
    all_locations_per_page = []
    
    # Loop through pages to get a page with list of locations
    for page in range(1,total_pages):    
        page_locations = []    
        url = f'https://api.cqc.org.uk/public/v1/locations?page={page}'
        locations_per_page = requests.get(url, params=filters).json()
        for location_id in locations_per_page['locations']:        
            page_locations.append(location_id['locationId'])
        all_locations_per_page.append(page_locations)

    # Unpack LIST of list of locations [[1,2,3],[4,5,6]]
    flat_list_of_locations = [one_list for nested_list in all_locations_per_page for one_list in nested_list]
    return flat_list_of_locations

# Using async to run parallel requests to get location URLs
async def get_locations(session, location_id):   
    location_url_id = f'https://api.cqc.org.uk/public/v1/locations/{location_id}'
    async with session.get(location_url_id) as location_response:
        locations_per_page = await location_response.json()
        return locations_per_page
               
async def main():
    flat_list_of_locations = get_urls()
    async with aiohttp.ClientSession() as session:
        tasks = []
        for location_id in flat_list_of_locations:                        
            task = asyncio.ensure_future(get_locations(session, location_id))
            await asyncio.sleep(0.2)
            tasks.append(task)
        all_the_locations = await asyncio.gather(*tasks)
    return all_the_locations

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
locations_results = asyncio.run(main())
df = pd.DataFrame(locations_results)

# Loop through to get regulated activities, service types, specialisms etc
def extract_values(key):    
    list_of_values = []
    for value in (df[key]):
        row_values = []        
        try: # Use try/except to check if exists
            for the_name in value:
                row_values.append(the_name['name'])
        except:
            row_values.append('')
        list_of_values.append(' | '.join(row_values))
    return list_of_values

# Loop through to get ratings data if available and split it up outcome and date
def current_ratings():    
    overall_rating = []
    for current_rating in df['currentRatings']:
        overall_rating_outcome = []               
        try:                
            overall_rating_outcome.append('|'.join([current_rating['overall']['rating'], 
                                                    datetime.strptime(current_rating['overall']['reportDate'],'%Y-%m-%d').strftime("%d-%m-%Y")]))
        except:            
            overall_rating_outcome.append('')               
        overall_rating.append(overall_rating_outcome)          
    return overall_rating

# Loop through to get last published dates if available or return blank date
def last_report_date():    
    report_dates = []
    for report_date in df['lastReport']:
        published_date = []
        try:
            # Append and convert the date string which is in format YYYY-MM-DD into actual date and format as DD-MM-YYYY
            published_date.append(datetime.strptime(report_date['publicationDate'],'%Y-%m-%d').strftime("%d-%m-%Y"))
        except:
            published_date.append('')               
        report_dates.append(published_date)
    return report_dates

# Run the functions to feed into the df
def get_final_output():    
    regulated_activities = extract_values('regulatedActivities')
    service_types = extract_values('gacServiceTypes')
    specialisms = extract_values('specialisms')
    last_report_dates = last_report_date()
    overall_rating = current_ratings()    
    df[('regulatedActivities')] = pd.DataFrame(regulated_activities)
    df[('gacServiceTypes')] = pd.DataFrame(service_types)
    df[('specialisms')] = pd.DataFrame(specialisms)
    df[('lastReport')] = pd.DataFrame(last_report_dates)
    df[('currentRatings')] = pd.DataFrame(overall_rating)
    #Split the ratings column which has outcome and date into two separate columns
    try:
        df[(['currentOverallRating','ratingsPubDate'])] = df[('currentRatings')].str.split('|', expand=True)
    except:
        df[(['currentOverallRating','ratingsPubDate'])] = df[('currentRatings')].str.split('', expand=True)
    df_locations = df[['locationId','name','registrationStatus','providerId','type','regulatedActivities','gacServiceTypes','specialisms','currentOverallRating','ratingsPubDate']]
    df_locations.columns = ['Location ID','Location Name','Location Status','Provider ID','Type','Regulated Activities','Service Types','Specialism','Current Overall Rating','Pubication Date']
 
    #Filter out for active locations only
    #active_locations = df[df['Location Status'] == 'Registered']       
    #print(df.iloc[:,:5])
    #print(df.tail())   
    return df_locations.to_csv(r'C:\Users\mjbah\OneDrive\Desktop\LA_locations.csv', index= False, header= True)

#Call function to run and export the data
get_final_output()
# Other method to run the event loop
# loop = asyncio.get_event_loop()
# loop.run_until_complete(main())
# loop.close()


