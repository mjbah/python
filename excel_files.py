import pandas as pd

df_locations = pd.read_excel('Locations.xlsx', sheet_name='Locations', index_col='Location ID')
df_ratings = pd.read_excel('Ratings.xlsx', sheet_name='Locations', index_col='Location ID')

# Get the list of columns
#loctions_columns = list(df_locations.columns)
ratings_cols = list(df_ratings.columns)

df_locations_cols = ['Location HSCA start date', 'Care homes beds','Provider ID', 'Provider Companies House Number', 'Provider Charity Number']
#df_ratings_cols = []

df_locations = df_locations.reindex(columns=df_locations_cols)
#df_ratings = df_ratings.reindex(columns = df_locations_cols)

new_ratings_df = df_ratings.merge(df_locations, how='left', on='Location ID')
new_ratings_df.to_csv('newfile.csv')
#print(new_ratings_df.head())

