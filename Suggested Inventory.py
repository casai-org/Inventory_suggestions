import pandas as pd
import numpy 
from sqlalchemy import create_engine
import os
from math import ceil

engine = create_engine('postgresql://{0}:{1}@{2}:{3}/{4}'.format(os.environ['DJANGO_USER'],
                                                                 os.environ['DJANGO_PASS'],
                                                                 os.environ['DJANGO_HOST'],
                                                                 os.environ['DJANGO_PORT'],
                                                                 os.environ['DJANGO_DB']),
                       connect_args={"options": "-c timezone=America/Mexico_City"})

# Read Amenities file
amenities = pd.read_csv('./Amenities.csv')

#Queries
units_info = '''select distinct(listing_nickname), 
            	list.bathrooms as bathrooms, 
            	list.bedrooms as bedrooms
            	from api_reservation as res
            	join api_listing as list on res.listingid = list.ID
            	order by listing_nickname '''

median_nights = ''' select listing_nickname, 
                percentile_disc(0.5) within group (order by nightscount) as nights from api_reservation
                where status= 'confirmed'
                group by listing_nickname'''

# Get 
days = 10
nights = pd.read_sql(median_nights, engine)
nights['checkouts'] = days / nights['nights']

units = pd.read_sql(units_info, engine)

units = units.merge(nights.copy(), on = 'listing_nickname').dropna()

# Limit the number of checkouts between [2,7]
units.loc[units['checkouts'] > 7, 'checkouts'] = 7
units.loc[units['checkouts'] < 2, 'checkouts'] = 2
units.head()

# Define the get_inventory function
def get_inventory(unit, amenities, is_building = False):
    '''
    unit: This function takes a dataframe with:
    listing_nickname | bathrooms | bedrooms | nights | checkouts
    ------------------------------------------------------------
    
    It may be a unit, or a building with all the 
    values summed up.
    
    amenities: A dataframe of products with:
    Product | Per | Quantity
    ------------------------
    
    is_building: Set to True if it's a building
    
    EXAMPLES:
    
    Unit:
    unit = units[unit['listing_nickname'] == 'ASOLA 72 - 501']
    get_inventory(unit, amenities)
    
    Building:
    building = units[units['listing_nickname'].str.startswith('BEF')].sum()
    get_inventory(building, amenities, True)
    '''
    if isinstance(unit, pd.DataFrame):
        unit = unit.iloc[0] # Series type is required
    
    print('Unit',unit['listing_nickname'])
    print('Days', days)
    print('Nights', unit['nights'])
    print('Checkouts', unit['checkouts'])
    print('Bedrooms',unit['bedrooms'])
    print('Bathrooms',unit['bathrooms'])
    

    amenities.loc[amenities['Per'] == 'Unit', 'Sugg_Inv'] = unit['checkouts'] * amenities['Quantity']
    amenities.loc[amenities['Per'] == 'Bathroom', 'Sugg_Inv'] = unit['checkouts'] * amenities['Quantity'] \
                                                                * unit['bathrooms']
    amenities.loc[amenities['Per'] == 'Bedroom', 'Sugg_Inv'] = unit['checkouts'] * amenities['Quantity'] \
                                                               * unit['bedrooms']
        
        
    amenities['Sugg_Inv'] = amenities['Sugg_Inv'].apply(ceil)
    
    if is_building:
        amenities.to_csv('Inventory Suggested - ' + unit['listing_nickname'][:4] + '.csv', index = False)
    else:
        amenities.to_csv('Inventory Suggested - ' + unit['listing_nickname'] + '.csv', index = False )

    

