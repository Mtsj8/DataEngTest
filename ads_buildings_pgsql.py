import psycopg2
import pandas as pd
import numpy as np
from geopy.distance import distance

from tqdm import tqdm

import json
import os

import argparse

parser = argparse.ArgumentParser()

parser.add_argument('-host', dest='host', type=str, default='localhost',
                        help='DB host')
parser.add_argument('-p', '--port', dest='port', type=int, default=5432,
                        help='DB port')
parser.add_argument('-u', '--user', dest='user', type=str, default='kzas',
                        help='DB user')
parser.add_argument('-pass', '--password', dest='password', type=str, default='kzas',
                        help='DB password')
parser.add_argument('-db', '--db_name', dest='db', type=str, default='kzas',
                        help='DB name')
parser.add_argument('-data', '--data_path', dest='data_path', type=str, default='data/',
                        help='Folder containing the folder "ads/" with json files of the ads,' +\
                              'and the file "buildings.csv" with the information of the buildings')

# Parsing args
parse_args = parser.parse_args()

HOST = parse_args.host
PORT = parse_args.port
USER = parse_args.user
PASSWORD = parse_args.password
DB = parse_args.db
DATA_PATH = parse_args.data_path

ads_path =  DATA_PATH + 'ads/'
buildings_path =  DATA_PATH + 'buildings.csv'

class Ads_buildings_DB(object):
    def __init__(self, 
                 host, 
                 port, 
                 user, 
                 password, 
                 db, 
                 buildings_path):
        
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = db
        
        self.conn = psycopg2.connect(host=host,
                                     port=port,
                                     user=user,
                                     password=password,
                                     dbname=db)
        
        # Setting connecting to automatically send commands to the DB
        self.conn.set_session(autocommit=True)
        
        # Creating table, if it doesn't already exist
        if not self.table_exists():
            print('Creating table...')
            self.create_table()
            
            print('Done!')
            
        else:
            print('Table already exists.')
            
        # Reading the file with the buildings
        self.df = self._read_buildings(buildings_path)
        
        # Creating another dataframe without buildings that have no lat or lon
        # to optimize search by approximation
        self.df_full_lat_lon = self.df.dropna(subset=['latitude', 
                                                      'longitude'])
    
    def table_exists(self):
        '''
        Checking if the `ads_buildings` table exists in the DB that self.conn is connected.
        '''
        cursor = self.conn.cursor()
        cursor.execute("""
                       SELECT table_name FROM information_schema.tables
                       WHERE(table_name = 'ads_buildings');
                       """
                      )
        exist = bool(cursor.rowcount)
        cursor.close()
        
        return exist
    
    def create_table(self):
        '''
        Create `ads_buildings` table in the DB that self.conn is connected.
        '''
        cursor = self.conn.cursor()
        cursor.execute("""
                       CREATE TABLE ads_buildings(ads_id VARCHAR(16) PRIMARY KEY, 
                                                  building_id INT,
                                                  property_type VARCHAR(28)[] NOT NULL,
                                                  state VARCHAR(2) NOT NULL,
                                                  city VARCHAR(64) NOT NULL,
                                                  neighborhood VARCHAR(128) NOT NULL,
                                                  street VARCHAR(128) NOT NULL, 
                                                  street_number VARCHAR(64),
                                                  built_area_min INT, 
                                                  bedrooms_min INT,
                                                  bathrooms_min INT, 
                                                  parking_space_min INT,
                                                  cep VARCHAR(9),
                                                  sale_price DECIMAL(12, 2),
                                                  latitude DECIMAL(10, 8), 
                                                  longitude DECIMAL(11, 8), 
                                                  accurate BOOLEAN NOT NULL)
                        """
                      )
        cursor.close()
    
    def drop_table(self):
        '''
        Dropping the `ads_buildings` table, if it exists.
        '''
        if self.table_exists():
            cursor = self.conn.cursor()
            cursor.execute("""
                           DROP TABLE ads_buildings;
                           """
                          )
            cursor.close()
        else:
            print('Table does not exist')
    
    def _read_buildings(self, path):
        '''
        Reading the buildings file, and formatting it for use in the ads searches.
        '''
        df = pd.read_csv(path, dtype={'id':np.int32, 
                                      'address':np.object, 
                                      'address_number':np.object, 
                                      'neighborhood':np.object, 
                                      'city':np.object,
                                      'state':np.object,
                                      'cep':np.object, 
                                      'latitude':np.float32, 
                                      'longitude':np.float32})

        # Replacing state names with their abbreviations
        df['state'] = df['state'].map({'Acre': 'AC',
                                       'Alagoas': 'AL',
                                       'Amapá': 'AP',
                                       'Amazonas': 'AM',
                                       'Bahia': 'BA',
                                       'Ceará': 'CE',
                                       'Distrito Federal': 'DF',
                                       'Espírito Santo': 'ES',
                                       'Goiás': 'GO',
                                       'Maranhão': 'MA',
                                       'Mato Grosso': 'MT',
                                       'Mato Grosso do Sul': 'MS',
                                       'Minas Gerais': 'MG',
                                       'Pará': 'PA',
                                       'Paraíba': 'PB',
                                       'Paraná': 'PR',
                                       'Pernambuco': 'PE',
                                       'Piauí': 'PI',
                                       'Rio de Janeiro': 'RJ',
                                       'Rio Grande do Norte': 'RN',
                                       'Rio Grande do Sul': 'RS',
                                       'Rondônia': 'RO',
                                       'Roraima': 'RR',
                                       'Santa Catarina': 'SC',
                                       'São Paulo': 'SP',
                                       'Sergipe': 'SE',
                                       'Tocantins': 'TO'})
        
        # Setting the indexes to optimize the search for ads
        df = df.set_index(['state', 'city',  'neighborhood','address', 'address_number'])
        
        return df
    
    def _create_data(self, ads_data):
        '''
        Creating data with the data from the ads, and those searched in the buildings file.
        '''
        data = {}
        
        # Adding native data from the ads information
        data['ads_id'] = ads_data['idno']
        data['property_type'] = ads_data['property_type'] if isinstance(ads_data['property_type'], list)\
                                else [ads_data['property_type']]
        data['property_type'] = set(data['property_type'])
        data['state'] = ads_data['state']
        data['city'] = ads_data['city_name']
        data['neighborhood'] = ads_data['neighborhood']
        data['street'] = ads_data['street']
        data['street_number'] = ads_data['street_number']
        data['built_area_min'] = ads_data['built_area_min']
        data['bedrooms_min'] = ads_data['bedrooms_min']
        data['bathrooms_min'] = ads_data['bathrooms_min']
        data['parking_space_min'] = ads_data['parking_space_min']
        data['sale_price'] = ads_data['sale_price']
        data['latitude'] = ads_data['lat']
        data['longitude'] = ads_data['lon']

        # Searching the exact building that the ads refers
        try:            
            building_data = self.df.loc[ads_data['state'], 
                                        ads_data['city_name'], 
                                        ads_data['neighborhood'], 
                                        ads_data['street'], 
                                        ads_data['street_number']]
            accurate = True

            data['building_id'] = building_data['id']
            data['cep'] = building_data['cep']
            data['latitude'] = data['latitude'] if data['latitude']!=None\
                               else building_data['latitude']
            data['longitude'] = data['longitude'] if data['longitude']!=None\
                               else building_data['longitude']
        
        # If the exact location of the ads is not found, we set the accurate flag to false
        except KeyError:
            accurate = False
        
        data['accurate'] = accurate
        
        # If the exact location of the building of the ads has not been found, we will try to 
        # search for the location by approximation using the latitude and longitude
        if not accurate:
            try:
                if ads_data['lat'] == None or ads_data['lon'] == None:
                    return data

                ads_point = (ads_data['lat'], ads_data['lon'])
                
                # There are differences in the names of the neighborhoods in the data sources, 
                # so let's skip this neighborhood filter in the search, and search for the state, 
                # city, street name and number, and assume that if the distance from the building 
                # closest to the ads is less than 0.5 km, we assume that this is really the building 
                # of the ads, and that before it was not found due to an error in the name of the 
                # neighborhood. (Even so, the accurate flag will still be false to indicate that it 
                # was done based on approximation)
                df_street = self.df_full_lat_lon.loc[ads_data['state'], 
                                                     ads_data['city_name'], 
                                                     :, 
                                                     ads_data['street'],
                                                     ads_data['street_number']].copy()
                
                if df_street.shape[0] < 1:
                    return data
                
                df_street.loc[:, 'dist'] = df_street.apply(lambda row: 
                                                           distance(ads_point, 
                                                                   (row['latitude'], 
                                                                    row['longitude'])).km, 
                                                           axis=1)
                closest_building = df_street[df_street['dist'] == df_street['dist'].min()]

                if closest_building.shape[0]==1 and closest_building['dist'].iloc[0] <= 0.5:
                    data['building_id'] = closest_building.iloc[0]['id']
                    data['cep'] = closest_building.iloc[0]['cep']

                else:
                    return data
                
            except KeyError:
                return data
        
        return data
    
    def _create_insert_query(self, data):
        '''
        Creating query format for data insertion.
        '''
        insert = """
                 INSERT INTO ads_buildings({})
                 VALUES (
                 """.format(', '.join(data.keys()))
        
        insert += ', '.join(['%('+key+')s' for key in data.keys()]) + ');'
        
        return insert
    
    def insert_data(self, ads_path):
        '''
        Inserting data into DB.
        '''
        if not self.table_exists():
            raise Exception('Table has been dropped, it is necessary to create it again for data insertion.'+\
                            '\nYou can create it automatically with "self.create_table()"')
            
        tqdm_files = tqdm(os.listdir(ads_path))
        
        cursor = self.conn.cursor()
        for file in tqdm_files:
            data = {}
            if file.endswith('.json'):
                tqdm_files.set_description("Enriching %s" % file)

                ads_data = json.load(open(ads_path + file, 'rb'))
                
                # Checking if the ads ID is already in the table, if so, it will not be added to the DB
                cursor.execute("""
                               SELECT EXISTS(SELECT 1 FROM ads_buildings WHERE ads_id=%(idno)s)
                               """, ads_data)
                if cursor.fetchone()[0]:
                    tqdm_files.set_description('%s contains ID already in DB (%s)' % 
                                              (file, ads_data['idno']))
                    
                    continue

                data = self._create_data(ads_data)
                
                # Dropping information that is None
                data_format = {key:str(value) for key, value in data.items() if value is not None}

                tqdm_files.set_description("Inserting %s" % file)

                insert = self._create_insert_query(data_format)

                try:
                    cursor.execute(insert, data_format)
                except Exception as exp:
                    tqdm_files.set_description('[DB ERROR]: %s' % (str(exp)))
                    
        cursor.close()
        
if __name__ == '__main__':
    ads_db = Ads_buildings_DB(HOST, PORT, USER, PASSWORD, DB, buildings_path)
    ads_db.insert_data(ads_path)