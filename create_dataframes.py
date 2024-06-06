"""
Created on Thu Jun 04 2024

@author: Santaiago Ramirez Vallejo

"""

# This code creates the needed dataframes in order to fit the data with the better model

# Calling the libreries
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
from pyproj import Geod

# Declaring the paths of the trips data
trips_path = r'./Encuesta de Movilidad 2019/EODH/Archivos_CSV/ViajesEODH2019.csv'
# Path of the population data
houses_path = r'./Encuesta de Movilidad 2019/EODH/Archivos_CSV/HogaresEODH2019.csv'
# Path of the ZATs shapefiles
shape_path = r'./Encuesta de Movilidad 2019/Zonificacion_(shapefiles)/ZONAS/ZONAS/ZAT.shp'
# Path of the streets shapefile
streets_path = r'./Encuesta de Movilidad 2019/Zonificacion_(shapefiles)/Malla_Vial_Integral_Bogota_D_C/Malla_Vial_Integral_Bogota_D_C.shp'
# Converting the CSV in a pandas dataframe
trips_df = pd.read_csv(trips_path, sep = ';')
# Converting the population data to dataframe
houses_df = pd.read_csv(houses_path, sep= ';', low_memory=False)
# Calling the ZAT's shapefile
zats_map = gpd.read_file(shape_path)
# Computing the centroid of each zat
zats_map['center'] = zats_map.to_crs('+proj=cea').centroid.to_crs(zats_map.crs)
zats_map = zats_map[(zats_map['MUNCod']==11001.0)|(zats_map['MUNCod']==25754.0)]
# Filtering the data in order to take only the trips in Bogotá and Soacha
trips_df = trips_df[(trips_df['mun_origen'] == 11001) | (trips_df['mun_origen'] == 25754)]
trips_df = trips_df[(trips_df['mun_destino'] == 11001) | (trips_df['mun_destino'] == 25754)]
houses_df = houses_df[(houses_df['municipio'] == 11001) | (houses_df['municipio'] == 25754)]
# Filtering the data to avoid the ZAT number 0.0
trips_df = trips_df[(trips_df['zat_origen']>0)&(trips_df['zat_destino']>0)]

# Defining that creates the directed dataframe i.e. T_ij != T_ji

def directed_dataframe():
    # Computing the number of trips between diferent zats 
    num_trips_df = trips_df[['zat_origen', 'zat_destino']]
    #num_trips_df['trips'] = np.ones(len(num_trips_df)) 
    num_trips_df.loc[0:, ['trips']] = 1
    num_trips_df = (num_trips_df.groupby(['zat_origen', 'zat_destino'], as_index=False)['trips']
                                .agg('sum')
                                .sort_values(by='trips', ascending=False))
    num_trips_df = num_trips_df[num_trips_df['zat_destino'] != num_trips_df['zat_origen']]

    num_dis_trips = (num_trips_df.merge(zats_map[['ZAT', 'center']], left_on='zat_origen', right_on='ZAT')
                                 .rename(columns={'center': 'Origin_center'})
                                 .drop(columns='ZAT'))
    num_dis_trips = (num_dis_trips.merge(zats_map[['ZAT', 'center']], left_on='zat_destino', right_on='ZAT')
                                  .rename(columns={'center': 'Destination_center'})
                                  .drop(columns='ZAT'))
    
    # Counting the in, out and total nodal strength of each ZAT
    origin_trips = (num_trips_df.groupby('zat_origen', as_index=False)['trips']
                    .agg('sum')
                    .sort_values(by='trips', ascending=False))
    dest_trips = (num_trips_df.groupby('zat_destino', as_index=False)['trips']
                    .agg('sum')
                    .sort_values(by='trips', ascending=False))
    total_trips = origin_trips.merge(dest_trips, left_on='zat_origen', right_on='zat_destino')
    total_trips['trips']=total_trips[['trips_x', 'trips_y']].sum(axis=1)
    total_trips = (total_trips.drop(columns=['trips_x', 'trips_y', 'zat_destino'])
                            .rename(columns={'zat_origen': 'ZAT'}))
    # Adding the mising zats that only have one trip
    miss_zats = list(set(origin_trips['zat_origen'])^set(dest_trips['zat_destino']))
    new_data= pd.DataFrame({'ZAT': miss_zats,
                            'trips':np.ones(len(miss_zats))})
    total_trips = pd.concat([total_trips, new_data], ignore_index=True)

    # Adding the nodal strength to our dataframe
    num_dis_trips = (num_dis_trips.merge(origin_trips, left_on='zat_origen', right_on='zat_origen')
                                .rename(columns={'trips_x': 'Trips', 'trips_y': 'Origin Trips'}))
    num_dis_trips = (num_dis_trips.merge(dest_trips, left_on='zat_destino', right_on='zat_destino')
                                .rename(columns={'trips': 'Destination Trips'})
                                .sort_values(by='Trips', ascending=False))
    num_dis_trips = (num_dis_trips.merge(total_trips, left_on='zat_origen', right_on='ZAT')
                                .rename(columns={'trips': 'Org Nodal Strength'}))
    num_dis_trips = (num_dis_trips.merge(total_trips, left_on='zat_destino', right_on='ZAT')
                                .rename(columns={'trips': 'Dest Nodal Strength'}))
    # Calculating the distances between origin and destination ZATS in meters
    distance = []
    geod = Geod(ellps = 'WGS84')
    for i in range(len(num_dis_trips)):
        line = LineString([num_dis_trips['Origin_center'].iloc[i], num_dis_trips['Destination_center'].iloc[i]])
        dist = geod.geometry_length(line)
        distance.append(dist)

    num_dis_trips['distance'] = distance
    num_dis_trips = num_dis_trips[['zat_origen', 'zat_destino', 'Origin Trips', 'Destination Trips', 'Org Nodal Strength', 'Dest Nodal Strength', 'distance','Trips']]
    
    return num_dis_trips

# Creating the directed dataframe
num_dis_trips = directed_dataframe()

print(num_dis_trips)
