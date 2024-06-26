"""
Created on Thu Jun 04 2024

@author: Santaiago Ramirez Vallejo

"""

# This code creates the needed dataframes in order to fit the data with the better model using only a edge list

# Calling the libreries
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import LineString
from pyproj import Geod
import winsound

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
# Adding the socio-economic stratification of the person that relises a trip
trips_df = (trips_df.merge(houses_df[['Id_Hogar', 'p5_estrato']], left_on='id_hogar', right_on='Id_Hogar')
            .rename(columns={'p5_estrato': 'stratification'})
            .drop(columns='Id_Hogar'))

# Measuring the distance between two ZATs
def zats_distance(or_zat, dest_zat):
    geod = Geod(ellps = 'WGS84')
    org_point = zats_map[zats_map['ZAT']==or_zat]['center'].iloc[0]
    dest_point = zats_map[zats_map['ZAT']==dest_zat]['center'].iloc[0]
    line = LineString([org_point, dest_point])
    dist = geod.geometry_length(line)
    return dist
# Measuring the distance between each ZAT 
zats_array = np.array(zats_map['ZAT'])
distances_array = []
zat_i = []
zat_j =[]
def dist_zats():
    for i in range(1, len(zats_array)):
        #print(i)
        for j in range(i+1, len(zats_array)):
            zat_i.append(zats_array[i])
            zat_j.append(zats_array[j])
            d = zats_distance(zats_array[i], zats_array[j])
            distances_array.append(d)
    return distances_array
distances_array = dist_zats()
# Making a dataframe with all the distances between zats
dict = {'zat i': zat_i, 'zat j': zat_j, 'Distances': distances_array}
dist_df = pd.DataFrame(dict).sort_values(by='Distances')

# Defining the function that creates the directed dataframe i.e. T_ij != T_ji
# The function needs only an edge list
def directed_dataframe(num_trips_df):
    # Computing the number of trips between diferent zats 
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

    # Computing s_ij for each trip registered 
    s_ij = []
    for i in range(len(num_dis_trips)):
        orgn = num_dis_trips.iloc[i]['zat_origen']
        dest = num_dis_trips.iloc[i]['zat_destino']
        #print(orgn, dest)
        my_dist = zats_distance(orgn, dest)
        my_array = dist_df[(dist_df['zat i']==orgn)|(dist_df['zat j']==orgn)]
        my_array = my_array[my_array['Distances']<my_dist]
        my_zones = list(my_array[my_array['zat i']==orgn]['zat j'])+list(my_array[my_array['zat j']==orgn]['zat i'])
        s = 0
        for j in my_zones:
            try:
                s = total_trips[total_trips['ZAT']==j]['trips'].iloc[0]+s
            except:
                continue
        s_ij.append(s)
    # Adding the s_ij quantity to the dataframe 
    num_dis_trips.loc[:, 's_ij'] = s_ij
    
    return num_dis_trips

# Creating the edge list of the diferent networks
# Edge list of all the network
all_network = trips_df[['zat_origen', 'zat_destino']]
# Edge list of the network separated by stratification
net_1 = trips_df[trips_df['stratification']==1][['zat_origen', 'zat_destino']]
net_2 = trips_df[trips_df['stratification']==2][['zat_origen', 'zat_destino']]
net_3 = trips_df[trips_df['stratification']==3][['zat_origen', 'zat_destino']]
net_4 = trips_df[trips_df['stratification']==4][['zat_origen', 'zat_destino']]
net_5 = trips_df[trips_df['stratification']==5][['zat_origen', 'zat_destino']]
net_6 = trips_df[trips_df['stratification']==6][['zat_origen', 'zat_destino']]

# Creating the directed dataframe of all the network
# num_dis_trips = directed_dataframe(all_network)
# Creating the dataframes for all the stratas
num_trips_1 = directed_dataframe(net_1)
num_trips_2 = directed_dataframe(net_2)
num_trips_3 = directed_dataframe(net_3)
num_trips_4 = directed_dataframe(net_4)
num_trips_5 = directed_dataframe(net_5)
num_trips_6 = directed_dataframe(net_6)

# Saving the dataframes
df_path = r'./Encuesta de Movilidad 2019/EODH/Archivos_CSV/num_dis_trips.csv'
path_1 = r'./Encuesta de Movilidad 2019/EODH/Archivos_CSV/estrato_1.csv'
path_2 = r'./Encuesta de Movilidad 2019/EODH/Archivos_CSV/estrato_2.csv'
path_3 = r'./Encuesta de Movilidad 2019/EODH/Archivos_CSV/estrato_3.csv'
path_4 = r'./Encuesta de Movilidad 2019/EODH/Archivos_CSV/estrato_4.csv'
path_5 = r'./Encuesta de Movilidad 2019/EODH/Archivos_CSV/estrato_5.csv'
path_6 = r'./Encuesta de Movilidad 2019/EODH/Archivos_CSV/estrato_6.csv'

# num_dis_trips.to_csv(df_path)
num_trips_1.to_csv(path_1)
num_trips_2.to_csv(path_2)
num_trips_3.to_csv(path_3)
num_trips_4.to_csv(path_4)
num_trips_5.to_csv(path_5)
num_trips_6.to_csv(path_6)

# Make a sound when the code ends
# time = 1000  # milliseconds
# freq = 440  # Hz
# winsound.Beep(freq, time)
