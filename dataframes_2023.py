"""
Created on Thu Jul 11 2024

@author: Santaiago Ramirez Vallejo

"""

# This code creates the needed dataframes in order to fit the data with the better model using only a edge list for the 2023 survey

# Calling the libreries 
import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import LineString
from pyproj import Geod
from datetime import datetime, timedelta, time
import winsound
import os

# Declaring the paths of the trips data
trips_path = r'./Encuesta de Movilidad 2023/EODH/CSV/Modulo_viajes.csv'
# Converting the CSV in a pandas dataframe
trips_df = pd.read_csv(trips_path, sep = ';', encoding='Windows-1252', low_memory=False)
# Path to the population data
houses_path = r'./Encuesta de Movilidad 2023/EODH/CSV/Modulo_hogares.csv'
# Converting the population data to a pandas df
houses_df = pd.read_csv(houses_path, sep=';', encoding='Windows-1252', low_memory=False)
# Path of the ZATs shapefiles
shape_path = r'./Encuesta de Movilidad 2023/03_Zonificacion EODH/ZAT2023/ZAT2023.shp'
# Calling the ZAT's shapefile
zats_map = gpd.read_file(shape_path)
# Path of the streets shapefile
streets_path = r'./Encuesta de Movilidad 2019/Zonificacion_(shapefiles)/Malla_Vial_Integral_Bogota_D_C/Malla_Vial_Integral_Bogota_D_C.shp'
# Calling the streets shapefile
streets_map = gpd.read_file(streets_path)

# Converting the coordinate reference system of the ZATs shapefile in to be compatible
zats_map = zats_map.to_crs(streets_map.crs)
# Computing the centroid of each zat
zats_map['center'] = zats_map.to_crs('+proj=cea').centroid.to_crs(zats_map.crs)
zats_map = zats_map[(zats_map['MUNCod']==11001.0)|(zats_map['MUNCod']==25754.0)]

# Filtering the data in order to take only the trips in Bogotá and Soacha
trips_df = trips_df[(trips_df['nom_mun_ori'] == 'Bogotá') | (trips_df['nom_mun_ori'] == 'Soacha')]
trips_df = trips_df[(trips_df['nom_mun_des'] == 'Bogotá') | (trips_df['nom_mun_des'] == 'Soacha')]

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
def directed_dataframe(num_trips_df, out_nodes, in_nodes):
    # Computing the number of trips between diferent zats
    num_trips_df.loc[0:, ['trips']] = 1
    # out_nodes = 'zat_ori'
    # in_nodes = 'zat_des'
    num_trips_df = (num_trips_df.groupby([out_nodes, in_nodes], as_index=False)['trips']
                                .agg('sum')
                                .sort_values(by='trips', ascending=False))
    num_trips_df = num_trips_df[num_trips_df[out_nodes] != num_trips_df[in_nodes]]

    # Adding the center of each ZAT to the df
    num_dis_trips = (num_trips_df.merge(zats_map[['ZAT', 'center']], left_on=out_nodes, right_on='ZAT')
                                 .rename(columns={'center': 'Origin_center'})
                                 .drop(columns='ZAT'))
    num_dis_trips = (num_dis_trips.merge(zats_map[['ZAT', 'center']], left_on=in_nodes, right_on='ZAT')
                                  .rename(columns={'center': 'Destination_center'})
                                  .drop(columns='ZAT'))
    
    # Counting the in, out and total nodal strength of each ZAT
    origin_trips = (num_trips_df.groupby(out_nodes, as_index=False)['trips']
                    .agg('sum')
                    .sort_values(by='trips', ascending=False))
    dest_trips = (num_trips_df.groupby(in_nodes, as_index=False)['trips']
                    .agg('sum')
                    .sort_values(by='trips', ascending=False))
    total_trips = origin_trips.merge(dest_trips, left_on=out_nodes, right_on=in_nodes)
    total_trips['trips']=total_trips[['trips_x', 'trips_y']].sum(axis=1)
    total_trips = (total_trips.drop(columns=['trips_x', 'trips_y', in_nodes])
                            .rename(columns={out_nodes: 'ZAT'}))
    # Adding the mising zats that only have one trip, or don´t have any origin or destination trip
    miss_zats = list(set(origin_trips[out_nodes])^set(dest_trips[in_nodes]))
    new_data= pd.DataFrame({'ZAT': miss_zats,
                            'trips':np.ones(len(miss_zats))})
    total_trips = pd.concat([total_trips, new_data], ignore_index=True)

    miss_zats_des = list(set(total_trips['ZAT'])-set(dest_trips[in_nodes]))
    new_des_data = pd.DataFrame({in_nodes: miss_zats_des,
                            'trips':np.zeros(len(miss_zats_des))})
    dest_trips = pd.concat([dest_trips, new_des_data], ignore_index=True)

    miss_zats_org = list(set(total_trips['ZAT'])-set(origin_trips[out_nodes]))
    new_org_data = pd.DataFrame({out_nodes: miss_zats_org,
                            'trips':np.zeros(len(miss_zats_org))})
    origin_trips = pd.concat([origin_trips, new_org_data], ignore_index=True)

    # Adding the nodal strength to our dataframe
    num_dis_trips = (num_dis_trips.merge(origin_trips, left_on=out_nodes, right_on=out_nodes)
                                .rename(columns={'trips_x': 'Trips', 'trips_y': 'Origin Trips'}))
    num_dis_trips = (num_dis_trips.merge(dest_trips, left_on=in_nodes, right_on=in_nodes)
                                .rename(columns={'trips': 'Destination Trips'})
                                .sort_values(by='Trips', ascending=False))
    num_dis_trips = (num_dis_trips.merge(total_trips, left_on=out_nodes, right_on='ZAT')
                                .rename(columns={'trips': 'Org Nodal Strength'}))
    num_dis_trips = (num_dis_trips.merge(total_trips, left_on=in_nodes, right_on='ZAT')
                                .rename(columns={'trips': 'Dest Nodal Strength'}))
    
    # Calculating the distances between origin and destination ZATS in meters
    distance = []
    geod = Geod(ellps = 'WGS84')
    for i in range(len(num_dis_trips)):
        line = LineString([num_dis_trips['Origin_center'].iloc[i], num_dis_trips['Destination_center'].iloc[i]])
        dist = geod.geometry_length(line)
        distance.append(dist)

    num_dis_trips['Distances'] = distance
    num_dis_trips = num_dis_trips[['zat_ori', 'zat_des', 'Origin Trips', 'Destination Trips', 'Org Nodal Strength', 'Dest Nodal Strength', 'Distances','Trips']]

    # Computing s_ij for each trip registered, and S_ij using the in nodal strength
    s_ij = []
    S_ij = []
    for i in range(len(num_dis_trips)):
        orgn = num_dis_trips.iloc[i][out_nodes]
        dest = num_dis_trips.iloc[i][in_nodes]
        my_dist = zats_distance(orgn, dest)
        my_array = dist_df[(dist_df['zat i']==orgn)|(dist_df['zat j']==orgn)]
        my_array = my_array[my_array['Distances']<my_dist]
        my_zones = list(my_array[my_array['zat i']==orgn]['zat j'])+list(my_array[my_array['zat j']==orgn]['zat i'])
        s = 0
        S = 0
        for j in my_zones:
            try:
                s = total_trips[total_trips['ZAT']==j]['trips'].iloc[0]+s
                S = dest_trips[dest_trips[in_nodes]==j]['trips'].iloc[0]+S
            except:
                continue
        s_ij.append(s)
        S_ij.append(S)
    # Adding the s_ij quantity to the dataframe 
    num_dis_trips.loc[:, 's_ij'] = s_ij
    num_dis_trips.loc[:, 'S_ij'] = S_ij

    return num_dis_trips

# Creating the edge list of the diferent networks
edges = ['zat_ori', 'zat_des']

# Edge list of all the network
all_network = trips_df[edges]

# Edge list of separated networks in function of the stratification
net_1 = trips_df[trips_df['estra_hg']=='1'][edges]
net_2 = trips_df[trips_df['estra_hg']=='2'][edges]
net_3 = trips_df[trips_df['estra_hg']=='3'][edges]
net_4 = trips_df[trips_df['estra_hg']=='4'][edges]
net_5 = trips_df[trips_df['estra_hg']=='5'][edges]
net_6 = trips_df[trips_df['estra_hg']=='6'][edges]

# Creating the directed dataframe of all the network
num_dis_trips = directed_dataframe(all_network, 'zat_ori', 'zat_des')

# Creating the dataframes for all the stratas
num_trips_1 = directed_dataframe(net_1, 'zat_ori', 'zat_des')
num_trips_2 = directed_dataframe(net_2, 'zat_ori', 'zat_des')
num_trips_3 = directed_dataframe(net_3, 'zat_ori', 'zat_des')
num_trips_4 = directed_dataframe(net_4, 'zat_ori', 'zat_des')
num_trips_5 = directed_dataframe(net_5, 'zat_ori', 'zat_des')
num_trips_6 = directed_dataframe(net_6, 'zat_ori', 'zat_des')

# Saving the dataframes
df_path = r'./Encuesta de Movilidad 2023/EODH/CSV/num_dis_trips.csv'
path_1 = r'./Encuesta de Movilidad 2023/EODH/CSV/estrato_1.csv'
path_2 = r'./Encuesta de Movilidad 2023/EODH/CSV/estrato_2.csv'
path_3 = r'./Encuesta de Movilidad 2023/EODH/CSV/estrato_3.csv'
path_4 = r'./Encuesta de Movilidad 2023/EODH/CSV/estrato_4.csv'
path_5 = r'./Encuesta de Movilidad 2023/EODH/CSV/estrato_5.csv'
path_6 = r'./Encuesta de Movilidad 2023/EODH/CSV/estrato_6.csv'

num_dis_trips.to_csv(df_path)
num_trips_1.to_csv(path_1)
num_trips_2.to_csv(path_2)
num_trips_3.to_csv(path_3)
num_trips_4.to_csv(path_4)
num_trips_5.to_csv(path_5)
num_trips_6.to_csv(path_6)

# Shutdown the computer when the code ends to run
# os.system("shutdown /s /t 1")

# Make a sound when the code ends
time = 1000  # milliseconds
freq = 440  # Hz
winsound.Beep(freq, time)