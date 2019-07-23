import netCDF4 as nc
import numpy as np
from datetime import datetime
from array import array
import sys
import os
import multiprocessing as mp

# path = 'http://geoloc-tremplin.ens-lyon.fr/climato-data/Toulouse-1/'
path = '/home/vidal/TremplinDesSciences/2019/ClimatLyon/ConvertedDrias/Toulouse-1/'

t_max_26_thisrun = nc.Dataset(path + 'tasmax_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc',
                              mode='r', format="NETCDF4", diskless=False)
t_max_26 = t_max_26_thisrun.variables['tasmax']
# print('Structure et taille du tableau exporté :\n\t', t_max_26_thisrun.variables['tasmax'].dimensions, t_max_26.shape)
# print('\nPremière ligne de données :\n', t_max_26[0, 0, :])
# print('Dernière ligne de données :\n', t_max_26[-1, -1, :])

thisrun_date = t_max_26_thisrun.variables['time']
print('Taille du tableau de dates - ', thisrun_date.shape)
print('Date de début de la simulation : ', nc.num2date(thisrun_date[0], thisrun_date.units).strftime("%c"))
print('Date de fin de la simulation : ', nc.num2date(thisrun_date[-1], thisrun_date.units).strftime("%c"))
#  latitude longitude
thisrun_lat, thisrun_lon=t_max_26_thisrun.variables['lat'], t_max_26_thisrun.variables['lon']
print('Emprise du projet en latititude-Longitude ;\n', thisrun_lat[0][0], '# ', thisrun_lon[0][0], ' :: ',
      thisrun_lat[-1][-1], '# ', thisrun_lon[-1][-1])
#  coordonnées métriques
thisrun_x, thisrun_y=t_max_26_thisrun.variables['x'], t_max_26_thisrun.variables['y']
print('Emprise du projet en x-y mètres ;\n', thisrun_x[0], '# ', thisrun_y[0], ' :: ', thisrun_x[-1], '# ', thisrun_y[-1])
thisrun_gridi, thisrun_gridj=t_max_26_thisrun.variables['i'], t_max_26_thisrun.variables['j']
#  coordonnées grille Aladin
print('Emprise du projet en noeuds ALADIN ;\n', thisrun_gridi[0], '# ', thisrun_gridj[0], ' :: ', thisrun_gridi[-1], '# ',
      thisrun_gridj[-1])

#  par sécurité efface le fichier portadatain=numpy.array(['foo', 'bar'], dtype='S3')nt ce nom
#  ! attention aux pertes possibles

try:
    os.remove('t_min-t_max-rstr_thisrun_26-45-85.nc')
except OSError:
    pass
extract_params_year_month=nc.Dataset('t_min-t_max-rstr_thisrun_26-45-85.nc', mode='w', format='NETCDF4')

#  tableau du nom des mois
listmonth=np.array(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 
            'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'All'])
#  Table of each month number of days for an ordinary year
len_month_a =[31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
#  Table of each month number of days for a leap year
len_month_b =[31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
#  Conversion en nombre entier des années extrêmes
# ==============================================================
# ==  Choix de la première et de la dernière année de l'étude ==
# ==============================================================
first_year = 2006
last_year = 2100
# ==============================================================
#  Détermination de la taille des tableaux de calcul
size_months = len(listmonth)
twelve_months = size_months - 1
size_years = last_year - first_year + 1
size_grid_i = thisrun_gridi.shape[0]
size_grid_j = thisrun_gridj.shape[0]
print("DateTime de départ  de l'étude : ", first_year, 
      "\nDateTime de fin  de l'étude : ", last_year, 
      "\nDurée de l'étude : ", size_years, 'ans',
      "\nNb de mois dans l'année : ", twelve_months)

# 
#  Création des dimensions du tableau mois et année sont séparés pour disjoindre les traitements sur ces variables
# 
extract_params_year_month.createDimension('i', size_grid_i)     # latitude axis
extract_params_year_month.createDimension('j', size_grid_j)    # longitude axis
extract_params_year_month.createDimension('month', size_months)    # month axis
extract_params_year_month.createDimension('year', size_years)  # year axis
extract_params_year_month.title = 'Extrait TSMax par moyenne mensuelle de 2006 a 2100 Lyon et sa region'
extract_params_year_month.institution = 'ENS de Lyon'
extract_params_year_month.institute_id = 'IFE Institut Francais de l Education'
extract_params_year_month.project_id = 'Climat et meteo tremplin pour l enseignement des sciences'
extract_params_year_month.model_id = 'CNRM-ALADIN52'
extract_params_year_month.product = 'output derived from Meteofrance DRIAS data'
extract_params_year_month.contact = 'gerard.vidal@ens-lyon.fr'
extract_params_year_month.creation_date = str(datetime.now())
extract_params_year_month.driving_experiment_name = 'DRIAS2014'
extract_params_year_month.experiment = 'RCP2.6 RCP4.5 RCP8.5 '
extract_params_year_month.model = 'ALADIN-Climat'
extract_params_year_month.author = 'Gerard Vidal'
extract_params_year_month.comment = "Extraction des moyennes de la region Lyonnaise de 2006 a 2100 " \
                                    "et changegement des variables"

#  Define two variables with the same names as dimensions, 
#  a conventional way to define "coordinate variables".
i = extract_params_year_month.createVariable('i', 'i4', ('i', ))
i.long_name = t_max_26_thisrun.variables['i'].long_name
j = extract_params_year_month.createVariable('j', 'i4', ('j', ))
j.long_name = t_max_26_thisrun.variables['j'].long_name
lat = extract_params_year_month.createVariable('lat', 'f4', ('j', 'i'))
lat.units = t_max_26_thisrun.variables['lat'].units
lat.long_name = t_max_26_thisrun.variables['lat'].long_name
lat.standard_name = t_max_26_thisrun.variables['lat'].standard_name
lat._CoordinateAxisType = t_max_26_thisrun.variables['lat']._CoordinateAxisType
lon = extract_params_year_month.createVariable('lon', 'f4', ('j', 'i', ))
lon.units = t_max_26_thisrun.variables['lon'].units
lon.long_name = t_max_26_thisrun.variables['lon'].long_name
lon.standard_name = t_max_26_thisrun.variables['lon'].standard_name
lon._CoordinateAxisType = t_max_26_thisrun.variables['lat']._CoordinateAxisType
x = extract_params_year_month.createVariable('x', 'i4', ('i', ))
x.units = t_max_26_thisrun.variables['x'].units
x.long_name = t_max_26_thisrun.variables['x'].long_name
x.standard_name = t_max_26_thisrun.variables['x'].standard_name
y = extract_params_year_month.createVariable('y', 'i4', ('j', ))
y.units = t_max_26_thisrun.variables['y'].units
y.long_name = t_max_26_thisrun.variables['y'].long_name
y.standard_name = t_max_26_thisrun.variables['y'].standard_name
month = extract_params_year_month.createVariable('month', 'S3', ('month', ))
month.units = 'month'
month.long_name = 'month_name'
month.standard_name = 'month_name'
year = extract_params_year_month.createVariable('year', 'u4', ('year', ))
year.units = 'date'
year.long_name = 'year'
year.standard_name = 'year'
#  Filling variables with  source data
i[:] = thisrun_gridi[:]
j[:] = thisrun_gridj[:]
lat[:] = thisrun_lat[:, :]
lon[:] = thisrun_lon[:, :]
x[:] = thisrun_x[:]
y[:] = thisrun_y[:]
#  Filling new variables
month[:] = listmonth[:]
dd = 0
for date in range(first_year, last_year+1):
    year[dd] = date
    dd += 1

t_max_26_thisrun.close()

# ========== Function addFileMeanTKToSet ====================================


def add_var_temp_to_set(ii, local_path, names):

    global twelve_months, len_month_a, len_month_b
    global first_year, size_years, size_months, size_grid_j, size_grid_i
    drias_filename = names[0]
    var_outname = names[1]
    var_in = names[2]
    minmax = names[3]

    print('start : ', var_outname, datetime.now())
    name_in = local_path + drias_filename
    name_out = 'this_set_' + var_outname
    # open source file for one parameter
    source_data_set = nc.Dataset(name_in, mode='r', format="NETCDF4")

    this_set_var = source_data_set.variables[var_in]
    this_set_dates = source_data_set.variables['time']

    # create temp netCDF to host results (netcdf cannot be returned)
    try:
        os.remove(name_out)
    except OSError:
        pass
    netcdf_output = nc.Dataset(name_out, mode='w', format='NETCDF4')
    netcdf_output.createDimension('i', size_grid_i)  # latitude axis
    netcdf_output.createDimension('j', size_grid_j)  # longitude axis
    netcdf_output.createDimension('month', size_months)  # month axis
    netcdf_output.createDimension('year', size_years)  # year axis

    netcdf_output.createVariable(var_outname, 'f4', ('year', 'month', 'j', 'i'), fill_value=1.e+20)
    for thisAttr in source_data_set.variables[var_in].ncattrs():
        netcdf_output.variables[var_outname].setncattr(thisAttr, source_data_set.variables[var_in].getncattr(thisAttr))
    # As we deal with temperatures that we are converting from K to C we change the "units" value
    # if var == 'tasmin' or var == 'tasmax':
    netcdf_output.variables[var_outname].setncattr('units', 'degree C')

    # date of the beginning of computations (num value)
    initial_year_date = nc.date2num(datetime(2006, 1, 1), this_set_dates.units)
    # date of the beginning of THIS study ( num value >  initial_year_date)
    first_year_date = nc.date2num(datetime(first_year, 1, 1), this_set_dates.units)
    # print('shape', this_set_dates.shape)
    # print ('initial_year_date first_year_date', initial_year_date, first_year_date)
    # iterj varies from 0 to sizeyears
    start_year = 0
    stop_year = size_years
    iteri = 0
    yeardate = initial_year_date
    while yeardate < first_year_date :
        iteri += 1
        yeardate += 24  # 24 hours a day ! we are counting in hours
#     this_date=nc.num2date(this_set_dates[iteri], this_set_dates.units).strftime("%c")
#     print('iteri this_date', iteri, this_date)
#     print('début -> fin, iteri= : ', start_year, stop_year, iteri)
#     print ('theseDates[iteri] :', theseDates[iteri], theseDates[100], theseDates[1000])

    for iterj in range(start_year, stop_year):

        this_date = nc.num2date(this_set_dates[iteri], this_set_dates.units).strftime("%c")
        this_year = int(nc.num2date(this_set_dates[iteri], this_set_dates.units).strftime("%Y"))
        # 
        #  Compteur permettant de suivre l'avancement du calcul et le fait que chaque année
        #  commence bien le 1er janvier à 00h00 (suivi correct des mois et années bissextiles)
        # 
        # print('\r', var_outname, iterj, str(this_date), end="")
        if this_year % 4 == 0:
            for p in range(len(len_month_b)):
                iteriLast=iteri + len_month_b[p]
                #  moyenne du mois année bissextile
                # tempo_name[iterj, p, :, :] = np.mean(this_set_var[iteri:iteriLast, :, :] - 273,
                # axis=0, dtype=np.float32)
                netcdf_output.variables[var_outname][iterj, p, :, :] = \
                    np.mean(this_set_var[iteri:iteriLast, :, :] - 273, axis=0, dtype=np.float32)
                iteri=iteriLast
        else:
            for p in range(len(len_month_a)):
                iteriLast=iteri + len_month_a[p]
                #  moyenne du mois année ordinaire
                # tempo_name[iterj, p, :, :] = np.mean(this_set_var[iteri:iteriLast, :, :] - 273,
                # axis=0, dtype=np.float32)
                netcdf_output.variables[var_outname][iterj, p, :, :] = \
                    np.mean(this_set_var[iteri:iteriLast, :, :] - 273, axis=0, dtype=np.float32)
                iteri=iteriLast
        #  max ou min de l'année
        if minmax == 'max':
            netcdf_output.variables[var_outname][iterj, len(len_month_a), :, :] = \
                np.amax(netcdf_output.variables[var_outname][iterj, 0:twelve_months, :, :], axis=0)
        elif minmax == 'min':
            netcdf_output.variables[var_outname][iterj, len(len_month_a), :, :] = \
                np.amin(netcdf_output.variables[var_outname][iterj, 0:twelve_months, :, :], axis=0)
        else:
            print('\nError on function min or max\n')

    print(var_outname, iterj, str(this_date), datetime.now())
    source_data_set.close()
    netcdf_output.close()

    return ii

# ========== Function add_vars_rstr_rstrc_to_set( ====================================


def add_vars_rstr_rstrc_to_set(jj, local_path, names):

    global twelve_months, len_month_a, len_month_b
    global first_year, size_years, size_months, size_grid_j, size_grid_i
    drias_filename = names[0]
    # Name of the rstr variable in the new NetCDF file
    var_outname_1 = names[1]
    # Name of the rstrc variable in the new netCDF file
    var_outname_2 = names[2]
    # Name of input variable from original file
    var_in = names[3]

    print('start : ', var_outname_1, var_outname_2, datetime.now())
    name_in = local_path + drias_filename
    name_out_1 = 'this_set_' + var_outname_1
    name_out_2 = 'this_set_' + var_outname_2
    # open source file
    source_data_set = nc.Dataset(name_in, mode='r', format="NETCDF4")

    this_set_var = source_data_set.variables[var_in]
    this_set_dates = source_data_set.variables['time']

    # create temp netCDF to host results (netcdf cannot be returned)
    try:
        os.remove(name_out_1)
    except OSError:
        pass
    netcdf_output_1 = nc.Dataset(name_out_1, mode='w', format='NETCDF4')
    try:
        os.remove(name_out_2)
    except OSError:
        pass
    netcdf_output_2 = nc.Dataset(name_out_2, mode='w', format='NETCDF4')
    netcdf_output_1.createDimension('i', size_grid_i)  # latitude axis
    netcdf_output_2.createDimension('i', size_grid_i)  # latitude axis
    netcdf_output_1.createDimension('j', size_grid_j)  # longitude axis
    netcdf_output_2.createDimension('j', size_grid_j)  # longitude axis
    netcdf_output_1.createDimension('month', size_months)  # month axis
    netcdf_output_2.createDimension('month', size_months)  # month axis
    netcdf_output_1.createDimension('year', size_years)  # year axis
    netcdf_output_2.createDimension('year', size_years)  # year axis

    netcdf_output_1.createVariable(var_outname_1, 'f4', ('year', 'month', 'j', 'i'), fill_value=1.e+20)
    netcdf_output_2.createVariable(var_outname_2, 'f4', ('year', 'month', 'j', 'i'), fill_value=1.e+20)
    for thisAttr in source_data_set.variables[var_in].ncattrs():
        netcdf_output_1.variables[var_outname_1].setncattr(thisAttr,
                                                                     source_data_set.variables[var_in].getncattr(thisAttr))
        netcdf_output_2.variables[var_outname_2].setncattr(thisAttr,
                                                     source_data_set.variables[var_in].getncattr(thisAttr))
    # The second variable : cumulated rain is added from the original file
    netcdf_output_2.variables[var_outname_2].setncattr('standard_name', 'large_scale_cumRainfall_flux')

    # date of the beginning of computations (num value)
    initial_year_date = nc.date2num(datetime(2006, 1, 1), this_set_dates.units)
    # date of the beginning of THIS study ( num value >  initial_year_date)
    first_year_date = nc.date2num(datetime(first_year, 1, 1), this_set_dates.units)
    # print('shape', this_set_dates.shape)
    # print ('initial_year_date first_year_date', initial_year_date, first_year_date)
    # iterj varies from 0 to sizeyears
    start_year = 0
    stop_year = size_years
    iteri = 0
    yeardate = initial_year_date
    while yeardate < first_year_date:
        iteri += 1
        yeardate += 24  # 24 hours a day ! we are counting in hours
    #     this_date=nc.num2date(this_set_dates[iteri], this_set_dates.units).strftime("%c")
    #     print('iteri this_date', iteri, this_date)
    #     print('début -> fin, iteri= : ', start_year, stop_year, iteri)
    #     print ('theseDates[iteri] :', theseDates[iteri], theseDates[100], theseDates[1000])

    for iterj in  range(start_year, stop_year) :

        this_date=nc.num2date(this_set_dates[iteri], this_set_dates.units).strftime("%c")
        this_year=int(nc.num2date(this_set_dates[iteri], this_set_dates.units).strftime("%Y"))
        #
        #  Compteur permettant de suivre l'avancement du calcul et le fait que chaque année
        #  commence bien le 1er janvier à 00h00 (suivi correct des mois et années bissextiles)
        #
        # print('\r', var_outname_1, iterj, str(this_date), end="")
        if this_year % 4 == 0:
            for p in range(len(len_month_b)):
                iteriLast = iteri + len_month_b[p]
                #  moyenne du mois année bissextile
                # tempo_name[iterj, p, :, :] = np.mean(this_set_var[iteri:iteriLast, :, :] - 273,
                # axis=0, dtype=np.float32)
                netcdf_output_1.variables[var_outname_1][iterj, p, :, :] = np.mean(
                    this_set_var[iteri:iteriLast, :, :], axis=0, dtype=np.float32)
                netcdf_output_2.variables[var_outname_2][iterj, p, :, :] = np.sum(
                    this_set_var[iteri:iteriLast, :, :], axis=0, dtype=np.float32)
                iteri = iteriLast
        else:
            for p in range(len(len_month_a)):
                iteriLast = iteri + len_month_a[p]
                #  moyenne du mois année ordinaire
                # tempo_name[iterj, p, :, :] = np.mean(this_set_var[iteri:iteriLast, :, :] - 273,
                # axis=0, dtype=np.float32)
                netcdf_output_1.variables[var_outname_1][iterj, p, :, :] = np.mean(
                    this_set_var[iteri:iteriLast, :, :], axis=0, dtype=np.float32)
                netcdf_output_2.variables[var_outname_2][iterj, p, :, :] = np.sum(
                    this_set_var[iteri:iteriLast, :, :], axis=0, dtype=np.float32)
                iteri = iteriLast
        #  somme de l'année
        netcdf_output_1.variables[var_outname_1][iterj, len(len_month_a), :, :] = np.mean(
            netcdf_output_1.variables[var_outname_1][iterj, 0:twelve_months, :, :], axis=0)
        netcdf_output_2.variables[var_outname_2][iterj, len(len_month_a), :, :] = np.sum(
            netcdf_output_2.variables[var_outname_2][iterj, 0:twelve_months, :, :], axis=0)

    print(var_outname_1, iterj, str(this_date), datetime.now())
    source_data_set.close()
    netcdf_output_1.close()
    netcdf_output_2.close()

    return jj

# ========================= Function add_var_delta_t_to_set ================================


def add_var_delta_t_to_set(kk, local_path, names):

    global twelve_months, len_month_a, len_month_b
    global first_year, size_years, size_months, size_grid_j, size_grid_i
    drias_filename_1 = names[0]
    # Name of first input variable from original file
    var_in_1 = names[1]
    drias_filename_2 = names[2]
    # Name of second input variable from original file
    var_in_2 = names[3]
    # Name of the rstr variable in the new NetCDF file
    var_outname = names[4]

    print('start : ', var_outname, datetime.now())
    name_in_1 = local_path + drias_filename_1
    name_in_2 = local_path + drias_filename_2
    name_out = 'this_set_' + var_outname
    # open source file for one parameter
    source_data_set_1 = nc.Dataset(name_in_1, mode='r', format="NETCDF4")
    source_data_set_2 = nc.Dataset(name_in_2, mode='r', format="NETCDF4")

    this_set_var_1 = source_data_set_1.variables[var_in_1]
    this_set_var_2 = source_data_set_2.variables[var_in_2]
    this_set_dates = source_data_set_1.variables['time']

    # create temp netCDF to host results (netcdf cannot be returned)
    try:
        os.remove(name_out)
    except OSError:
        pass
    netcdf_output = nc.Dataset(name_out, mode='w', format='NETCDF4')
    netcdf_output.createDimension('i', size_grid_i)  # latitude axis
    netcdf_output.createDimension('j', size_grid_j)  # longitude axis
    netcdf_output.createDimension('month', size_months)  # month axis
    netcdf_output.createDimension('year', size_years)  # year axis

    netcdf_output.createVariable(var_outname, 'f4', ('year', 'month', 'j', 'i'), fill_value=1.e+20)
    for thisAttr in source_data_set_1.variables[var_in_1].ncattrs():
        netcdf_output.variables[var_outname].setncattr(thisAttr,
                                                       source_data_set_1.variables[var_in_1].getncattr(thisAttr))
    # As we deal with temperatures that we are converting from K to C we change the "units" value
    # if var == 'tasmin' or var == 'tasmax':
    netcdf_output.variables[var_outname].setncattr('units', 'degree C')

    # date of the beginning of computations (num value)
    initial_year_date = nc.date2num(datetime(2006, 1, 1), this_set_dates.units)
    # date of the beginning of THIS study ( num value >  initial_year_date)
    first_year_date = nc.date2num(datetime(first_year, 1, 1), this_set_dates.units)
    # print('shape', this_set_dates.shape)
    # print ('initial_year_date first_year_date', initial_year_date, first_year_date)
    # iterj varies from 0 to sizeyears
    start_year = 0
    stop_year = size_years
    iteri = 0
    yeardate = initial_year_date
    while yeardate < first_year_date:
        iteri += 1
        yeardate += 24  # 24 hours a day ! we are counting in hours
    #     this_date=nc.num2date(this_set_dates[iteri], this_set_dates.units).strftime("%c")
    #     print('iteri this_date', iteri, this_date)
    #     print('début -> fin, iteri= : ', start_year, stop_year, iteri)
    #     print ('theseDates[iteri] :', theseDates[iteri], theseDates[100], theseDates[1000])

    for iterj in range(start_year, stop_year):

        this_date = nc.num2date(this_set_dates[iteri], this_set_dates.units).strftime("%c")
        this_year = int(nc.num2date(this_set_dates[iteri], this_set_dates.units).strftime("%Y"))
        #
        #  Compteur permettant de suivre l'avancement du calcul et le fait que chaque année
        #  commence bien le 1er janvier à 00h00 (suivi correct des mois et années bissextiles)
        #
        # print('\r', var_outname, iterj, str(this_date), end="")
        if this_year % 4 == 0:
            for p in range(len(len_month_b)):
                iteriLast = iteri + len_month_b[p]
                #  moyenne du mois année bissextile
                # tempo_name[iterj, p, :, :] = np.mean(this_set_var[iteri:iteriLast, :, :] - 273,
                # axis=0, dtype=np.float32)
                netcdf_output.variables[var_outname][iterj, p, :, :] = \
                    np.mean((this_set_var_1[iteri:iteriLast, :, :] - this_set_var_2[iteri:iteriLast, :, :]),
                            axis=0, dtype=np.float32)
                iteri = iteriLast
        else:
            for p in range(len(len_month_a)):
                iteriLast = iteri + len_month_a[p]
                #  moyenne du mois année ordinaire
                # tempo_name[iterj, p, :, :] = np.mean(this_set_var[iteri:iteriLast, :, :] - 273,
                # axis=0, dtype=np.float32)
                netcdf_output.variables[var_outname][iterj, p, :, :] = \
                    np.mean((this_set_var_1[iteri:iteriLast, :, :] - this_set_var_2[iteri:iteriLast, :, :]),
                            axis=0, dtype=np.float32)
                iteri = iteriLast
        #  moyenne de l'année
        netcdf_output.variables[var_outname][iterj, len(len_month_a), :, :] = \
            np.mean(netcdf_output.variables[var_outname][iterj, 0:twelve_months, :, :], axis=0)

    print(var_outname, iterj, str(this_date), datetime.now())
    source_data_set_1.close()
    source_data_set_2.close()
    netcdf_output.close()

    return kk


# =================================== collect_resultii ========


# def collect_resultii(result):
    # global resultii
#     resultii.append(result)

# =================================== collect_resultjj ========


# def collect_resultjj(result):
    # global resultjj
#     resultjj.append(result)

# =================================== collect_resultkk ========


# def collect_resultkk(result):
    # global resultkk
 #    resultkk.append(result)

# ============================== Listes des fichiers à traiter ==================================


namesT = [['tasmax_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc', 't_max_26', 'tasmax', 'max'],
          ['tasmax_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc', 't_max_45', 'tasmax', 'max'],
          ['tasmax_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc', 't_max_85', 'tasmax', 'max'],
          ['tasmin_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc', 't_min_26', 'tasmin', 'min'],
          ['tasmin_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc', 't_min_45', 'tasmin', 'min'],
          ['tasmin_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc', 't_min_85', 'tasmin', 'min']]

namesP = [['rstr_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc', 'rstr_26', 'rstrc_26', 'rstr'],
          ['rstr_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc', 'rstr_45', 'rstrc_45', 'rstr'],
          ['rstr_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc', 'rstr_85', 'rstrc_85', 'rstr']]

namesDT = [['tasmax_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc', 'tasmax',
            'tasmin_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc', 'tasmin', 'delta_t_26'],
          ['tasmax_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc', 'tasmax',
           'tasmin_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc', 'tasmin', 'delta_t_45'],
          ['tasmax_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc', 'tasmax',
           'tasmin_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc', 'tasmin', 'delta_t_85']]

ii = 0
jj = 0
kk = 0

# ======================================= boucle linéaire =========================================

for ii, names in enumerate(namesT):
    add_var_temp_to_set(ii, path, names)
for jj, names in enumerate(namesP):
    add_vars_rstr_rstrc_to_set(kk, path, names)
for kk, names in enumerate(namesDT):
    add_var_delta_t_to_set(kk, path, names)
# ======================================= /boucle linéaire =========================================

# ======================================= boucle parallèle =========================================
# resultii = []
# resultjj = []
# resultkk = []
# pool = mp.Pool(mp.cpu_count())
# print("Number of processors: ", mp.cpu_count())

#  Températures
# for ii, names in enumerate(namesT):
#     pool.apply_async(add_var_temp_to_set, args=(ii, path, names), callback=collect_resultii)
# Précipitations  et précipitations cumulées
# for jj, names in enumerate(namesP):
#     pool.apply_async(add_vars_rstr_rstrc_to_set, args=(kk, path, names), callback=collect_resultjj)

# pool.close()
# pool.join()
# A second pool is necessary to prevent parallel computing messing up values from same source
# in multple computations
# pool = mp.Pool(mp.cpu_count())
# print("Number of processors: ", mp.cpu_count())

# deltaT quotidien
# for kk, names in enumerate(namesDT):
#     pool.apply_async(add_var_delta_t_to_set, args=(kk, path, names), callback=collect_resultkk)

# pool.close()
# pool.join()
# ======================================= /boucle parallèle =========================================

for ii, names in enumerate(namesT):

    # source file name
    drias_filename = names[0]
    # output name of the variable processed
    var_outname_1 = names[1]
    # input name of the variable processed
    var_in = names[2]
    # input filename
    name_in = path + drias_filename
    # output filename
    name_out = 'this_set_' + var_outname_1

    # open input original file
    source_data_set = nc.Dataset(name_in, mode='r', format="NETCDF4")
    # open as input the file bearing results of the previous calculation
    data_set_1 = nc.Dataset(name_out, mode='r', format="NETCDF4")
    # add the processed variable to the output file
    extract_params_year_month.createVariable(var_outname_1, 'f4', ('year', 'month', 'j', 'i'), fill_value=1.e+20)

    # retrieve variable attribute values from original file and write them to output
    for thisAttr in source_data_set.variables[var_in].ncattrs():
        extract_params_year_month.variables[var_outname_1].setncattr(thisAttr,
                                                                     source_data_set.variables[var_in].getncattr(thisAttr))
    # As we deal with temperatures that we are converting from K to C we change the "units" value
    # if var == 'tasmin' or var == 'tasmax':
    extract_params_year_month.variables[var_outname_1].setncattr('units', 'degree C')
    extract_params_year_month.variables[var_outname_1][:] = data_set_1.variables[var_outname_1][:]

for jj, names in enumerate(namesP):

    # source file name
    drias_filename = names[0]
    # Name of the rstr variable in the new NetCDF file
    var_outname_1 = names[1]
    # Name of the rstrc variable in the new netCDF file
    var_outname_2 = names[2]
    # Name of input variable from original file
    var_in = names[3]
    # input filename
    name_in = path + drias_filename
    # output 1 filename
    name_out_1 = 'this_set_' + var_outname_1
    # output 2 filename
    name_out_2 = 'this_set_' + var_outname_2

    # open input original file
    source_data_set = nc.Dataset(name_in, mode='r', format="NETCDF4")
    # open as input the file bearing results of the previous calculation
    data_set_1 = nc.Dataset(name_out_1, mode='r', format="NETCDF4")
    data_set_2 = nc.Dataset(name_out_2, mode='r', format="NETCDF4")
    # add the processed variable to the output file
    extract_params_year_month.createVariable(var_outname_1, 'f4', ('year', 'month', 'j', 'i'), fill_value=1.e+20)
    extract_params_year_month.createVariable(var_outname_2, 'f4', ('year', 'month', 'j', 'i'), fill_value=1.e+20)

    # retrieve variable attribute values from original file and write them to output
    for thisAttr in source_data_set.variables[var_in].ncattrs():
        extract_params_year_month.variables[var_outname_1].setncattr(thisAttr,
                                                                     source_data_set.variables[var_in].getncattr(thisAttr))
        extract_params_year_month.variables[var_outname_2].setncattr(thisAttr,
                                                                     source_data_set.variables[var_in].getncattr(thisAttr))
    # The second variable : cumulated rain is added from the original file
    extract_params_year_month.variables[var_outname_1].setncattr('standard_name', 'largescale_cumRainfall_flux')
    extract_params_year_month.variables[var_outname_1][:] = data_set_1.variables[var_outname_1][:]
    extract_params_year_month.variables[var_outname_2][:] = data_set_2.variables[var_outname_2][:]

for kk, names in enumerate(namesDT):

    # source file name
    drias_filename = names[0]
    # Name of first input variable from original file
    var_in = names[1]
    # output name of the variable processed
    var_outname_1 = names[4]
    # input filename
    name_in = path + drias_filename
    # output filename
    name_out = 'this_set_' + var_outname_1

    # open input original file
    source_data_set = nc.Dataset(name_in, mode='r', format="NETCDF4")
    # open as input the file bearing results of the previous calculation
    data_set_1 = nc.Dataset(name_out, mode='r', format="NETCDF4")
    # add the processed variable to the output file
    extract_params_year_month.createVariable(var_outname_1, 'f4', ('year', 'month', 'j', 'i'), fill_value=1.e+20)
    for thisAttr in source_data_set.variables[var_in].ncattrs():
        extract_params_year_month.variables[var_outname_1].setncattr(thisAttr,
                                                                     source_data_set.variables[var_in].getncattr(thisAttr))
    # As we deal with difference of temperatures  we change the "units" value
    extract_params_year_month.variables[var_outname_1].setncattr('units', 'degree C')
    extract_params_year_month.variables[var_outname_1][:] = data_set_1.variables[var_outname_1][:]


# print ('\n\nextract_params_year_month contents :', extract_params_year_month.variables['t_max_26'][:])

extract_params_year_month.close()
