import netCDF4 as nc
import numpy as np
from datetime import datetime
from array import array
import sys, os
import multiprocessing as mp

tMax_26_Lyon = nc.Dataset('http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/tasmax_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc',mode='r', format="NETCDF4",diskless=False)
lyon_tmax_26 = tMax_26_Lyon.variables['tasmax']
#print('Structure et taille du tableau exporté :\n\t',tMax_26_Lyon.variables['tasmax'].dimensions, lyon_tmax_26.shape)
tMax_45_Lyon = nc.Dataset('http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/tasmax_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc',mode='r', format="NETCDF4",diskless=False)
lyon_tmax_45 = tMax_45_Lyon.variables['tasmax']
#print('Structure et taille du tableau exporté :\n\t',tMax_45_Lyon.variables['tasmax'].dimensions, lyon_tmax_45.shape)
#print('\nPremière ligne de données :\n',lyon_temp_45[0,0,:])
#print('Dernière ligne de données :\n',lyon_temp_45[-1,-1,:])
tMax_85_Lyon = nc.Dataset('http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/tasmax_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc',mode='r', format="NETCDF4",diskless=False)
lyon_tmax_85 = tMax_85_Lyon.variables['tasmax']
#print('Structure et taille du tableau exporté :\n\t',tMax_85_Lyon.variables['tasmax'].dimensions, lyon_tmax_85.shape)
#print('\nPremière ligne de données :\n',lyon_temp_85[0,0,:])
#print('Dernière ligne de données :\n',lyon_temp_85[-1,-1,:])
tMin_26_Lyon = nc.Dataset('http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/tasmin_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc',mode='r', format="NETCDF4",diskless=False)
lyon_tmin_26 = tMin_26_Lyon.variables['tasmin']
#print('Structure et taille du tableau exporté :\n\t',tMin_26_Lyon.variables['tasmin'].dimensions, lyon_tmin_26.shape)
#print('\nPremière ligne de données :\n',lyon_temp_26[0,0,:])
#print('Dernière ligne de données :\n',lyon_temp_26[-1,-1,:])
tMin_45_Lyon = nc.Dataset('http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/tasmin_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc',mode='r', format="NETCDF4",diskless=False)
lyon_tmin_45 = tMin_45_Lyon.variables['tasmin']
#print('Structure et taille du tableau exporté :\n\t',tMin_45_Lyon.variables['tasmin'].dimensions, lyon_tmin_45.shape)
#print('\nPremière ligne de données :\n',lyon_temp_45[0,0,:])
#print('Dernière ligne de données :\n',lyon_temp_45[-1,-1,:])
tMin_85_Lyon = nc.Dataset('http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/tasmin_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc',mode='r', format="NETCDF4",diskless=False)
lyon_tmin_85 = tMin_85_Lyon.variables['tasmin']
#print('Structure et taille du tableau exporté :\n\t',tMin_85_Lyon.variables['tasmin'].dimensions, lyon_tmin_85.shape)
#print('\nPremière ligne de données :\n',lyon_temp_85[0,0,:])
#print('Dernière ligne de données :\n',lyon_temp_85[-1,-1,:])
rstr_26_Lyon = nc.Dataset('http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/rstr_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc',mode='r', format="NETCDF4",diskless=False)
lyon_rstr_26 = rstr_26_Lyon.variables['rstr'][:]
#print('Structure et taille du tableau exporté :\n\t',rstr_26_Lyon.variables['rstr'].dimensions, lyon_rstr_26.shape)
#print('\nPremière ligne de données :\n',lyon_rstr_26[0,0,:])
#print('Dernière ligne de données :\n',lyon_rstr_26[-1,-1,:])
rstr_45_Lyon = nc.Dataset('http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/rstr_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc',mode='r', format="NETCDF4",diskless=False)
lyon_rstr_45 = rstr_45_Lyon.variables['rstr'][:]
#print('Structure et taille du tableau exporté :\n\t',rstr_45_Lyon.variables['rstr'].dimensions, lyon_rstr_45.shape)
#print('\nPremière ligne de données :\n',lyon_rstr_45[0,0,:])
#print('Dernière ligne de données :\n',lyon_rstr_45[-1,-1,:])
rstr_85_Lyon = nc.Dataset('http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/rstr_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc',mode='r', format="NETCDF4",diskless=False)
lyon_rstr_85 = rstr_85_Lyon.variables['rstr'][:]
#print('Structure et taille du tableau exporté :\n\t',rstr_85_Lyon.variables['rstr'].dimensions, lyon_rstr_85.shape)
#print('\nPremière ligne de données :\n',lyon_temp_26[0,0,:])
#print('Dernière ligne de données :\n',lyon_temp_26[-1,-1,:])

lyon_date = tMax_26_Lyon.variables['time']
print('Taille du tableau de dates - ',lyon_date.shape)
print('Date de début de la simulation : ',nc.num2date(lyon_date[0],lyon_date.units).strftime("%c"))
print('Date de fin de la simulation : ',nc.num2date(lyon_date[-1],lyon_date.units).strftime("%c"))
lyon_lat,lyon_lon = tMax_26_Lyon.variables['lat'], tMax_26_Lyon.variables['lon'] # latitude longitude
print('Emprise du projet en latititude-Longitude ;\n', lyon_lat[0][0],'#',lyon_lon[0][0],' :: ',lyon_lat[-1][-1],'#',lyon_lon[-1][-1])
lyon_x,lyon_y = tMax_26_Lyon.variables['x'], tMax_26_Lyon.variables['y']  # coordonnées métriques
print('Emprise du projet en x-y mètres ;\n', lyon_x[0],'#',lyon_y[0],' :: ',lyon_x[-1],'#',lyon_y[-1])
lyon_gridi,lyon_gridj = tMax_26_Lyon.variables['i'], tMax_26_Lyon.variables['j'] # coordonnées grille Aladin
print('Emprise du projet en noeuds ALADIN ;\n', lyon_gridi[0],'#',lyon_gridj[0],' :: ',lyon_gridi[-1],'#',lyon_gridj[-1])

try: os.remove('tmin-tmax-rstr_Lyon_26-45-85.nc')  # par sécurité efface le fichier portadatain = numpy.array(['foo','bar'],dtype='S3')nt ce nom ! attention aux pertes possibles
except OSError : pass
extractParamsYearMonth = nc.Dataset('tmin-tmax-rstr_Lyon_26-45-85.nc',mode='w',format='NETCDF4')

# tableau du nom des mois
listmonth = np.array(['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul',
            'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'All'])
# Tableau du nombre de jours par mois année ordinaire
lenMonthA =[31,28,31,30,31,30,31,31,30,31,30,31]
# Tableau du nombre de jours par mois année bissextile
lenMonthB =[31,29,31,30,31,30,31,31,30,31,30,31]
# Conversion en nombre entier des années extrêmes
#==============================================================
#==  Choix de la première et de la dernière année de l'étude ==
#==============================================================
firstYear = 2006
lastYear = 2100
#==============================================================
# Détermination de la taille des tableaux de calcul
sizeMonths = len(listmonth)
twelvMonths = sizeMonths - 1
sizeyears = lastYear - firstYear + 1
sizegridi = lyon_gridi.shape[0]
sizegridj = lyon_gridj.shape[0]
print ("DateTime de départ  de l'étude : ", firstYear,
      "\nDateTime de fin  de l'étude : ", lastYear,
      "\nDurée de l'étude : ", sizeyears, 'ans',
      "\nNb de mois dans l'année : ", twelvMonths)

#
# Création des dimensions du tableau mois et année sont séparés pour disjoindre les traitements sur ces variables
#
extractParamsYearMonth.createDimension('i', sizegridi)     # latitude axis
extractParamsYearMonth.createDimension('j', sizegridj)    # longitude axis
extractParamsYearMonth.createDimension('month', sizeMonths)    # month axis
extractParamsYearMonth.createDimension('year', sizeyears) # year axis

tmax_26 = extractParamsYearMonth.createVariable('tmax_26','f4',('year','month','j','i'),fill_value=1.e+20)
tmax_26.units = 'degree C' # degrees Celsius
tmax_26.standard_name = tMax_26_Lyon.variables['tasmax'].standard_name # this is a CF standard name
tmax_26.long_name = tMax_26_Lyon.variables['tasmax'].long_name
tmax_26.cell_methods = tMax_26_Lyon.variables['tasmax'].cell_methods
tmax_26.coordinates = tMax_26_Lyon.variables['tasmax'].coordinates
tmax_26.missing_value = tMax_26_Lyon.variables['tasmax'].missing_value
tmax_45 = extractParamsYearMonth.createVariable('tmax_45','f4',('year','month','j','i'),fill_value=1.e+20)
tmax_45.units = 'degree C' # degrees Celsius
tmax_45.standard_name = tMax_45_Lyon.variables['tasmax'].standard_name # this is a CF standard name
tmax_45.long_name = tMax_45_Lyon.variables['tasmax'].long_name
tmax_45.cell_methods = tMax_45_Lyon.variables['tasmax'].cell_methods
tmax_45.coordinates = tMax_45_Lyon.variables['tasmax'].coordinates
tmax_45.missing_value = tMax_45_Lyon.variables['tasmax'].missing_value
tmax_85 = extractParamsYearMonth.createVariable('tmax_85','f4',('year','month','j','i'),fill_value=1.e+20)
tmax_85.units = 'degree C' # degrees Celsius
tmax_85.standard_name = tMax_85_Lyon.variables['tasmax'].standard_name # this is a CF standard name
tmax_85.long_name = tMax_85_Lyon.variables['tasmax'].long_name
tmax_85.cell_methods = tMax_85_Lyon.variables['tasmax'].cell_methods
tmax_85.coordinates = tMax_85_Lyon.variables['tasmax'].coordinates
tmax_85.missing_value = tMax_85_Lyon.variables['tasmax'].missing_value
tmin_26 = extractParamsYearMonth.createVariable('tmin_26','f4',('year','month','j','i'),fill_value=1.e+20)
tmin_26.units = 'degree C' # degrees Celsius
tmin_26.standard_name = tMin_26_Lyon.variables['tasmin'].standard_name # this is a CF standard name
tmin_26.long_name = tMin_26_Lyon.variables['tasmin'].long_name
tmin_26.cell_methods = tMin_26_Lyon.variables['tasmin'].cell_methods
tmin_26.coordinates = tMin_26_Lyon.variables['tasmin'].coordinates
tmin_26.missing_value = tMin_26_Lyon.variables['tasmin'].missing_value
tmin_45 = extractParamsYearMonth.createVariable('tmin_45','f4',('year','month','j','i'),fill_value=1.e+20)
tmin_45.units = 'degree C' # degrees Celsius
tmin_45.standard_name = tMin_45_Lyon.variables['tasmin'].standard_name # this is a CF standard name
tmin_45.long_name = tMin_45_Lyon.variables['tasmin'].long_name
tmin_45.cell_methods = tMin_45_Lyon.variables['tasmin'].cell_methods
tmin_45.coordinates = tMin_45_Lyon.variables['tasmin'].coordinates
tmin_45.missing_value = tMin_45_Lyon.variables['tasmin'].missing_value
tmin_85 = extractParamsYearMonth.createVariable('tmin_85','f4',('year','month','j','i'),fill_value=1.e+20)
tmin_85.units = 'degree C' # degrees Celsius
tmin_85.standard_name = tMin_85_Lyon.variables['tasmin'].standard_name # this is a CF standard name
tmin_85.long_name = tMin_85_Lyon.variables['tasmin'].long_name
tmin_85.cell_methods = tMin_85_Lyon.variables['tasmin'].cell_methods
tmin_85.coordinates = tMin_85_Lyon.variables['tasmin'].coordinates
tmin_85.missing_value = tMin_85_Lyon.variables['tasmin'].missing_value
rstr_26 = extractParamsYearMonth.createVariable('rstr_26','f4',('year','month','j','i'),fill_value=1.e+20)
rstr_26.units = rstr_26_Lyon.variables['rstr'].units # mm par jour
rstr_26.standard_name = rstr_26_Lyon.variables['rstr'].standard_name # this is a CF standard name
rstr_26.long_name = rstr_26_Lyon.variables['rstr'].long_name
rstr_26.cell_methods = rstr_26_Lyon.variables['rstr'].cell_methods
rstr_26.coordinates = rstr_26_Lyon.variables['rstr'].coordinates
rstr_26.missing_value = rstr_26_Lyon.variables['rstr'].missing_value
rstr_45 = extractParamsYearMonth.createVariable('rstr_45','f4',('year','month','j','i'),fill_value=1.e+20)
rstr_45.units = rstr_45_Lyon.variables['rstr'].units # mm par jour
rstr_45.standard_name = rstr_45_Lyon.variables['rstr'].standard_name # this is a CF standard name
rstr_45.long_name = rstr_45_Lyon.variables['rstr'].long_name
rstr_45.cell_methods = rstr_45_Lyon.variables['rstr'].cell_methods
rstr_45.coordinates = rstr_45_Lyon.variables['rstr'].coordinates
rstr_45.missing_value = rstr_45_Lyon.variables['rstr'].missing_value
rstr_85 = extractParamsYearMonth.createVariable('rstr_85','f4',('year','month','j','i'),fill_value=1.e+20)
rstr_85.units = rstr_85_Lyon.variables['rstr'].units # mm par jour
rstr_85.standard_name = rstr_85_Lyon.variables['rstr'].standard_name # this is a CF standard name
rstr_85.long_name = rstr_85_Lyon.variables['rstr'].long_name
rstr_85.cell_methods = rstr_85_Lyon.variables['rstr'].cell_methods
rstr_85.coordinates = rstr_85_Lyon.variables['rstr'].coordinates
rstr_85.missing_value = rstr_85_Lyon.variables['rstr'].missing_value
rstrc_26 = extractParamsYearMonth.createVariable('rstrc_26','f4',('year','month','j','i'),fill_value=1.e+20)
rstrc_26.units = rstr_26_Lyon.variables['rstr'].units # mm par jour
rstrc_26.standard_name = 'largescale_cumRainfall_flux26' # this is a CF standard name
rstrc_26.long_name = rstr_26_Lyon.variables['rstr'].long_name
rstrc_26.cell_methods = rstr_26_Lyon.variables['rstr'].cell_methods
rstrc_26.coordinates = rstr_26_Lyon.variables['rstr'].coordinates
rstrc_26.missing_value = rstr_26_Lyon.variables['rstr'].missing_value
rstrc_45 = extractParamsYearMonth.createVariable('rstrc_45','f4',('year','month','j','i'),fill_value=1.e+20)
rstrc_45.units = rstr_45_Lyon.variables['rstr'].units # mm par jour
rstrc_45.long_name = rstr_45_Lyon.variables['rstr'].long_name
rstrc_45.cell_methods = rstr_45_Lyon.variables['rstr'].cell_methods
rstrc_45.coordinates = rstr_45_Lyon.variables['rstr'].coordinates
rstrc_45.missing_value = rstr_45_Lyon.variables['rstr'].missing_value
rstrc_45.standard_name = 'largescale_cumRainfall_flux_45' # this is a CF standard name
rstrc_85 = extractParamsYearMonth.createVariable('rstrc_85','f4',('year','month','j','i'),fill_value=1.e+20)
rstrc_85.units = rstr_85_Lyon.variables['rstr'].units # mm par jour
rstrc_85.standard_name = 'largescale_cumRainfall_flux_85' # this is a CF standard name
rstrc_85.long_name = rstr_85_Lyon.variables['rstr'].long_name
rstrc_85.cell_methods = rstr_85_Lyon.variables['rstr'].cell_methods
rstrc_85.coordinates = rstr_85_Lyon.variables['rstr'].coordinates
rstrc_85.missing_value = rstr_85_Lyon.variables['rstr'].missing_value
extractParamsYearMonth.title = 'Extrait TSMax par moyenne mensuelle de 2006 a 2100 Lyon et sa region'
extractParamsYearMonth.institution = 'ENS de Lyon'
extractParamsYearMonth.institute_id = 'IFE Institut Francais de l Education'
extractParamsYearMonth.project_id = 'Climat et meteo tremplin pour l enseignement des sciences'
extractParamsYearMonth.model_id = 'CNRM-ALADIN52'
extractParamsYearMonth.product = 'output derived from Meteofrance DRIAS data'
extractParamsYearMonth.contact = 'gerard.vidal@ens-lyon.fr'
extractParamsYearMonth.creation_date = str(datetime.now())
extractParamsYearMonth.driving_experiment_name = 'DRIAS2014'
extractParamsYearMonth.experiment = 'RCP2.6 RCP4.5 RCP8.5 '
extractParamsYearMonth.model = 'ALADIN-Climat'
extractParamsYearMonth.author = 'Gerard Vidal'
extractParamsYearMonth.comment = "Extraction des moyennes de la region Lyonnaise de 2006 a 2100 et changegement des variables"

# Define two variables with the same names as dimensions,
# a conventional way to define "coordinate variables".
i = extractParamsYearMonth.createVariable('i', 'i4', ('i',))
i.long_name =  tMax_26_Lyon.variables['i'].long_name
j = extractParamsYearMonth.createVariable('j', 'i4', ('j',))
j.long_name = tMax_26_Lyon.variables['j'].long_name
lat = extractParamsYearMonth.createVariable('lat', 'f4', ('j','i'))
lat.units = tMax_26_Lyon.variables['lat'].units
lat.long_name =  tMax_26_Lyon.variables['lat'].long_name
lat.standard_name =  tMax_26_Lyon.variables['lat'].standard_name
lat._CoordinateAxisType =  tMax_26_Lyon.variables['lat']._CoordinateAxisType
lon = extractParamsYearMonth.createVariable('lon', 'f4', ('j','i',))
lon.units =  tMax_26_Lyon.variables['lon'].units
lon.long_name = tMax_26_Lyon.variables['lon'].long_name
lon.standard_name = tMax_26_Lyon.variables['lon'].standard_name
lon._CoordinateAxisType = tMax_26_Lyon.variables['lat']._CoordinateAxisType
x = extractParamsYearMonth.createVariable('x', 'i4', ('i',))
x.units = tMax_26_Lyon.variables['x'].units
x.long_name = tMax_26_Lyon.variables['x'].long_name
x.standard_name = tMax_26_Lyon.variables['x'].standard_name
y = extractParamsYearMonth.createVariable('y', 'i4', ('j',))
y.units = tMax_26_Lyon.variables['y'].units
y.long_name = tMax_26_Lyon.variables['y'].long_name
y.standard_name = tMax_26_Lyon.variables['y'].standard_name
month = extractParamsYearMonth.createVariable('month','S3', ('month',))
month.units = 'month'
month.long_name = 'month_name'
month.standard_name = 'month_name'
year = extractParamsYearMonth.createVariable('year', 'u4', ('year',))
year.units = 'date'
year.long_name = 'year'
year.standard_name = 'year'
# Filling variables with  source data
i[:] = lyon_gridi[:]
j[:] = lyon_gridj[:]
lat[:] = lyon_lat[:,:]
lon[:] = lyon_lon[:,:]
x[:] = lyon_x[:]
y[:] = lyon_y[:]
# Filling new variables
month[:] = listmonth[:]
dd = 0
for date in range (firstYear,lastYear+1) :
    year[dd] = date
    dd +=1

tMax_26_Lyon.close()
tMax_45_Lyon.close()
tMax_85_Lyon.close()
tMin_26_Lyon.close()
tMin_45_Lyon.close()
tMin_85_Lyon.close()
rstr_26_Lyon.close()
rstr_45_Lyon.close()
rstr_85_Lyon.close()

#========== Function addFileMeanTKToSet ====================================
def addFileMeanTKToSet (ii, localPath, names) :

    global extractParamsYearMonth, twelvMonths, lenMonthA,lenMonthB
    global firstYear, sizeyears, sizeMonths, sizegridj, sizegridi
    driasFileName = names[0]
    tempoName = names[1]
    varOutName = names[2]
    var = names[3]
    minmax = names[4]
    nameIn = localPath + driasFileName
    lyonDataSet = nc.Dataset(nameIn,mode='r', format="NETCDF4")
    selectVar = lyonDataSet.variables[var]
    tempoName = np.zeros((sizeyears,sizeMonths,sizegridj,sizegridi), dtype=np.float32)
    thisSetDates = lyonDataSet.variables['time']
    initialYearDate = nc.date2num(datetime(2006,1,1),thisSetDates.units)
    firstYearDate = nc.date2num(datetime(firstYear,1,1),thisSetDates.units)
#    print('shape',thisSetDates.shape)
#    print ('initialYearDate firstYearDate',initialYearDate,firstYearDate)
    startYear = 0
    stopYear = sizeyears
    iteri = 0
    yeardate = initialYearDate
    while yeardate < firstYearDate :
        iteri +=  1
        yeardate += 24 # 24 hours a day ! we are counting in hours
    iteriFirst = iteri
#    thisDate = nc.num2date(thisSetDates[iteri],thisSetDates.units).strftime("%c")
#    print('iteri thisDate',iteri,thisDate)
#    print('début -> fin, iterifirst : ', startYear,stopYear,iteriFirst)
#    print ('theseDates[iteri] :', theseDates[iteri], theseDates[100],theseDates[1000])

    for iterj in  range(startYear,stopYear) :

        thisDate = nc.num2date(thisSetDates[iteri],thisSetDates.units).strftime("%c")
        thisYear = int(nc.num2date(thisSetDates[iteri],thisSetDates.units).strftime("%Y"))
        #
        # Compteur permettant de suivre l'avancement du calcul et le fait que chaque année
        # commence bien le 1er janvier à 00h00 (suivi correct des mois et années bissextiles)
        #
        print('\r',varOutName, iterj,str(thisDate), end = "")
        if (thisYear % 4 == 0) :
            for p in range(len(lenMonthB)) :
                iteriLast = iteri + lenMonthB[p]
                # moyenne du mois année bissextile
                tempoName[iterj,p,:,:] = np.mean(selectVar[iteri:iteriLast,:,:] - 273, axis=0,dtype=np.float32)
                iteri = iteriLast
        else :
            for p in range(len(lenMonthA)) :
                iteriLast = iteri + lenMonthA[p]
                # moyenne du mois année ordinaire
                tempoName[iterj,p,:,:] = np.mean(selectVar[iteri:iteriLast,:,:] - 273, axis=0,dtype=np.float32)
                iteri = iteriLast
        # max ou min de l'année
        if minmax == 'max' :
            tempoName[iterj,len(lenMonthA),:,:] = np.amax(tempoName[iterj,0:twelvMonths,:,:], axis=0)
        elif minmax == 'min' :
            tempoName[iterj,len(lenMonthA),:,:] = np.amin(tempoName[iterj,0:twelvMonths,:,:], axis=0)
        else :
            print('\nError on function min or max\n')
        #print('\n',tempoName[iterj,len(lenMonthA),5,5])

    lyonDataSet.close()
    #print ('\nend', tempoName[:])
    #print ('\nend', tempoName)
    return(ii, tempoName)

#==========Function addFileMeanTCToSet ====================================
def addFileMeanTCToSet (jj, localPath, names) :

    global extractParamsYearMonth, twelvMonths, lenMonthA,lenMonthB
    global firstYear, sizeyears, sizeMonths, sizegridj, sizegridi
    driasFileName = names[0]
    tempoName = names[1]
    varOutName = names[2]
    var = names[3]
    nameIn = localPath + driasFileName
    lyonDataSet = nc.Dataset(nameIn,mode='r', format="NETCDF4")
    selectVar = lyonDataSet.variables[var]
    tempoName = np.zeros((sizeyears,sizeMonths,sizegridj,sizegridi), dtype=np.float32)
    thisSetDates = lyonDataSet.variables['time']
    initialYearDate = nc.date2num(datetime(2006,1,1),thisSetDates.units)
    firstYearDate = nc.date2num(datetime(firstYear,1,1),thisSetDates.units)
#    print('shape',thisSetDates.shape)
#    print ('initialYearDate firstYearDate',initialYearDate,firstYearDate)
    startYear = 0
    stopYear = sizeyears
    iteri = 0
    yeardate = initialYearDate
    while yeardate < firstYearDate :
        iteri +=  1
        yeardate += 24 # 24 hours a day ! we are counting in hours
    iteriFirst = iteri
#    thisDate = nc.num2date(thisSetDates[iteri],thisSetDates.units).strftime("%c")
#    print('iteri thisDate',iteri,thisDate)
#    print('début -> fin, iterifirst : ', startYear,stopYear,iteriFirst)
#    print ('theseDates[iteri] :', theseDates[iteri], theseDates[100],theseDates[1000])

    for iterj in  range(startYear,stopYear) :

        thisDate = nc.num2date(thisSetDates[iteri],thisSetDates.units).strftime("%c")
        thisYear = int(nc.num2date(thisSetDates[iteri],thisSetDates.units).strftime("%Y"))
        #
        # Compteur permettant de suivre l'avancement du calcul et le fait que chaque année
        # commence bien le 1er janvier à 00h00 (suivi correct des mois et années bissextiles)
        #
        print('\r',varOutName, iterj,str(thisDate), end = "")
        if (thisYear % 4 == 0) :
            for p in range(len(lenMonthB)) :
                iteriLast = iteri + lenMonthB[p]
                # moyenne du mois année bissextile
                tempoName[iterj,p,:,:] = np.mean(selectVar[iteri:iteriLast,:,:], axis=0,dtype=np.float32)
                iteri = iteriLast
        else :
            for p in range(len(lenMonthA)) :
                iteriLast = iteri + lenMonthA[p]
                # moyenne du mois année ordinaire
                tempoName[iterj,p,:,:] = np.mean(selectVar[iteri:iteriLast,:,:], axis=0,dtype=np.float32)
                iteri = iteriLast
        # moyenne de l'année
        tempoName[iterj,len(lenMonthA),:,:] = np.mean(tempoName[iterj,0:twelvMonths,:,:], axis=0)
        #print('\n',tempoName[iterj,len(lenMonthA),5,5])

    lyonDataSet.close()
    #print ('\nend', tempoName[:])
    #print ('\nend', tempoName)
    return(jj, tempoName[:])

#==========Function addFileSumToSet ====================================
def addFileSumToSet (kk, localPath, names) :

    global extractParamsYearMonth, twelvMonths, lenMonthA,lenMonthB
    global firstYear, sizeyears, sizeMonths, sizegridj, sizegridi
    driasFileName = names[0]
    tempoName = names[1]
    varOutName = names[2]
    var = names[3]
    nameIn = localPath + driasFileName
    lyonDataSet = nc.Dataset(nameIn,mode='r')
    selectVar = lyonDataSet.variables[var]
    tempoName = np.zeros((sizeyears,sizeMonths,sizegridj,sizegridi), dtype=np.float32)
    thisSetDates = lyonDataSet.variables['time']
    initialYearDate = nc.date2num(datetime(2006,1,1),thisSetDates.units)
    firstYearDate = nc.date2num(datetime(firstYear,1,1),thisSetDates.units)
#    print('shape',thisSetDates.shape)
#    print ('initialYearDate firstYearDate',initialYearDate,firstYearDate)
    startYear = 0
    stopYear = sizeyears
    iteri = 0
    yeardate = initialYearDate
    while yeardate < firstYearDate :
        iteri +=  1
        yeardate += 24 # 24 hours a day ! we are counting in hours
    iteriFirst = iteri
#    thisDate = nc.num2date(thisSetDates[iteri],thisSetDates.units).strftime("%c")
#    print('iteri thisDate',iteri,thisDate)
#    print('début -> fin, iterifirst : ', startYear,stopYear,iteriFirst)
#    print ('theseDates[iteri] :', theseDates[iteri], theseDates[100],theseDates[1000])

    for iterj in  range(startYear,stopYear) :

        thisDate = nc.num2date(thisSetDates[iteri],thisSetDates.units).strftime("%c")
        thisYear = int(nc.num2date(thisSetDates[iteri],thisSetDates.units).strftime("%Y"))
        #
        # Compteur permettant de suivre l'avancement du calcul et le fait que chaque année
        # commence bien le 1er janvier à 00h00 (suivi correct des mois et années bissextiles)
        #
        print('\r',varOutName, iterj,str(thisDate), end = "")
        if (thisYear % 4 == 0) :
            for p in range(len(lenMonthB)) :
                iteriLast = iteri + lenMonthB[p]
                # moyenne du mois année bissextile
                tempoName[iterj,p,:,:] = np.sum(selectVar[iteri:iteriLast,:,:], axis=0,dtype=np.float32)
                iteri = iteriLast
        else :
            for p in range(len(lenMonthA)) :
                iteriLast = iteri + lenMonthA[p]
                # moyenne du mois année ordinaire
                tempoName[iterj,p,:,:] = np.sum(selectVar[iteri:iteriLast,:,:], axis=0,dtype=np.float32)
                iteri = iteriLast
        # somme de l'année
        tempoName[iterj,len(lenMonthA),:,:] = np.sum(tempoName[iterj,0:twelvMonths,:,:], axis=0)
        #print('\n',tempoName[iterj,len(lenMonthA),5,5])

    lyonDataSet.close()
    #print ('\nend', tempoName[:])
    #print ('\nend', tempoName)
    return(kk, tempoName[:])

#=================================== collect_resultii ========
def collect_resultii(result) :
    #global resultii
    resultii.append(result)

#=================================== collect_resultjj ========
def collect_resultjj(result) :
    #global resultjj
    resultjj.append(result)

#=================================== collect_resultkk ========
def collect_resultkk(result) :
    #global resultkk
    resultkk.append(result)

#============================== Listes des fichiers à traiter ==================================

path = 'http://geoloc-tremplin.ens-lyon.fr/climato-data/Lyon-1/'

namesT = [['tasmax_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc','tempMax26','tmax_26','tasmax','max'],
          ['tasmax_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc','tempMax45','tmax_45','tasmax','max'],
          ['tasmax_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc','tempMax85','tmax_85','tasmax','max'],
          ['tasmin_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc','tempMin26','tmin_26','tasmin','min'],
          ['tasmin_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc','tempMin45','tmin_45','tasmin','min'],
          ['tasmin_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc','tempMin85','tmin_85','tasmin','min']]

namesP = [['rstr_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc','Rstr26','rstr_26','rstr'],
          ['rstr_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc','Rstr45','rstr_45','rstr'],
          ['rstr_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc','Rstr85','rstr_85','rstr']]

namesPC = [['rstr_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc','Rstrc26','rstrc_26','rstr'],
          ['rstr_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc','Rstrc45','rstrc_45','rstr'],
          ['rstr_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc','Rstrc85','rstrc_85','rstr']]

#======================================= boucle parallèle =========================================
resultii = []
resultjj = []
resultkk = []
# températures
pool = mp.Pool(mp.cpu_count())
for ii, names in enumerate(namesT) :
    pool.apply_async(addFileMeanTKToSet, args=(ii, path, names) , callback = collect_resultii)
rangeii = ii + 1
#Precipitations
for jj, names in enumerate(namesP) :
    pool.apply_async(addFileMeanTCToSet, args=(jj, path, names) , callback = collect_resultjj)
rangejj = jj + 1
#Précipitations cumulées
for kk, names in enumerate(namesPC) :
    pool.apply_async(addFileSumToSet, args=(kk, path, names) , callback = collect_resultkk)
rangekk = kk + 1
#================================================================
pool.close()
pool.join()

for resi in range(rangeii) :
    #print('\n', resultii[resi][0],namesT[resultii[resi][0]][2])
    #print(extractParamsYearMonth.variables[namesT[resultii[resi][0]][2]][1,:,5,5])
    extractParamsYearMonth.variables[namesT[resultii[resi][0]][2]][:] = resultii[resi][1][:]
    #print(extractParamsYearMonth.variables[namesT[resultii[resi][0]][2]][1,:,5,5])
print('\n')
#======================================= précipitations moyennes =========================================
for resj in range(rangejj) :
    #print('\n', resultii[result][0], namesT[resultjj[result][0]][2], extractParamsYearMonth.variables[namesP[resultjj[result][0]][2]])
    extractParamsYearMonth.variables[namesP[resultjj[resj][0]][2]][:] = resultjj[resj][1][:]
    #print(extractParamsYearMonth.variables[namesT[resultjj[result][0]][2]][1,:,5,5])
print('\n')
#======================================= précipitations cumulées =========================================
for resk in range(rangekk) :
    #print('\n', resultkk[result][0], namesT[resultkk[result][0]][2], extractParamsYearMonth.variables[namesPC[resultkk[result][0]][2]])
    extractParamsYearMonth.variables[namesPC[resultkk[resk][0]][2]][:] = resultkk[resk][1][:]
    #print(extractParamsYearMonth.variables[namesT[resultkk[result][0]][2]][1,:,5,5])
print('\n')

#print ('\n\nextractParamsYearMonth contents :', extractParamsYearMonth.variables['tmax_26'][:])

extractParamsYearMonth.close()
