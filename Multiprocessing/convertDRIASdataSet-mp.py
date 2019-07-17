import netCDF4 as nc
import numpy as np
from datetime import datetime
from array import array
import sys
import os
import multiprocessing as mp


def convert_file(localpathin, localpathout, driasfilename):

    namein = localpathin + driasfilename
    nameout = localpathout + driasfilename
    # print (namein, '\n',  nameout, '\n\n')
    # open input file
    orig_drias = nc.Dataset(namein)
    # open output file
    # for security reasons remove the file bearing this filename 
    # ! take care not to loose important data
    try:
        os.remove(nameout)  
    except OSError:
        pass
    converted_set = nc.Dataset(nameout, mode='w', format='NETCDF4')
 
    for thisAttr in orig_drias.ncattrs():
        converted_set.setncattr(thisAttr, orig_drias.getncattr(thisAttr))
    # create dimensions
    for dimens in orig_drias.dimensions.keys():
        this_dim = orig_drias.dimensions[dimens]
        converted_set.createDimension(this_dim.name, this_dim.size)
    # create variables
    for var in orig_drias.variables.keys():
        this_name = orig_drias.variables[var].name
        this_type = orig_drias.variables[var].dtype
        if this_type == 'float64':
            this_type = 'float32'
        this_dim = orig_drias.variables[var].dimensions
        try:
            orig_drias.variables[var]._FillValue
        except AttributeError:
            converted_set.createVariable(this_name, this_type, this_dim)
        else:
            converted_set.createVariable(this_name, this_type, this_dim, orig_drias.variables[var]._FillValue)
        for thisAttr in orig_drias.variables[var].ncattrs():
            converted_set.variables[var].setncattr(thisAttr, orig_drias.variables[var].getncattr(thisAttr))
        converted_set.variables[var][:] = orig_drias.variables[var][:]

    converted_set.close()
    print(driasfilename, '\t\t', datetime.now())

    return driasfilename

# =================================parallel execution ========================


def collect_result(result):
    global results
    results.append(result)


pool = mp.Pool(mp.cpu_count())
print("Number of processors: ", mp.cpu_count())

results = []

# =================================parallel execution ========================

i = 0

# pathIn = '/home/vidal/TremplinDesSciences/2019/ClimatLyon/DataDrias/'
# pathOut = '/home/vidal/TremplinDesSciences/2019/ClimatLyon/ConvertedDrias/'
path_in = '/home/vidal/TremplinDesSciences/2019/ClimatLyon/DataDrias/Toulouse-1/'
path_out = '/home/vidal/TremplinDesSciences/2019/ClimatLyon/ConvertedDrias/Toulouse-1/'

Names = ['tasmin_metro_CNRM_Aladin_histo_QT_REF_19500101-20051231.nc',
         'tasmin_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc',
         'tasmin_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc',
         'tasmin_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc',
         'huss_metro_CNRM_Aladin_histo_QT_REF_19500101-20051231.nc',
         'huss_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc',
         'huss_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc',
         'huss_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc',
         'prsnls_metro_CNRM_Aladin_histo_QT_REF_19500101-20051231.nc',
         'prsnls_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc',
         'prsnls_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc',
         'prsnls_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc',
         'rstr_metro_CNRM_Aladin_histo_QT_REF_19500101-20051231.nc',
         'rstr_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc',
         'rstr_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc',
         'rstr_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc',
         'tasmax_metro_CNRM_Aladin_histo_QT_REF_19500101-20051231.nc',
         'tasmax_metro_CNRM_Aladin_rcp2.6_QT_RCP2.6_20060101-21001231.nc',
         'tasmax_metro_CNRM_Aladin_rcp4.5_QT_RCP4.5_20060101-21001231.nc',
         'tasmax_metro_CNRM_Aladin_rcp8.5_QT_RCP8.5_20060101-21001231.nc', ]

# ============================== linear execution ==========================
#for i, this_name in enumerate(Names):
#    convert_file(path_in, path_out, this_name)
#    i += 1
# ============================== /linear execution ==========================

# ================================= parallel execution ========================
for i, this_name in enumerate(Names):
    pool.apply_async(convert_file, args=(path_in, path_out, this_name), callback=collect_result)

pool.close()
pool.join()
# ================================= /parallel execution ========================
