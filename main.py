import zipfile
import os
import cdsapi
import xarray as xr
import subprocess
from util import mergetime

cmip6d = #Data Directory
#define subdirectories here, as example here are three

d1 = os.path.join(cmip6d,'1')
d2 = os.path.join(cmip6d,'2')
d3 = os.path.join(cmip6d,'3')

c = cdsapi.Client()
variable = #Define variable using CDS variable naming
mlist = #Define model list
flist = #choose forcings
dlist = [d1,d2,d3]

failedlist = []
failedfile = os.path.join(cmip6d,'failed.txt') #error output file


for forcing,direc in zip(flist,dlist):
    print(forcing)
    for model in mlist:
          print(model)
          fname = os.path.join(direc,model+'.zip')
          try:
              c.retrieve(
                  'projections-cmip6',
                  {
                      'format': 'zip',
                      'temporal_resolution': 'monthly',
                      'experiment': forcing,
                      'variable': variable,
                      'model': model,
                      'month': [
                          '01', '02', '03',
                          '04', '05', '06',
                          '07', '08', '09',
                          '10', '11', '12',
                      ], #all months
                      'year': [
                          '2015', '2016', '2017',
                          '2018', '2019', '2020',
                          '2021', '2022', '2023',
                          '2024', '2025', '2026',
                          '2027', '2028', '2029',
                          '2030', '2031', '2032',
                          '2033', '2034', '2035',
                          '2036', '2037', '2038',
                          '2039', '2040', '2041',
                          '2042', '2043', '2044',
                          '2045', '2046', '2047',
                          '2048', '2049', '2050',
                          '2051', '2052', '2053',
                          '2054', '2055', '2056',
                          '2057', '2058', '2059',
                          '2060', '2061', '2062',
                          '2063', '2064', '2065',
                          '2066', '2067', '2068',
                          '2069', '2070', '2071',
                          '2072', '2073', '2074',
                          '2075', '2076', '2077',
                          '2078', '2079', '2080',
                          '2081', '2082', '2083',
                          '2084', '2085', '2086',
                          '2087', '2088', '2089',
                          '2090', '2091', '2092',
                          '2093', '2094', '2095',
                          '2096', '2097', '2098',
                          '2099',
                      ], #all CMIP6 years, remove or redefine if needed
                  },
                  fname)

              bad_vars = ['time_bnds','plev_bnds','lat_bnds','latitude_bnds'] #Unwanted variables to be removed
              with zipfile.ZipFile(fname,'r') as file:
                  count = 0
                  flist = []
                  for f in file.namelist():
                      if 'va' in f:
                          file.extract(f,direc)
                          nf = os.path.join(direc,f)
                          os.rename(nf,os.path.join(direc,model+str(count)+'.nc'))

                          flist.append(os.path.join(direc,model+str(count)+'.nc'))
                          tds = xr.open_dataset(os.path.join(direc,model+str(count)+'.nc')) #Open as Xarray to manipulate
                          try:
                              for var in bad_vars:
                                  try:
                                      tds.drop_vars(var)
                                  except:
                                      pass #Unwanted variables not always present
                                if zonmean == True:
                                    tds['va'].mean(dim='lon').to_netcdf(os.path.join(direc,model+str(count)+'a.nc'))#lon in some, longitude in others
                                else:
                                    tds.to_netcdf(os.path.join(direc,model+str(count)+'a.nc'))
                          except:
                              for var in bad_vars:
                                  try:
                                      tds.drop_vars(var)
                                  except:
                                      pass
                                if zonmean == True:
                                    tds['va'].mean(dim='longitude').to_netcdf(os.path.join(direc,model+str(count)+'a.nc'))
                                else:
                                    tds.to_netcdf(os.path.join(direc,model+str(count)+'a.nc'))
                          os.remove(os.path.join(direc,model+str(count)+'.nc'))
                          os.rename(os.path.join(direc,model+str(count)+'a.nc'),os.path.join(direc,model+str(count)+'.nc'))
                          count +=1
              subprocess.run(mergetime(os.path.join(direc,model+'*'+'.nc'),os.path.join(direc,'a'+model+'.nc')))
              for file in flist:
                  os.remove(file)
              os.rename(os.path.join(direc,'a'+model+'.nc'),os.path.join(direc,model+'.nc'))
              os.remove(fname)
          except:
              failedlist.append(variable + ' ' + model + ' ' + forcing)

            
with open(failedfile, 'w') as f:
    for line in failedlist:
        f.write(f"{line}\n")
