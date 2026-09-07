# Extract global, NH, SH surface air temperature from historical simulations
# and save it to CSV files to simplify downstream analysis

# On perlmutter
"""
source /global/common/software/e3sm/anaconda_envs/load_e3sm_unified_1.11.0_pm-cpu.sh
"""

import cdms2, cdutil, cdtime
import numpy as np
import numpy.ma as ma
import os

# -----------------------------------------------------------------------------
def main():

  # --- Historical simulations ---
  exps = [

    {'inout':'E3SMv3/v3.LR.historical_'},
    {'inout':'E3SMv3.lowECS/v3.LR.lowECS.historical_'},
    {'inout':'E3SMv3.highECS/v3.LR.highECS.historical_'},

  ]

  ensembles = ['0051', '0091', '0101', '0111', '0121',
               '0131', '0141', '0151', '0161', '0171',
               '0181', '0191', '0201', '0211', '0221',
               '0231', '0241', '0251', '0261', '0271',
               '0281', '0291', '0301', '0311', '0321']

  #hist = []
  for i in range(len(exps)):

    for ens in ensembles:

      #print("Reading %s" % exps[i]['inout'])
      print(f"Reading {exps[i]['inout']}{ens}.xml")

      # Read data
      #f = cdms2.open(exps[i]['input'])
      if not os.path.exists(f"{exps[i]['inout']}{ens}.xml"):
        continue
      if os.path.exists(f"{exps[i]['inout']}{ens}.csv"):
        continue
      f = cdms2.open(f"{exps[i]['inout']}{ens}.xml")
      trefht  = f('TREFHT')
      ts      = f('TS')
      ocnfrac = f('OCNFRAC')
      f.close()

      # Compute blended surface temperature: 
      #   SST over ice-free ocean, threfht over land and sea ice
      var = ocnfrac*ts + (1.0-ocnfrac)*trefht

      # Annual averages
      var = cdutil.YEAR(var)

      # Time: use center of time bounds
      time = var.getTime()
      time_bounds = time.getBounds()
      time[:] = 0.5*(time_bounds[:,0]+time_bounds[:,1])
      date = time.asComponentTime()
      year = np.array([ date[n].year for n in range(len(time)) ])
      #print(year)

      # Regional averages
      regions = ['glb', 'nh', 'sh']
      tas = ma.zeros( (len(year),len(regions)), 'float64')
      j = 0
      for region in regions:

        if region == 'nh':
          nh = var(latitude=(0.,90.))
          tas[:,j] = cdutil.averager(nh, axis='xy', weights='generate').asma()

        elif region == 'sh':
          sh = var(latitude=(-90.,0.))
          tas[:,j] = cdutil.averager(sh, axis='xy', weights='generate').asma()

        elif region == 'glb':
          tas[:,j] = cdutil.averager(var, axis='xy', weights='generate').asma()
  
        j += 1

      # Output to file
      data = ma.zeros( (len(year),len(regions)+1), 'float64')
      data[:,0] = year
      for j in range(len(regions)):
        data[:,j+1] = tas[:,j]
      np.savetxt(f"{exps[i]['inout']}{ens}.csv", data, delimiter=',')

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()

