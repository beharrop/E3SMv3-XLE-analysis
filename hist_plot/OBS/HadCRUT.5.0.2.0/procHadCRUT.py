# Script to pre-process HadCRUT.5.0.2.0 data into the format used by ts_tas plotting

import numpy as np
import numpy.ma as ma

def read_hadcrut5(infile,outfile):

    tmp_tas = []
    f = open(infile, 'r')
    fn = open(outfile,'w')

    line = f.readline() # description line
    line = f.readline()
    mean_tas = 0.0
    f_count = 0.0
    while line:
      tmp = line.split(",")
      tmptime = tmp[0].split("-")
      # current year in string format
      currYear = tmptime[0]
      tmp_year = int(tmptime[0])
      tmp_month = int(tmptime[1])

      tmp_tas.append(float(tmp[1]))
      tas1 = float(tmp[1])
      # convert to fixed format string, as f.write can only take string
      fmt_tas1 = f"{tas1:6.3f}"
      mean_tas += tas1
      f_count += 1

      if tmp_month == 1:
          fn.write(currYear + " ")
          fn.write(fmt_tas1 + " ")
      else:
          fn.write(fmt_tas1 + " ")
      
      if tmp_month == 12:
          # compute annual mean
          mean_tas /= f_count    
          fmt_mean_tas = f"{mean_tas:6.3f}"
          fn.write(fmt_mean_tas+" ")
          fn.write("\n")
          # to mimic original HadCRUT data with an extra line. need to know the meaning of the numbers at the 2nd strong. Is it percentage data coverage?
          fn.write(tmptime[0]+"\n")  

          # Possible to use print with format directly
#         print(tmp_year,' ',file=fn)
#         for v in tmp_tas:
#           print(v,' ',file=fn)
#         print("\n",file=fn)
          tmp_tas = []
          mean_tas = 0.0
          f_count = 0.0

      line = f.readline()

    # Handling partial year data, padding the line with missing values, after reading the last line of the file
    if tmp_month != 12: 
       for n in range(tmp_month+1,13):
           tas1=-9.999
           fmt_tas1 = f"{tas1:6.3f}"
           fn.write(fmt_tas1 + " ")
       mean_tas /= f_count
       fmt_mean_tas = f"{mean_tas:6.3f}"
       fn.write(fmt_mean_tas+" ")
       fn.write("\n")
       # to mimic original HadCRUT data
       fn.write(currYear+"\n")  
    f.close()
    fn.close()

# -----------------------------------------------------------------------------
def main():

  # read and process data

 read_hadcrut5("HadCRUT.5.0.2.0.analysis.summary_series.global.monthly.csv","HadCRUT5.0Analysis_gl.txt")
 read_hadcrut5("HadCRUT.5.0.2.0.analysis.summary_series.northern_hemisphere.monthly.csv","HadCRUT5.0Analysis_nh.txt")
 read_hadcrut5("HadCRUT.5.0.2.0.analysis.summary_series.southern_hemisphere.monthly.csv","HadCRUT5.0Analysis_sh.txt")

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    main()

