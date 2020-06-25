import s3fs
import numpy as np
import os

# Use the anonymous credentials to access public data
fs = s3fs.S3FileSystem(anon=True)

# Get latest scan from bucket
# satellite=goes16 or goes17; domain=FULLDISK, CONUS, M1, or M2
def getLatestScan(satellite, product, domain, channel, download=False, path=''):
    # List contents of GOES bucket
    goesData = np.array(fs.ls('s3://noaa-' + satellite + '/' + product + domain[0]))

    # Get Latest Year
    goesData = np.array(fs.ls(goesData[len(goesData)-1]))

    # Get Latest Day
    goesData = np.array(fs.ls(goesData[len(goesData)-1]))

    # Get Latest Hour
    goesData = np.array(fs.ls(goesData[len(goesData)-1]))

    # Get Latest Scan
    scanNum = int((len(goesData) / 16) * channel)-1

    fileName=goesData[scanNum][goesData[scanNum].rindex('/'):]

    # Download scan
    if download and os.path.exists(path):
        if os.path.exists(f'{path}/{fileName}') == False:
            fs.get(goesData[scanNum], f'{path}/{fileName}')

    return fileName
