from datetime import datetime, timedelta
import s3fs
import numpy as np
import os
import pandas as pd
import urllib.request as request

def getLatestScan_IowaState(radarSite, download=False, path='', name=''):
    dataList = pd.read_csv(f'https://mesonet-nexrad.agron.iastate.edu/level2/raw/{radarSite}/dir.list', delimiter=' ', header=None)
    latestScan=dataList[1].iloc[-1]
    dataFile=f'https://mesonet-nexrad.agron.iastate.edu/level2/raw/{radarSite}/{latestScan}'
    
    # Download scan
    if download and os.path.exists(path):
        if name == '':
            if os.path.exists(f'{path}/{latestScan}') == False:
                request.urlretrieve(dataFile, f'{path}/{latestScan}')
        else:
            request.urlretrieve(dataFile, f'{path}/{name}')
            latestScan=name

    return f'{path}/{latestScan}'

    
def getLatestScan(radarSite, download=False, path='', name=''):
    # Use the anonymous credentials to access public data
    fs = s3fs.S3FileSystem(anon=True)
    
    # List contents of NOAA NEXRAD L2 bucket
    radarBin = np.array(fs.ls('s3://noaa-nexrad-level2/'))

    # Get latest year (Subtract 2 to remove index.html)
    radarBin = np.array(fs.ls(radarBin[len(radarBin)-2]))

    # Get latest month
    radarBin = np.array(fs.ls(radarBin[len(radarBin)-1]))

    # Get latest day
    radarBin = np.array(fs.ls(radarBin[len(radarBin)-1] + '/' + radarSite))

    # Get latest file
    radarBin = np.array(fs.ls(radarBin[len(radarBin)-1]))
    latestScan = radarBin[0].replace('_MDM', '')

    # Download scan
    fileName=latestScan[latestScan.rindex('/'):]
    if download and os.path.exists(path):
        if name == '':
            if os.path.exists(f'{path}/{fileName}') == False:
                fs.get(latestScan, f'{path}/{fileName}')
        else:
            fileName=name
            fs.get(latestScan, f'{path}/{name}')

    return fileName


def getArchivedScan(radarSite, time):
    # Use the anonymous credentials to access public data
    fs = s3fs.S3FileSystem(anon=True)
    
    # List contents of NOAA NEXRAD L2 bucket for given date/time and radar
    radarBin = np.array(fs.ls('s3://noaa-nexrad-level2/' + time.strftime('%Y/%m/%d') + '/' + radarSite + '/'))

    closestScan=radarBin[0]
    closestScanTime=99999
    for scan in radarBin:

        if '.ta' not in scan and '_MDM' not in scan:
            # Capture time from scan
            s = scan.rfind('/') + 1
            e = scan.rfind('_V06')

            scanTime = datetime.strptime(scan[s:e], radarSite+'%Y%m%d_%H%M%S')

            # Find closest scan
            timeDif = scanTime-time
            if (abs(timeDif.total_seconds())<=closestScanTime):
                closestScanTime = abs(timeDif.total_seconds())
                closestScan = scan

    return closestScan








