import gzip
import os
import glob
import pandas as pd
import re
import shutil
import tarfile
import ftplib

def FTPImport():
    startYear = loopingYear = 1980
    endYear = 2018
    print('Starting connection to NOAA database')
    # Try connecting to the server
    try:
        ftp = ftplib.FTP('ftp.ncdc.noaa.gov') 
        ftp.login()
        print('Connection successful')
    except ftplib.all_errors as e:
        errorcode_string = str(e).split(None, 1)[0]
        
    ftp.cwd('/pub/data/gsod/')
    all_files= ftp.nlst()
    
    directoryName = 'GSOD Data'
    
    if not os.path.exists(directoryName):
        os.makedirs(directoryName)
    # Move into the folder
    directoryPath = '%s/%s' % (os.getcwd(), directoryName)
    os.chdir(directoryPath)
    
    while loopingYear <= endYear:
    
        tempDirectory = '/pub/data/gsod/%s' % loopingYear
        tempFileName = 'gsod_%s.tar' % loopingYear
        file = open(tempFileName, 'wb')
        
        ftp.cwd(tempDirectory)
        
        print('Downloading %s' % loopingYear)
        
        try:
            ftp.retrbinary('RETR %s' % tempFileName, file.write)
            print('Successfully downloaded %s' % loopingYear)
        except ftplib.all_errors as e:
            print('Error downloading %s' % loopingYear)
            errorcode_string = str(e).split(None, 1)[0]
        
        loopingYear += 1
        
    print('All files downloaded')
    print('Closing connection')
    # Close the connection
    ftp.close()
    
def TarReader():
    path = "C:/Users/kerim/OneDrive/Documents/Notebooks/GSOD Data/"
    extension = 'tar'
    os.chdir(path)
    results = glob.glob('*.{}'.format(extension))
    
    for result in results:
        f = tarfile.open(result)
        f.extractall("C:/Users/kerim/OneDrive/Documents/Notebooks/Extracted GSOD Data")
        f.close()

def GZtoCSV():
    path = "C:/Users/kerim/OneDrive/Documents/Notebooks/Extracted GSOD Data"
    extension = 'gz'
    os.chdir(path)
    results = glob.glob('*.{}'.format(extension))
    
    for result in results:
        f = gzip.open(result, 'rb')
        features_train = pd.read_csv(f,header=None)
        f.close()
        
        features_train = features_train[1:]
        
        for x in range(0,len(features_train)):
            features_train.iloc[x,0] = (features_train.iloc[x,0].replace('        ',',').replace('       ',',').replace('      ',',').
            replace('     ',',').replace('    ',',').replace('   ',',').replace('  ',',').replace(' ',','))
        
        for x in range(1,22):
            features_train[x] = features_train[0].str.split(',').str[x]
        
        features_train[0] = features_train[0].str.split(',').str[0]
        
        features_train.columns = ["STN---","WBAN","YEARMODA","TEMP","FLAG1",
        "DEWP","FLAG2","SLP","FLAG3","STP","FLAG4","VISIB","FLAG5","WDSP","FLAG6","MXSPD","GUST","MAX","MIN","PRCP","SNDP","FRSHTT"]
        
        features_train.to_csv("C:/Users/kerim/OneDrive/Documents/Notebooks/GSOD CSVs/{}.csv".format(result))

def CreateYears():
    years = ['1980','1981','1982','1983','1984','1985','1986','1987','1988','1989','1990','1991','1992','1993','1994','1995',
            '1996','1997','1998','1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011',
            '2012','2013','2014','2015','2016','2017','2018']
    
    for year in years:
        path = "C:/Users/kerim/OneDrive/Documents/Notebooks/Weather Data/{}".format(year)
        
        try:
            os.mkdir(path)
        except OSError:
            print ("Creation of the directory %s failed" % path)
        else:
            print ("Successfully created the directory %s " % path)

def ConsolidateYears():
    years = ['1980','1981','1982','1983','1984','1985','1986','1987','1988','1989','1990','1991','1992','1993','1994','1995',
            '1996','1997','1998','1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011',
            '2012','2013','2014','2015','2016','2017','2018']
    
    path = "C:/Users/kerim/OneDrive/Documents/Notebooks/GSOD CSVs"
    extension = 'csv'
    os.chdir(path)
    results = glob.glob('*.{}'.format(extension))
    
    for year in years:
        for result in results:
            if '99999' + '-' + '{}'.format(year) in result:
                shutil.move(result, "C:/Users/kerim/OneDrive/Documents/Notebooks/Weather Data/{}".format(year))
            else:
                next

def MergeYears():
    years = ['1980','1981','1982','1983','1984','1985','1986','1987','1988','1989','1990','1991','1992','1993','1994','1995',
            '1996','1997','1998','1999','2000','2001','2002','2003','2004','2005','2006','2007','2008','2009','2010','2011',
            '2012','2013','2014','2015','2016','2017','2018']
    
    for year in years:
        path = "C:/Users/kerim/OneDrive/Documents/Notebooks/Weather Data/{}".format(year)
        extension = 'csv'
        os.chdir(path)
        results = glob.glob('*.{}'.format(extension))
        
        df = []
        for filename in sorted(results):
             df.append(pd.read_csv(filename))
        full_df = pd.concat(df)
        full_df.to_csv("C:/Users/kerim/OneDrive/Documents/Notebooks/Merged Weather Data/{}_merged.csv".format(year),index=False)