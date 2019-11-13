import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import numpy as np

base_url = "https://usda.library.cornell.edu/concern/publications/3t945q76s?locale=en"

all_links = []
all_dates = []

n = 12
for i in range(1, n+1):
    if (i == 1):
        # handle first page
        r = requests.get(base_url)
    r = requests.get(base_url + "&page=%d" % i)
    
    soup = BeautifulSoup(r.content, 'html.parser')
    
    table = soup.find_all("table",{"class","release-items related-files table table-striped"})
    
    for row in table:
        links = row.find_all("a",href=True)
        for link in links:
            if ("xls" in link['href']):
                all_links.append(link['href'])
            else:
                next
    
    for row in table:
        releases = row.find_all("tr",{"class","release attributes row"})       
        for dates in releases:
            date = dates.find("td",{"class","attribute date_uploaded"})
            all_dates.append(date.text)

index = [x for x, s in enumerate(all_links) if '06-10-2015' in s]
new_format = all_links[0:index[0]]
old_format = all_links[index[0]:len(all_links)]

date_index = [x for x, s in enumerate(all_dates) if 'Jun 10, 2015' in s]
new_dates = all_dates[0:date_index[0]]
old_dates = all_dates[date_index[0]:len(all_dates)]

def NewFormat_toCSV():
    i = 0
    
    for report_link in new_format:
        report = report_link
        socket = urlopen(report)
        xd = pd.ExcelFile(socket)
        
        df1 = xd.parse(xd.sheet_names[1], parse_cols = "B:I",skiprows=9)
        df1.columns = ['Year','Proj Month','Output','Supply','Trade','Total Use','Ending Stocks']
        df1 = df1[df1.Output != "filler"]
        df1 = df1[df1.Year.isnull() == False]
        
        df2 = xd.parse(xd.sheet_names[2], parse_cols = "B:I",skiprows=10)
        df2.columns = ['Year','Proj Month','Output','Supply','Trade','Total Use','Ending Stocks']
        df2 = df2[df2.Output != "filler"]
        df2 = df2[df2.Year.isnull() == False]
        
        df3 = xd.parse(xd.sheet_names[3], parse_cols = "B:I",skiprows=9)
        df3.columns = ['Year','Proj Month','Output','Supply','Trade','Total Use','Ending Stocks']
        df3 = df3[df3.Output != "filler"]
        df3 = df3[df3.Year.isnull() == False]
        
        df_data = df1.append([df2,df3], ignore_index=True)
        
        df_data.to_csv("C:/Users/kerim/OneDrive/Documents/Notebooks/WASDE Reports/{}.csv".format(new_dates[i]))
        i += 1
            
def OldFormat_toCSV():
    i = 0  
    for report_link in old_format:
        report = report_link
        socket = urlopen(report)
        xd = pd.ExcelFile(socket)
        
        df1 = xd.parse(xd.sheet_names[1], parse_cols = "I:O",skiprows=11)
        df1.columns = ['Year','Proj Month','Output','Supply','Trade','Total Use','Ending Stocks']
        df1 = df1[df1.Output != "filler"]
        df1 = df1[df1.Year.isnull() == False]
        
        df2 = xd.parse(xd.sheet_names[2], parse_cols = "D:J",skiprows=10)
        df2.columns = ['Year','Proj Month','Output','Supply','Trade','Total Use','Ending Stocks']
        df2 = df2[df2.Output != "filler"]
        df2 = df2[df2.Year.isnull() == False]
        
        df3 = xd.parse(xd.sheet_names[3], parse_cols = "D:J",skiprows=10)
        df3.columns = ['Year','Proj Month','Output','Supply','Trade','Total Use','Ending Stocks']
        df3 = df3[df3.Output != "filler"]
        df3 = df3[df3.Year.isnull() == False]
        
        df_data = df1.append([df2,df3], ignore_index=True)
        
        df_data.to_csv("C:/Users/kerim/OneDrive/Documents/Notebooks/WASDE Reports/{}.csv".format(old_dates[i]))
        i += 1