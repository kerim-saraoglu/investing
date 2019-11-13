import requests
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen
import nltk
import numpy as np
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
import string
from nltk.stem import PorterStemmer
from nltk.tokenize import PunktSentenceTokenizer

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

def NewFormatCommentary():
    
    i = 0

    for report_link in new_format:
        report = report_link
        socket = urlopen(report)
        xd = pd.ExcelFile(socket)
        
        df_commentary = xd.parse(xd.sheet_names[0], parse_cols = "A")
        df_commentary.columns = ['Commentary']
        df_commentary = df_commentary[df_commentary.Commentary.isnull() == False]
        

        all_rows = []
        
        stopWords = set(stopwords.words('english'))
        
        for row in df_commentary['Commentary']:
            filtered_row = []
            tokenized_row = word_tokenize(row)
            tokenized_row = [word.lower() for word in tokenized_row if word.isalpha() or word.isnumeric()]
            
            for w in tokenized_row:
                if w not in stopWords:
                    filtered_row.append(w)
            
            all_rows.append(filtered_row)
        
        row_df =pd.DataFrame(all_rows)
        row_df.to_csv("C:/Users/kerim/OneDrive/Documents/Notebooks/WASDE Commentary/{} Commentary.csv".format(new_dates[i]))
        i += 1

def OldFormatCommentary():
    
    i = 0

    for report_link in new_format:
        report = report_link
        socket = urlopen(report)
        xd = pd.ExcelFile(socket)
        
        df_commentary = xd.parse(xd.sheet_names[0], parse_cols = "A")
        df_commentary.columns = ['Commentary']
        df_commentary = df_commentary[df_commentary.Commentary.isnull() == False]
        

        all_rows = []
        
        stopWords = set(stopwords.words('english'))
        
        for row in df_commentary['Commentary']:
            filtered_row = []
            tokenized_row = word_tokenize(row)
            tokenized_row = [word.lower() for word in tokenized_row if word.isalpha() or word.isnumeric()]
            
            for w in tokenized_row:
                if w not in stopWords:
                    filtered_row.append(w)
            
            all_rows.append(filtered_row)
        
        row_df =pd.DataFrame(all_rows)
        row_df.to_csv("C:/Users/kerim/OneDrive/Documents/Notebooks/WASDE Commentary/{} Commentary.csv".format(old_dates[i]))
        i += 1