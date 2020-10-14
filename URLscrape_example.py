from bs4 import BeautifulSoup
import Levenshtein as lev
import re
import time
import urllib.request
import pandas as pd

header = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.77 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml',
    'Accept-Encoding': 'none',
    'Accept-Charset': 'utf-8'
}


file='PUT NAME OF THE CSV FILE'
country='PUT COUNTRY NAME'
col_v_name='PUT COLUMN NAME WITH VESSEL NAME' 

links=[]
urls=[]
     
url = 'PUT URL TO SCRAPE' +country

    
req = urllib.request.Request(url, None, header)
with urllib.request.urlopen(req, timeout=20) as response:
    page = response.read()
html = BeautifulSoup(page,'lxml')
tables = html.findAll("tbody")
time.sleep(5)


for table in tables:
    for row in table.find_all('a', attrs={'class':'ship-link'}):
        links.append(row.get('href'))                  
                
    items=[]  
                
    for i in range(0, len(links)):
                
        linkscsv2=links[i]
                            
        start = re.escape("/vessels/")
        end   = re.escape("-IMO")
        name_scrape = re.search('%s(.*)%s' % (start, end), linkscsv2).group(1)
    
    
        start = re.escape("-IMO-")
        end   = re.escape("-MMSI")
        imo_scrape = re.search('%s(.*)%s' % (start, end), linkscsv2).group(1)
    
                
        start = re.escape("-MMSI-")
        end   = re.escape("")
        mmsi_scrape = re.search('%s(.*)%s' % (start, end), linkscsv2).group(1)
    
        items.append((name_scrape,imo_scrape,mmsi_scrape))
      


df = pd.DataFrame(items,columns=['name','imo','mmsi'])
df=df[['name','imo','mmsi']]
df = df.astype(str)
table_vessel=df.to_dict()


df2 = pd.read_csv(file+".csv", sep=';', encoding='latin-1')
df2 = df2.astype(str)
table_fleet_gr=df2.to_dict()
#######################################################################################
name_scrape=[]
imo_scrape=[]
mmsi_scrape=[]


for i in table_fleet_gr[col_v_name]:
    naz=''
    im=''
    mms=''
    if table_fleet_gr[col_v_name][i]!="":
        for z in table_vessel['name']:
            if lev.ratio(table_fleet_gr[col_v_name][i].lower(),table_vessel['name'][z].lower())>=0.8:
                naz=naz+table_vessel['name'][z]
                im=im+table_vessel['imo'][z]
                mms=mms+table_vessel['mmsi'][z]
            else:
                naz=naz+' '
                im=im+' '
                mms=mms+' '
                
        if naz.isspace()==True:
            name_scrape.append('---')
        else:
            name_scrape.append(re.sub('[ ]{2,}', ',', naz).strip()[1:-1])          
            
        if im.isspace()==True:
            imo_scrape.append('---')
        else:
            imo_scrape.append(re.sub('[ ]{2,}', ',', im).strip()[1:-1])
            
        if mms.isspace()==True:
            mmsi_scrape.append('---')
        else:
            mmsi_scrape.append(re.sub('[ ]{2,}', ',', mms).strip()[1:-1])
                
    else:
        name_scrape.append(table_fleet_gr['name_scrape'][i])
        imo_scrape.append(table_fleet_gr['imo_scrape'][i])
        mmsi_scrape.append(table_fleet_gr['mmsi_scrape'][i])



name_1 = dict(enumerate(name_scrape))
table_fleet_gr['name_scrape']=name_1

imo_1 = dict(enumerate(imo_scrape))
table_fleet_gr['imo_scrape']=imo_1

mmsi_1 = dict(enumerate(mmsi_scrape))
table_fleet_gr['mmsi_scrape']=mmsi_1


df_u = pd.DataFrame.from_dict(table_fleet_gr,  orient='columns')
df_u.to_csv(file+"_result.csv",sep=';',index=False)