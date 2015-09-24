import scraperwiki
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
from dateutil import parser

url='https://www.iwight.com/licensing/licenceconsultationlist.aspx'
response =requests.get(url)

soup=BeautifulSoup(response.content)
viewstate = soup.find('input' , id ='__VIEWSTATE')['value']
eventvalidation=soup.find('input' , id ='__EVENTVALIDATION')['value']
viewstategenerator=soup.find('input' , id ='__VIEWSTATEGENERATOR')['value']

params={'__VIEWSTATE':viewstate,
        '__VIEWSTATEGENERATOR':viewstategenerator,
        '__EVENTVALIDATION':eventvalidation,
        'q':'Search the site...',
        'ddlList':'Premises',
        'btnViewReg':'View Applications'}

r=requests.post(url,data=params)


def licenseConsultations(p):
    data=[]
    t=BeautifulSoup(p).find('div',id='pnlResults').find('table')
    rows=t.findAll('tr')
    cells=rows[0].findAll('td')
    head=[c.text for c in cells]
    for row in rows[1:]:
        i=0
        dr={}
        cells=row.findAll('td')
        for cell in cells:
            dr[head[i]]=cell.text
            i=i+1
        dr['stub']=cells[0].find('a')['href']
        data.append(dr)
        #cn=re.sub('[\r\t\n]','',cells[0].text).strip().strip(':').replace(u'\xa0',' ')
        #data[cn]=cells[1].text.strip()
        #try:
        #    data[cn+'_t']=parser.parse(data[cn], dayfirst=True)
        #except: 
        #    data[cn+'_t']=None
    return pd.DataFrame(data)


df=licenseConsultations(r.content)

#Use nominatim for now
def geocoder(addr):
    url='http://nominatim.openstreetmap.org/search'
    params={'q':addr+', UK','format':'json'}
    r=requests.get(url,params=params)
    print(addr,r.content)
    jdata=json.loads(r.content)[0]

    return jdata

def geocoder2(addr):
    url='https://maps.googleapis.com/maps/api/geocode/json'
    params={'address':addr+', UK'}
    r=requests.get(url,params=params)
    jdata=json.loads(r.content)['results']
    #This is a fudge: if we don't get a result, assume last phrase is a postcode and try that
    #Really should try to parse out postcode first and perhaps geocode against that?
    if len(jdata)==0:
        addr2=addr.split(',')[-1]+', UK'
        jdata=json.loads(requests.get(url,params={'address':addr2}).content)['results']
    if len(jdata)==0:
        retval=None
    else:
        retval=jdata[0]
    return retval

latlonlookup={}
for addr in df['address'].unique():
    g=geocoder2(addr)
    if 'geometry' in g:
        latlonlookup[addr]={'all':g,'lat':g['geometry']['location']['lat'],'lon':g['geometry']['location']['lng']}
    else:
        latlonlookup[addr]={'all':None,'lat':None,'lon':None}


def postcodeStripPatcher(latlonlookup,addr):
    addr2=','.join(addr.split(',')[:-1])
    g=geocoder2(addr2)
    latlonlookup[addr]={'all':g,'lat':g['geometry']['location']['lat'],'lon':g['geometry']['location']['lng']}
    return latlonlookup
latlonlookup=postcodeStripPatcher(latlonlookup,'Devonia Slipway, Esplanade, Sandown, Isle of Wight, PO36 8NJ')

df['lat']=df['address'].apply(lambda x: latlonlookup[x]['lat'])
df['lon']=df['address'].apply(lambda x: latlonlookup[x]['lon'])
df['end_consultation_t']=df['end_consultation'].apply(lambda x: parser.parse(x, dayfirst=True))

dt="CREATE TABLE IF NOT EXISTS 'IWLICENSEAPPLICATIONS' ('address' text,'end_consultation' text,'licence' text,'name' text,'number' text,'stub' text,'lat' real,'lon' real,'end_consultation_t' text)"
scraperwiki.sqlite.execute(dt)

t='IWLICENSEAPPLICATIONS'
if d != []:
  scraperwiki.sqlite.save(unique_keys=['number'],table_name=t, data=df.to_dict(orient='records'))
