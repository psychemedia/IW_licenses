import scraperwiki
import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import datetime
from dateutil import parser

url='https://www.iwight.com/licensing/licenceconsultationlist.aspx'
response =requests.get(url)


def licenseConsultations(p):
    data=[]
    t=BeautifulSoup(p).find('div',id='pnlResults')
    if t is not None: t=t.find('table')
    else: return pd.DataFrame()
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



def postcodeStripPatcher(latlonlookup,addr):
    addr2=','.join(addr.split(',')[:-1])
    g=geocoder2(addr2)
    latlonlookup[addr]={'all':g,'lat':g['geometry']['location']['lat'],'lon':g['geometry']['location']['lng']}
    return latlonlookup

def licenseScraper(ltype='Premises'):
	soup=BeautifulSoup(response.content)
	viewstate = soup.find('input' , id ='__VIEWSTATE')['value']
	eventvalidation=soup.find('input' , id ='__EVENTVALIDATION')['value']
	viewstategenerator=soup.find('input' , id ='__VIEWSTATEGENERATOR')['value']

	params={'__VIEWSTATE':viewstate,
			'__VIEWSTATEGENERATOR':viewstategenerator,
			'__EVENTVALIDATION':eventvalidation,
			'q':'Search the site...',
			'ddlList':ltype,
			'btnViewReg':'View Applications'}

	r=requests.post(url,data=params)

	df=licenseConsultations(r.content)
	if len(df)==0: return
	latlonlookup={}
	for addr in df['address'].unique():
		g=geocoder2(addr)
		if 'geometry' in g:
			latlonlookup[addr]={'all':g,'lat':g['geometry']['location']['lat'],'lon':g['geometry']['location']['lng']}
		else:
			latlonlookup[addr]={'all':None,'lat':None,'lon':None}

	#latlonlookup=postcodeStripPatcher(latlonlookup,'Devonia Slipway, Esplanade, Sandown, Isle of Wight, PO36 8NJ')
        df['licenseType']=ltype
	df['lat']=df['address'].apply(lambda x: latlonlookup[x]['lat'])
	df['lon']=df['address'].apply(lambda x: latlonlookup[x]['lon'])
	df['end_consultation_t']=df['end_consultation'].apply(lambda x: parser.parse(x, dayfirst=True))
	df['end_consultation_t']= df['end_consultation_t'].apply(lambda x: datetime.date(x.year,x.month,x.day))

	dt="CREATE TABLE IF NOT EXISTS 'IWLICENSEAPPLICATIONS' ('address' text,'end_consultation' text,'licence' text,'name' text,'number' text,'stub' text,'lat' real,'lon' real,'end_consultation_t' text)"
	scraperwiki.sqlite.execute(dt)

	t='IWLICENSEAPPLICATIONS'
	dfd=df.to_dict(orient='records')
	newRecords=[]
	for r in dfd:
		if len(scraperwiki.sqlite.select("* from {t} where number='{n}'".format(t=t,n=r['number'])))==0:
			print('First seen',r)
			r['firstSeen']=datetime.datetime.utcnow()
			newRecords.append(r)
	if len(newRecords)>0:
		print('Adding {} new records'.format(len(newRecords)))
		scraperwiki.sqlite.save(unique_keys=['number'],table_name=t, data=newRecords)
	return

for l in ['Premises','Sex Establishments','Street Trading','Street Furniture']:
        licenseScraper(l)
