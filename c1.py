#!/usr/bin/python
#Filename:c1.py

import glob,gzip,datetime
begin = datetime.date(2016,6,30)
end = datetime.date(2016,7,17)
pv=0
uv=set()
for name in glob.glob('/data1/logs/actsta/2016-07-16/*.sta.gz'):
    for line in gzip.open(name):
        ss=line.split('\t')
        if len(ss)<12:
            continue
        b1,b2,url,uid=ss[0].strip(),ss[2].strip(),ss[3].strip(),ss[5].strip()

        if b1=='appdownload' and b2=='clienth5' and  'http://dl.cm.ksmobile.com/static/res/86/77/CleanMaster_2010006241_0_1467185404.apk' in url:
            uv.add(uid)
            pv+=1
print pv,len(uv)            




import glob,gzip,datetime,time

begin = datetime.date(2016,6,30)
end = datetime.date(2016,7,17)
pv=0
uv=set()
for d  in range((end-begin).days+1):
    date=time.strftime("%Y-%m-%d",time.localtime(time.mktime(time.strptime('2016-06-30','%Y-%m-%d'))+86400*d))
    #d=begin
    #delta = datetime.timedelta(days=1)
    #print d,delta
    for name in glob.glob('/data1/logs/actsta/%s/*.sta.gz'%date):
        for line in gzip.open(name):
            ss=line.split('\t')
            if len(ss)<12:
                continue

            b1,b2,url,uid=ss[0].strip(),ss[2].strip(),ss[3].strip(),ss[5].strip()
            if b1=='appdownload' and b2=='clienth5' and  'http://dl.cm.ksmobile.com/static/res/86/77/CleanMaster_2010006241_0_1467185404.apk' in url:
                uv.add(uid)
                pv+=1
    print date, pv,len(uv)
    #d += delta
