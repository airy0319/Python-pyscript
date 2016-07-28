# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 17:43:33 2016

@author: jiaopeng
"""
#Filename:firstCount.py
import glob,gzip
pv=0
uv=set()
for name in glob.glob('/data1/logs/actsta/2016-07-08/1200*.sta.gz'):
    for line in gzip.open(name):
        ss=line.split('\t')
        if len(ss)<12:
            continue
        b1,b2,url,uid=ss[0].strip(),ss[2].strip(),ss[3].strip(),ss[5].strip()

        if b1=='appdownload' and b2=='clienth5' and  'http://m.moji.com/?from=ifengnews' in url:
            uv.add(uid)
            pv+=1
print pv,len(uv)
