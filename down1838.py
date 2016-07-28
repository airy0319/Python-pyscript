# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 10:22:55 2016

@author: jiaopeng
"""

#Filename:down1838.py
import glob
dic = {}
for name in glob.glob('/data1/logs/app/othapp/2016-07-20/*.sta'):
    for line in open(name):
        ss=line.split('\t')
        if len(ss)<10:
            continue
        b1,plat,pub,url=ss[0].strip(),ss[2].strip(),ss[4].strip(),ss[12].strip()
        if b1=='down_news' and pub=='1838' and 'aid='  in url:
           url = url.split('&')[0]
           dic.setdefault((plat,pub,url),0)
           dic[(plat,pub,url)] +=1
for k in dic:
    #print dic,'\t'
    print k,'\t',dic[k],'\t'