# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 10:15:12 2016

@author: jiaopeng
"""

#Filename:down5312.py
import glob
#import re
dic = {}
for name in glob.glob('/data1/logs/app/othapp/2016-07-20/*.sta'):
    for line in open(name):
        ss=line.split('\t')
        if len(ss)<10:
            continue
        b1,plat,pub,url=ss[0].strip(),ss[2].strip(),ss[4].strip(),ss[12].strip()
        if b1=='down_news' and pub=='5312' and 'aid='  in url:
           aid=url.split('aid=')[1].split('&')[0]
           url = url.split('?')[0]+'?aid='+aid
           #id= re.compile(r'aid=(\d*)')正则匹配ID的值
           #aid = re.findall(id,url)只获取aid的值
           #url = url.split('?')[0]+'?aid='+aid[0]
           
           dic.setdefault((plat,pub,url),0)
           dic[(plat,pub,url)] +=1
for k in dic:
    #print dic,'\t'
    print k,'\t',dic[k],'\t'
    

