# -*- coding: utf-8 -*-
"""
Created on Fri Jul 15 10:15:09 2016

@author: jiaopeng
"""

#Filename:countpvuv.py
import glob,gzip
dic={}
for name in glob.glob('/data1/logs/actsta/2016-07-14/1000*.sta.gz'):
    for line in gzip.open(name):
        ss=line.split('\t')
        if len(ss)<12:#name的长度
            continue
        b1,b2,url,uid=ss[0].strip(),ss[2].strip(),ss[3].strip(),ss[5].strip()
        if b1=='appdownload' and b2=='clienth5' and url == 'http://m.moji.com/?from=ifengnews.*':
            dic.setdefault(url,[0,set()])
            dic[url][0]+=1
            dic[url][1].add(uid)
for key in dic:
    print key,'\t',dic[key][0],'\t',len(dic[key][1])
    
    
#dic.setdefault(url,[0,set()])
#如果字典里没有url这个可以，就建一个，对应的value初始值设为一个数列[0,set()]；如果已经有url这个key了，就没有操作
#dic[url][0]+=1
#给字段url这个key的value的这个数列的第一个位就是刚刚那个0加个1
#dic[url][1].add(uid)
#给字段url这个key的value的这个数列的第二位就是刚刚那个set（）数组里面加上这行的用户id
