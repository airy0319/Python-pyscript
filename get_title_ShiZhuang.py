# -*- coding: utf-8 -*-
"""
Created on Wed Jul 27 09:32:03 2016

@author: jiaopeng
"""

#Filename:get_title_ShiZhuang.py
#coding=utf-8
import cx_Oracle,os,time
import urllib2,json,datetime

print '===start: %s==='%time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

dic_recommend={0:u'广场.',1:u'广场.',2:u'精选.'}

os.environ["NLS_LANG"] = ".UTF8"
con = cx_Oracle.connect("app", "app", "10.32.21.77:1521/orcl")
cur = con.cursor()

date = (datetime.date.today() + datetime.timedelta(days = -1)).strftime('%Y-%m-%d')
date_today = datetime.date.today().strftime('%Y-%m-%d')
print 'Date:',date
for recommend_id in dic_recommend:
        url = 'http://10.90.3.41:8100/api/GetTopicTitleListByDate?beginTime=%s&endTime=%s&all=1&recommend=%s'%(date,date_today,recommend_id)#%s字符串匹配
        content = urllib2.urlopen(url).read()
        dic = json.loads(content)
        print dic
        recommend = dic_recommend[recommend_id]
        for key in dic['d']['list']:
                try:
                        id,title,user_id,publishtime = key['id'],key['title'],key['user_id'],key['publishtime']
                        if title=='' or title==None or len(title)==0:
                                title=key['title2']
                        sql = "insert into d_shizhuang_doc values('%s','%s','%s','%s',%d)"%(id,title,recommend,user_id,publishtime)
                        print sql
                        print id
                        cur.execute(sql)
                except Exception, e:
                        print id,e
                        continue
con.commit()#COMMIT命令用于把事务所做的修改保存到数据库，它把上一个COMMIT或ROLLBACK命令之后的全部事务都保存到数据库
con.close()

print '===done: %s==='%time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))