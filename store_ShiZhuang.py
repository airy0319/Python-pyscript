# -*- coding: utf-8 -*-
"""
Created on Wed Jul 27 15:18:13 2016

@author: jiaopeng
"""
#Filename:store_ShiZhuang.py
#coding=utf-8
import time,datetime,glob,os,cx_Oracle,sys,urllib2,json

def openDB():
        con=cx_Oracle.connect("app", "app", "10.32.21.77:1521/orcl")
        cur=con.cursor()
        return con,cur

def statOverall(date):#概况
        total_uid = set()#总的用户ID
        date1 = (datetime.datetime.strptime(date,'%Y-%m-%d') + datetime.timedelta(days = -1)).strftime('%Y-%m-%d')
        filename = '/data2/logs/ShiZhuang/total_uid/%s.txt'%date1
        if os.path.exists(filename):
                for line in open(filename):
                        total_uid.add(line.strip())
        total_uv_old = len(total_uid)
        users = set()
        start_pv,odur,pv = 0,0,0
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        #act用户操作的动作     ,rec是记录的简写                   
                        if app != 'ShiZhuang':
                                continue
                        if sysinfo.startswith('android_') and ver == '1.0.1':
                                continue
                        if act == 'pushaccess':#推送到达，不计入DAU
                                continue
                        users.add(uid)
                        total_uid.add(uid)
                        if act == 'in':#启动客户端发送in字段；
                                start_pv += 1#启动次数
                        elif act == 'end':
                                dur = float(rec.split('=')[1])
                                if dur > 0 and dur < 3600:
                                        odur += dur#odur此次启动访问时长，浮点型,单位为秒（锁屏/后台运行不计时长）
                        elif act == 'page':#用户在客户端主动点击生成的页面，均计入PV统计
                                pv += 1
        dau = len(users)#日活跃用户
        total_uv = len(total_uid)#总uv
        new_uv = total_uv - total_uv_old#新增UV
        filename = '/data2/logs/ShiZhuang/total_uid/%s.txt'%date
        fout = open(filename,'w')
        for uid in total_uid:
                print >>fout,uid
        fout.close()
        return new_uv,dau,total_uv,start_pv,int(odur),pv

def storeOverall(date):
        new_uv,dau,total_uv,start_pv,odur,pv = statOverall(date)
        os.environ["NLS_LANG"] = ".UTF8"
        con,cur = openDB()
        sql0 = "delete from f_shizhuang_overall where day = to_date('%s','yyyy-mm-dd')"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql0,e
        sql = "insert into f_shizhuang_overall values(to_date('%s','yyyy-mm-dd'),%d,%d,%d,%d,%d,%d)"%(date,new_uv,dau,total_uv,start_pv,odur,pv)
        try:
                cur.execute(sql)
        except Exception, e:
                print sql,e
        con.commit()#COMMIT命令用于把事务所做的修改保存到数据库，它把上一个COMMIT或ROLLBACK命令之后的全部事务都保存到数据库。
        sql0 = "delete from f_shizhuang_plat_remain where day = to_date('%s','yyyy-mm-dd') and plat = 'all'"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql,e
        sql = "insert into f_shizhuang_plat_remain values(to_date('%s','yyyy-mm-dd'),'all',%d,null,null,null,null,null,null)"%(date,new_uv)
        try:
                cur.execute(sql)
        except Exception, e:
                print sql,e
        con.commit()
        con.close()

def statPlat(date):#平台数据-系统版本
        old_users = set()
        date1 = (datetime.datetime.strptime(date,'%Y-%m-%d') + datetime.timedelta(days = -1)).strftime('%Y-%m-%d')
        filename = '/data2/logs/ShiZhuang/total_uid/%s.txt'%date1
        if os.path.exists(filename):
                for line in open(filename):
                        old_users.add(line.strip())
        dic = {}
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        if app != 'ShiZhuang':
                                continue
                        if sysinfo.startswith('android_') and ver == '1.0.1':
                                continue
                        if act == 'pushaccess':
                                continue
                        dic.setdefault(sysinfo,[set(),set(),0,0,0])
                        dic[sysinfo][1].add(uid)
                        if not uid in old_users:
                                dic[sysinfo][0].add(uid)
                        if act == 'in':
                                dic[sysinfo][2] += 1
                        elif act == 'end':
                                dur = float(rec.split('=')[1])
                                if dur > 0 and dur < 3600:
                                        dic[sysinfo][3] += dur
                        elif act == 'page':
                                dic[sysinfo][4] += 1
        return dic


def storePlat(date):
        data = statPlat(date)
        os.environ["NLS_LANG"] = ".UTF8"
        con,cur = openDB()
        sql0 = "delete from f_shizhuang_plat where day = to_date('%s','yyyy-mm-dd')"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql0,e
        for plat in data:
                new_uv,dau,start_pv,odur,pv = len(data[plat][0]),len(data[plat][1]),data[plat][2],data[plat][3],data[plat][4]
                sql = "insert into f_shizhuang_plat values(to_date('%s','yyyy-mm-dd'),'%s',%d,%d,%d,%d,%d)"%(date,plat,new_uv,dau,start_pv,odur,pv)
                try:
                        cur.execute(sql)
                except Exception, e:
                        print sql,e
        con.commit()
        con.close()


def statVer(date):#客户端版本号
        old_users = set()
        date1 = (datetime.datetime.strptime(date,'%Y-%m-%d') + datetime.timedelta(days = -1)).strftime('%Y-%m-%d')
        filename = '/data2/logs/ShiZhuang/total_uid/%s.txt'%date1
        if os.path.exists(filename):
                for line in open(filename):
                        old_users.add(line.strip())
        dic = {}
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        if app != 'ShiZhuang':
                                continue
                        mos=sysinfo.split('_')[0].strip().lower()
                        ##pushpv,pushuv,openpushpv,openpushuv,pv,uv,newcomer,start,olast,olast_num
                        ##  0      1        2           3     4   5    6       7    8      9
                        dic.setdefault(mos,{}).setdefault(ver,[0,set(),0,set(),0,set(),set(),0,0,0])
                        if act=='pushaccess':
                                dic[mos][ver][0] +=1
                                dic[mos][ver][1].add(uid)
                                continue
                        elif act=='openpush':
                                dic[mos][ver][2] +=1
                                dic[mos][ver][3].add(uid)
                        elif act=='in':
                                dic[mos][ver][7] +=1
                        elif act=='end':
                                try:
                                        odur=float(rec.split('odur=')[1].split('$')[0])
                                        if odur>0 and odur<3600:
                                                olast=odur
                                                olast_num=1
                                                dic[mos][ver][8] +=olast
                                                dic[mos][ver][9] +=olast_num
                                except Exception,e:
                                        print e,'end',rec
                        elif act=='page':
                                dic[mos][ver][4] +=1
                        dic[mos][ver][5].add(uid)
                        if uid not in old_users:
                                dic[mos][ver][6].add(uid)
        for mos1 in dic:
                for ver1 in dic[mos1]:
                        print mos1,ver1,dic[mos1][ver1][0],len(dic[mos1][ver1][1]),dic[mos1][ver1][2],len(dic[mos1][ver1][3]),dic[mos1][ver1][4],len(dic[mos1][ver1][5]),len(dic[mos1][ver1][6]),dic[mos1][ver1][7],dic[mos1][ver1][8],dic[mos1][ver1][9]

        return dic


def storeVer(date):
        befdate=time.strftime('%Y-%m-%d',time.localtime(time.mktime(time.strptime(date,'%Y-%m-%d'))-86400))
        print befdate,date
        dic=statVer(date)
        con,cur=openDB()
        del_sql="delete from app.f_shizhuang_ver where day=to_date('%s','yyyy-mm-dd')"%date
        try:
                cur.execute(del_sql)
        except Exception, e:
                print del_sql,e
        for mos1 in dic:
                for ver1 in dic[mos1]:
                        tmp=(mos1,ver1,dic[mos1][ver1][0],len(dic[mos1][ver1][1]),dic[mos1][ver1][2],len(dic[mos1][ver1][3]),dic[mos1][ver1][4],len(dic[mos1][ver1][5]),len(dic[mos1][ver1][6]),dic[mos1][ver1][7],dic[mos1][ver1][8],dic[mos1][ver1][9],date)
                        insert_sql="insert into app.f_shizhuang_ver (MOS,VER,PUSHPV,PUSHUV,OPENPUSHPV,OPENPUSHUV,PV,UV,NEWCOMER,START_PV,OLAST,OLAST_NUM,DAY) values (:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,to_date(:13,'yyyy-mm-dd'))"
                        try:
                                cur.execute(insert_sql,tmp)
                                con.commit()
                        except Exception,e:
                                print insert_sql,e

                        update_sql="update app.f_shizhuang_ver set totaluv=to_number(nvl(newcomer,0))+(select to_number(nvl(totaluv,0)) from app.f_shizhuang_ver where day=to_date('%s','yyyy-mm-dd') and mos='%s' and ver='%s') where day=to_date('%s','yyyy-mm-dd') and mos='%s' and ver='%s'"%(befdate,mos1,ver1,date,mos1,ver1)
                        try:
                                cur.execute(update_sql)
                                con.commit()
                        except Exception,e:
                                print update_sql,e
        con.close()
        
def statPlatOverall(date):#平台数据-平台概况
        old_users = set()
        date1 = (datetime.datetime.strptime(date,'%Y-%m-%d') + datetime.timedelta(days = -1)).strftime('%Y-%m-%d')
        filename = '/data2/logs/ShiZhuang/total_uid/%s.txt'%date1
        if os.path.exists(filename):
                for line in open(filename):
                        old_users.add(line.strip())
        dic = {}
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        if app != 'ShiZhuang':
                                continue
                        if sysinfo.startswith('android_') and ver == '1.0.1':
                                continue
                        if act == 'pushaccess':
                                continue
                        sysinfo = sysinfo.split('_')[0]
                        if sysinfo == '#':
                                continue
                        dic.setdefault(sysinfo,[set(),set(),0,0,0])
                        dic[sysinfo][1].add(uid)
                        if not uid in old_users:
                                dic[sysinfo][0].add(uid)
                        if act == 'in':
                                dic[sysinfo][2] += 1
                        elif act == 'end':
                                dur = float(rec.split('=')[1])
                                if dur > 0 and dur < 3600:
                                        dic[sysinfo][3] += dur
                        elif act == 'page':
                                dic[sysinfo][4] += 1
        return dic

def storePlatOverall(date):
        data = statPlatOverall(date)
        os.environ["NLS_LANG"] = ".UTF8"
        con,cur = openDB()
        sql0 = "delete from f_shizhuang_plat_overall where day = to_date('%s','yyyy-mm-dd')"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql0,e
        for plat in data:
                new_uv,dau,start_pv,odur,pv = len(data[plat][0]),len(data[plat][1]),data[plat][2],data[plat][3],data[plat][4]
                sql = "insert into f_shizhuang_plat_overall values(to_date('%s','yyyy-mm-dd'),'%s',%d,%d,%d,%d,%d)"%(date,plat,new_uv,dau,start_pv,odur,pv)
                try:
                        cur.execute(sql)
                except Exception, e:
                        print sql,e
        con.commit()
        sql0 = "delete from f_shizhuang_plat_remain where day = to_date('%s','yyyy-mm-dd') and plat <> 'all'"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql,e
        for plat in data:
                new_uv = len(data[plat][0])
                if new_uv == 0:
                        continue
                sql = "insert into f_shizhuang_plat_remain values(to_date('%s','yyyy-mm-dd'),'%s',%d,null,null,null,null,null,null)"%(date,plat,new_uv)
                try:
                        cur.execute(sql)
                except Exception, e:
                        print sql,e
        con.commit()
        con.close()

def statCh(date):#渠道数据
        old_users = set()
        date1 = (datetime.datetime.strptime(date,'%Y-%m-%d') + datetime.timedelta(days = -1)).strftime('%Y-%m-%d')
        filename = '/data2/logs/ShiZhuang/total_uid/%s.txt'%date1
        if os.path.exists(filename):
                for line in open(filename):
                        old_users.add(line.strip())
        dic = {}
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        if app != 'ShiZhuang':
                                continue
                        if sysinfo.startswith('android_') and ver == '1.0.1':
                                continue
                        if act == 'pushaccess':
                                continue
                        dic.setdefault(ch,[set(),set(),0,0,0])#newcomer,dau,in,dur,pv
                        dic[ch][1].add(uid)
                        if not uid in old_users:
                                dic[ch][0].add(uid)
                        if act == 'in':
                                dic[ch][2] += 1
                        elif act == 'end':
                                dur = float(rec.split('=')[1])
                                if dur > 0 and dur < 3600:
                                        dic[ch][3] += dur
                        elif act == 'page':
                                dic[ch][4] += 1
        return dic

def storeCh(date):
        data = statCh(date)
        os.environ["NLS_LANG"] = ".UTF8"
        con,cur = openDB()
        sql0 = "delete from f_shizhuang_ch where day = to_date('%s','yyyy-mm-dd')"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql0,e
        for ch in data:
                new_uv,dau,start_pv,odur,pv = len(data[ch][0]),len(data[ch][1]),data[ch][2],data[ch][3],data[ch][4]
                sql = "insert into f_shizhuang_ch values(to_date('%s','yyyy-mm-dd'),'%s',%d,%d,%d,%d,%d)"%(date,ch,new_uv,dau,start_pv,odur,pv)
                try:
                        cur.execute(sql)
                except Exception, e:
                        print sql,e
        con.commit()
        con.close()

def getNewcomerUid(date):#获取新增用户
        old_users,new_users = set(),set()
        date1 = (datetime.datetime.strptime(date,'%Y-%m-%d') + datetime.timedelta(days = -1)).strftime('%Y-%m-%d')
        filename = '/data2/logs/ShiZhuang/total_uid/%s.txt'%date1
        if os.path.exists(filename):
                for line in open(filename):
                        old_users.add(line.strip())
        filename = '/data2/logs/ShiZhuang/total_uid/%s.txt'%date
        if os.path.exists(filename):
                for line in open(filename):
                        uid = line.strip()
                        if not uid in old_users:
                                new_users.add(uid)
        return new_users

def statPlatRemain(date):#留存分析
        days_list = [-1,-3,-7,-15,-30,-90]
        new_users = []
        for i in range(6):
                date1 = (datetime.datetime.strptime(date,'%Y-%m-%d') + datetime.timedelta(days = days_list[i])).strftime('%Y-%m-%d')
                new_users.append(getNewcomerUid(date1))
        remain_users = [{},{},{},{},{},{}]
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        if app != 'ShiZhuang':
                                continue
                        if sysinfo.startswith('android_') and ver == '1.0.1':
                                continue
                        if act == 'pushaccess':
                                continue
                        sysinfo = sysinfo.split('_')[0]
                        if sysinfo == '#':
                                continue
                        for i in range(6):
                                if uid in new_users[i]:
                                        remain_users[i].setdefault(sysinfo,set())
                                        remain_users[i][sysinfo].add(uid)
                                        remain_users[i].setdefault('all',set())
                                        remain_users[i]['all'].add(uid)
        return remain_users

def storePlatRemain(date):
        remain_users = statPlatRemain(date)
        days_list = [-1,-3,-7,-15,-30,-90]
        column_name = ['one','three','seven','fifteen','thirty','ninety']
        os.environ["NLS_LANG"] = ".UTF8"
        con,cur = openDB()
        for i in range(6):
                date1 = (datetime.datetime.strptime(date,'%Y-%m-%d') + datetime.timedelta(days = days_list[i])).strftime('%Y-%m-%d')
                for plat in remain_users[i]:
                        sql = "update f_shizhuang_plat_remain set %s = %d where day = to_date('%s','yyyy-mm-dd') and plat = '%s'"%(column_name[i],len(remain_users[i][plat]),date1,plat)
                        try:
                                cur.execute(sql)
                        except Exception, e:
                                print sql,e
                sql = "update f_shizhuang_plat_remain set %s = 0 where day = to_date('%s','yyyy-mm-dd') and %s is null"%(column_name[i],date1,column_name[i])
                try:
                        cur.execute(sql)
                except Exception, e:
                        print sql,e
        con.commit()
        con.close()

def statDoc(date):#内容-点击排行，单页流量趋势
        dic = {}
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        if app != 'ShiZhuang':
                                continue
                        if sysinfo.startswith('android_') and ver == '1.0.1':
                                continue
                        if act == 'page':
                                if not 'id=' in rec:
                                        continue
                                id = rec.split('id=')[1].split('$')[0].strip()
                                dic.setdefault(id,[0,set()])
                                dic[id][0] += 1
                                dic[id][1].add(uid)
        return dic

def storeDoc(date):
        data = statDoc(date)
        os.environ["NLS_LANG"] = ".UTF8"
        con,cur = openDB()
        sql0 = "delete from f_shizhuang_doc where day = to_date('%s','yyyy-mm-dd')"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql0,e
        for id in data:
                pv,uv = data[id][0],len(data[id][1])
                sql = "select title,recommend from d_shizhuang_doc where docid = '%s'"%id
                title,recommend = '',''
                try:
                        cur.execute(sql)
                        for [title,recommend] in cur.fetchall():
                                pass
                        if title == '':
                                url = 'http://10.90.3.41:8100/api/GetTopicData?id=%s'%(id)
                                content = urllib2.urlopen(url).read()
                                dic = json.loads(content)
                                key = dic['d']
                                print id,len(key)
                                if len(key) > 0:
                                        title,user_id,publishtime,rec_id = key['title'],key['user_id'],key['publishtime'],key['recommend']
                                        if title=='' or title==None or len(title)==0:
                                                title = key['title2']
                                        print id,rec_id
                                        if rec_id == '2':
                                                recommend = u'精选'
                                        elif rec_id == '0':
                                                recommend = u'广场'
                                        elif rec_id == '1':
                                                recommend = u'广场'
                                        sql = "insert into d_shizhuang_doc values('%s','%s','%s','%s',%d)"%(id,title,recommend,user_id,publishtime)
                                        cur.execute(sql)
                except Exception, e:
                        title,recommend = '',''
                        print 'd_shizhuang_doc',id,e

                sql = "insert into f_shizhuang_doc values(to_date('%s','yyyy-mm-dd'),'%s','%s','%s',%d,%d)"%(date,id,title,recommend,uv,pv)
                try:
                        cur.execute(sql)
                except Exception, e:
                        print 'f_shizhuang_doc',id,uv,pv,e
        con.commit()
        con.close()

def statDocRef(date):#内容-单页来源
        dic = {}
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        if app != 'ShiZhuang':
                                continue
                        if sysinfo.startswith('android_') and ver == '1.0.1':
                                continue
                        if act == 'page':
                                if not 'id=' in rec:
                                        continue
                                id = rec.split('id=')[1].split('$')[0].strip()
                                if not 'ref=' in rec:
                                        ref = '#'
                                else:
                                        ref = rec.split('ref=')[1].split('$')[0].strip()
                                        if ref == '':
                                                ref = '#'
                                dic.setdefault(id,{})
                                dic[id].setdefault(ref,[0,set()])
                                dic[id][ref][0] += 1
                                dic[id][ref][1].add(uid)
        return dic

def storeDocRef(date):
        data = statDocRef(date)
        os.environ["NLS_LANG"] = ".UTF8"
        con,cur = openDB()
        sql0 = "delete from f_shizhuang_doc_ref where day = to_date('%s','yyyy-mm-dd')"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql0,e
        for id in data:
                for ref_id in data[id]:
                        pv,uv = data[id][ref_id][0],len(data[id][ref_id][1])
                        sql = "select title from d_shizhuang_doc where docid = '%s'"%id
                        title = ''
                        try:
                                cur.execute(sql)
                                for [title,] in cur.fetchall():
                                        pass
                        except:
                                pass
                        if ref_id == '#':
                                ref_title = ''
                        elif ref_id == 'push':
                                ref_title = '推送'
                        else:
                                sql = "select title from d_shizhuang_doc where docid = '%s'"%ref_id
                                ref_title = ''
                                try:
                                        cur.execute(sql)
                                        for [ref_title,] in cur.fetchall():
                                                pass
                                except:
                                        pass
                        sql = "insert into f_shizhuang_doc_ref values(to_date('%s','yyyy-mm-dd'),'%s','%s','%s','%s',%d,%d)"%(date,id,title,ref_id,ref_title,uv,pv)
                        try:
                                cur.execute(sql)
                        except Exception, e:
                                print 'f_shizhuang_doc_ref',id,ref_id,e
        con.commit()
        con.close()

def statPlatOpen(date):#概况-打开方式
        dic = {}
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        if app != 'ShiZhuang':
                                continue
                        if sysinfo.startswith('android_') and ver == '1.0.1':
                                continue
                        if act != 'in':
                                continue
                        sysinfo = sysinfo.split('_')[0]
                        if sysinfo == '#':
                                continue
                        dic.setdefault(sysinfo,[set(),set(),0,0])
                        if 'type=direct' in rec:
                                dic[sysinfo][0].add(uid)
                                dic[sysinfo][2] += 1
                        elif 'type=push' in rec:
                                dic[sysinfo][1].add(uid)
                                dic[sysinfo][3] += 1
        return dic

def storePlatOpen(date):
        data = statPlatOpen(date)
        os.environ["NLS_LANG"] = ".UTF8"
        con,cur = openDB()
        sql0 = "delete from f_shizhuang_plat_open where day = to_date('%s','yyyy-mm-dd')"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql0,e
        for plat in data:
                direct_uv,push_uv,direct_pv,push_pv = len(data[plat][0]),len(data[plat][1]),data[plat][2],data[plat][3]
                sql = "insert into f_shizhuang_plat_open values(to_date('%s','yyyy-mm-dd'),'%s',%d,%d,%d,%d)"%(date,plat,direct_uv,push_uv,direct_pv,push_pv)
                try:
                        cur.execute(sql)
                except Exception, e:
                        print sql,e
        con.commit()
        con.close()
        
def statAction(date):#概况-用户动作
        dic = {}
        for filename in glob.glob('/data2/logs/ShiZhuang/%s/????.sta'%date):
                for line in open(filename):
                        tmp = line.split('\t')
                        if len(tmp) < 14:
                                continue
                        app,ip,sysinfo,ver,ch,uid,hardid,ua,net,act,rec = tmp[0].strip(),tmp[1].strip(),tmp[2].strip(),tmp[3].strip(),tmp[4].strip(),tmp[5].strip(),tmp[6].strip(),tmp[7].strip(),tmp[8].strip(),tmp[12].strip(),tmp[13].strip()
                        if app != 'ShiZhuang':
                                continue
                        if sysinfo.startswith('android_') and ver == '1.0.1':
                                continue
                        if act == 'action':
                                if not 'type=' in rec:
                                        continue
                                type = rec.split('type=')[1].split('$')[0].strip()
                                if 'status=no' in rec:
                                        dic.setdefault(type,[set(),0,set(),0])
                                        dic[type][3] += 1
                                        dic[type][2].add(uid)
                                elif 'status=yes' in rec or not 'status=' in rec:
                                        dic.setdefault(type,[set(),0,set(),0])
                                        dic[type][1] += 1
                                        dic[type][0].add(uid)
        return dic

def storeAction(date):
        data = statAction(date)
        os.environ["NLS_LANG"] = ".UTF8"
        con,cur = openDB()
        sql0 = "delete from f_shizhuang_action where day = to_date('%s','yyyy-mm-dd')"%date
        try:
                cur.execute(sql0)
        except Exception, e:
                print sql0,e
        for type in data:
                yes_uv,yes_pv,no_uv,no_pv = len(data[type][0]),data[type][1],len(data[type][2]),data[type][3]
                if type == 'publish':
                        atype = u'发布'
                elif type == 'leavemessage':
                        atype = u'留言'
                elif type == 'buy':
                        atype = u'购买'
                elif type == 'pushoff':
                        atype = u'推送关'
                elif type == 'pushon':
                        atype = u'推送开'
                elif type == 'hotlab':
                        atype = u'热门标签'
                else:
                        print date,type,yes_uv,yes_pv,no_uv,no_pv
                        continue
                #print date,atype,yes_uv,yes_pv,no_uv,no_pv
                sql = "insert into f_shizhuang_action values(to_date('%s','yyyy-mm-dd'),'%s',%d,%d,%d,%d)"%(date,atype,yes_uv,yes_pv,no_uv,no_pv)
                try:
                        cur.execute(sql)
                except Exception, e:
                        print sql,e
        con.commit()
        con.close()

if __name__ == '__main__':
        print '===start: %s==='%time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
        date = (datetime.date.today() + datetime.timedelta(days = -1)).strftime('%Y-%m-%d')
        func_list = [storeOverall,storePlat,storeVer,storePlatOverall,storeCh,storePlatRemain,storeDoc,storeDocRef,storePlatOpen,storeAction]
        #func_list = [storeAction]
        for func in func_list:
                try:
                        func(date)
                except Exception, e:
                        print func,e
        print '===done: %s==='%time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))