#Filename:transform.py
# -*- coding: utf-8 -*-
#解析日志脚本
"""
Created on Tue Jul 26 17:51:04 2016

@author: jiaopeng
"""

# /usr/local/python-2.7.8/bin/python
import time
import urllib
import os
import traceback
import sys
import datetime
import gzip


base_path_format = '/data/logs/nginx/dig/access_dig.%s'#当天原始日志路径
base_path_format_day = '/data/logs/nginx/dig/%s/access_dig.%s%s'#历史日志路径？

transform_items = {
"fhtttest": {"app_type": "app=fhtttest&", "out_path": "/data2/logs/fhtttest/", "mode": "noappend"},
"fhttreg": {"app_type": "app=fhttreg&", "out_path": "/data2/logs/fhttreg/", "mode": "noappend"},
"fhty": {"app_type": "app=fhty&", "out_path": "/data2/logs/fhty/", "mode": "noappend"},
"fhtytest": {"app_type": "app=fhtytest&", "out_path": "/data2/logs/fhty/", "mode": "append"},
"fhyl": {"app_type": "app=fhyl&", "out_path": "/data2/logs/fhyl/", "mode": "noappend"},

"ShiZhuang": {"app_type": "app=ShiZhuang&", "out_path": "/data2/logs/ShiZhuang/", "mode": "noappend"}
}


def parse_line(line):
    dic_parse = {
'app': '#',#客户端产品名
'ip': '#',
'os': '#',#操作系统
'ver': '#',#客户端版本号
'pub': '#',#渠道号
'uid': '#',#用户唯一标识
'hardid': '#',#设备ID
'ua': '#',#设备名称，空格“ ”一律替换为“_”
'net': '#',#网络环境，6种情况
'tmfirst': '#',#最早安装的时间戳
'tmsetup': '#',#本次安装的时间戳
'tmopa': '#',#操作发生的时间
'opa': '#',#操作的动作
'detail': '#'#操作的内容
}
    items = line.split(",")
    dic_parse['ip'] = items[0]
    session = items[3]#用户行为，必须urlencode
    #采集用户操作行为数据，每条session可以包含多个操作记录（record）。
    if session.startswith(r'''"GET /'''):#判断有没有以http：sisson会话是不是以get形式。开始
        for item in session.split('&'):
            name, data = item.split('=')#以=分割，前后分别赋值给name,data
            if not data:#如果data不存在
                data = '#'
            if name.endswith("app"):#判断name是否以app结尾
                dic_parse['app'] = data.split()[0].strip()
                continue

            elif name.endswith("ch"):
                dic_parse['pub'] = data.split()[0].strip()
                continue

            elif name.endswith("firsttm"):
                dic_parse['tmfirst'] = data.split()[0].strip()
                continue

            elif name.endswith("setuptm"):
                dic_parse['tmsetup'] = data.split()[0].strip()
                continue

            elif name.endswith("uid"):
                dic_parse['uid'] = data.split()[0].strip()
                continue

            elif name.endswith("hardid"):
                dic_parse['hardid'] = data.split()[0].strip()
                continue

            elif name.endswith("net"):
                dic_parse['net'] = data.split()[0].strip()
                continue

            elif name.endswith("os"):
                dic_parse['os'] = data.split()[0].strip()
                continue

            elif name.endswith("ver"):
                dic_parse['ver'] = data.split()[0].strip()
                continue

            elif name.endswith("ua"):#设备名称，空格“ ”一律替换为“_”
                dic_parse['ua'] = data.split()[0].strip()
                continue

            elif name.endswith("session"):#采集用户操作行为数据，每条session可以包含多个操作记录（record）
                unsession = urllib.unquote(data)#对字符串进行解码，返回值字符串
                records = []#记录（record）列表
                for rec in unsession.split(" HTTP")[0].split("@"):#session由多个record组成，以@分隔:
                    dic_parse_tmp = {}
                    try:
                        tmopa, opa, detail = rec.split("#")
                        #record表示每条操作数据，结构为：（以#作为分隔符号）
                        #操作发生的时间#操作的动作#操作的内容
                        #print tmopa, opa, detail
                    except:
                        print line.strip()
                        continue
                    dic_parse_tmp['tmopa'] = tmopa.split()[0].strip()
                    dic_parse_tmp['opa'] = opa.split()[0].strip()
                    if detail:
                        dic_parse_tmp['detail'] = detail.split()[0].strip()
                    records.append(dic_parse_tmp)
                dic_parse["session"] = records
                #print dic_parse["session"] 
                continue

        if "session" in dic_parse:
            records = []
            for rec in dic_parse["session"]:
                dic_parse_tmp = rec#如果rec在dic_parse的session键的值中，把rec赋给dic_parse_tmp
                for key in dic_parse:
                    if key not in ('session','tmopa','opa','detail'):
                        dic_parse_tmp[key] = dic_parse[key]#如果key不在这些键里，传递key给dic_parse_tmp{}
                records.append(dic_parse_tmp)#追加records
            return records
    return [dic_parse]#返回dic_parse字典列表？
    
def transform(tm, mode = "append"):#解析函数，模式为追加模式
    dayStr = tm[:8]#取时间的前8位
    dayStr = "-".join([dayStr[:4], dayStr[4:6], dayStr[6:]])#以-连接日期
    tm_tail = tm[8:]#尾部是tm后面的第9位以后的
    dic_out = {}
    for key in transform_items:
        if not os.path.exists("".join([transform_items[key]["out_path"], dayStr, '/'])):
            os.system("mkdir -p %s%s%s" % (transform_items[key]["out_path"], dayStr, '/'))
            #如果不是已存在的路径，创建路径
        if transform_items[key]["mode"] == "append":
            dic_out.setdefault(key, open(''.join([transform_items[key]["out_path"], dayStr, '/', tm_tail, '.sta']), 'a'))
            #字典默认设置
        else:
            dic_out.setdefault(key, open(''.join([transform_items[key]["out_path"], dayStr, '/', tm_tail, '.sta']), 'w'))
    if os.path.exists(base_path_format % tm):
        iszip = False
        f = open(base_path_format % tm)
    else:
        iszip = True
        f = gzip.open(base_path_format_day % (tm[:8], tm, ".gz"))#解压缩文件
    # with open(base_path_format % tm) as f:
    for line in f:
        app_type = ''
        for key in transform_items:
            if transform_items[key]["app_type"] in line:
                app_type = key
                break
        if not app_type:
            continue
        try:
            record = parse_line(line)
        except:
            print traceback.print_exc()#try..except捕获异常,然后traceback.print_exc()打印
            print line
            continue
        for parse_dic in record:
            print >> dic_out[app_type], '\t'.join([parse_dic['app'], parse_dic['ip'], parse_dic['os'], parse_dic['ver'], parse_dic['pub'], parse_dic['uid'], parse_dic['hardid'], parse_dic['ua'], parse_dic['net'], parse_dic['tmfirst'], parse_dic['tmsetup'], parse_dic['tmopa'], parse_dic['opa'], parse_dic['detail']])
    # if not iszip:
    f.close()
    for key in dic_out:
        dic_out[key].close()
        
def main(num):
    tm = time.strftime('%Y%m%d%H%M', time.localtime(time.time()-num*60))
    transform(tm)


if __name__ == "__main__":#让你写的脚本模块既可以导入到别的模块中用，另外该模块自己也可执行
    if 'test' in sys.argv:#sys.argv[]是用来获取命令行参数的，sys.argv[0]表示代码本身文件路径
        a = datetime.datetime.strptime("201512091130", "%Y%m%d%H%M")#将字符串格式化成时间
        endtime = datetime.datetime.strptime("201512091400", "%Y%m%d%H%M")
        while a < endtime:
            transform(a.strftime("%Y%m%d%H%M"))
            a += datetime.timedelta(minutes = 1)#当前时间向后自动加1分钟的时间
    if 'normal' in sys.argv:
        main(3)