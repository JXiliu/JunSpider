#-*- coding: utf-8 -*-
#=====================
#=====================

import random,time
import csv,os
import pymysql
import playsound
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

class JianYuSpider(object):

    def __init__(self,cookie,industry:str,day = 1,storage = 'all',runtime = 3.0):
        """
        :param cookie:cookie字符串

        :param industry:需要爬取的行业

        :param day : 查询天数范围

        :param storage:存储情况 ('all','csv','mysql') 可以单独选择存储

        :param runtime:运行时间，默认每天只能运行三小时左右，长时间爬取容易永久封账号
        """
        self.__oneday = 86400
        self.cookie = cookie
        self.runtime = 3600*runtime
        self.storage = storage
        self.exceedpage = list() #
        self.headers = {
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_2) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1309.0 Safari/537.17',
        'cookie': self.cookie,
        'referer': 'https://www.jianyu360.com/jylab/supsearch/index.html',
        'origin': 'https://www.jianyu360.com'
    }
        self.jumptime = self.__oneday * day
        self.industry_dict = {
                        '建筑工程': '建筑工程_材料设备,建筑工程_工程施工,建筑工程_勘察设计,建筑工程_监理咨询,建筑工程_机电安装',
                         '行政办公': '行政办公_专业设备,行政办公_办公用品,行政办公_生活用品,行政办公_通用办公设备,行政办公_办公家具',
                         '医疗卫生': '医疗卫生_设备,医疗卫生_耗材,医疗卫生_药品',
                         '服务采购': '服务采购_仓储物流,服务采购_广告宣传印刷,服务采购_物业,服务采购_其他,服务采购_法律咨询,服务采购_会计,服务采购_审计,服务采购_安保',
                         '机械设备': '机械设备_工程机械,机械设备_车辆,机械设备_其他机械设备,机械设备_机床相关,机械设备_机械零部件,机械设备_矿山机械',
                         '水利水电': '水利水电_水利工程,水利水电_发电工程,水利水电_航运工程,水利水电_其他工程', '弱电安防': '弱电安防_综合布线,弱电安防_智能系统,弱电安防_智能家居',
                         '信息技术': '信息技术_系统集成及安全,信息技术_软件开发,信息技术_运维服务,信息技术_其他',
                         '交通工程': '交通工程_道路,交通工程_轨道,交通工程_桥梁,交通工程_隧道,交通工程_其他',
                         '市政设施': '市政设施_道路,市政设施_绿化,市政设施_线路管网,市政设施_综合项目',
                         '农林牧渔': '农林牧渔_生产物资,农林牧渔_生产设备,农林牧渔_相关服务'}
        self.industry_get = self.industry_dict[industry]
        self.citys =['北京', '天津', '上海', '重庆', '河北', '山西', '辽宁', '吉林', '黑龙江', '江苏', '浙江',
                     '安徽', '福建', '江西', '山东', '河南', '湖北', '湖南', '广东', '海南', '四川', '贵州',
                     '云南', '陕西', '甘肃', '青海', '内蒙古', '广西', '西藏', '宁夏', '新疆']
        self.printf = True
        self.host = '127.0.0.1'
        self.user = 'root'
        self.password = None
        self.db = None
        self.table_name = None
        self.charset = 'utf8'

        if os.path.exists('schedule/') == False:
            os.mkdir('schedule')

    def Save_CSV(self,datelist, city):
        try:
            file = '招标信息公告' + city + '.csv'
            with open(file, 'a', newline='', encoding='utf_8_sig') as f:
                w = csv.writer(f)
                if not os.path.getsize(file):
                    w.writerow(['项目名称', '招标时间', '所属城市', '金额', '招标种类', '行业类别', '招标类型', '招标结果', '详情网址'])
                    for dictdate in datelist:
                        w.writerow(dictdate)
                else:
                    for dictdate in datelist:
                        w.writerow(dictdate)
            if self.printf == True:
                print('csv写入成功！！！')
        except:
            if self.printf == True:
                print('csv写入失败！！！')
            return False

    def Save_MySQL(self, datalist, starttime, stop_the_time):
        db = pymysql.Connect(host=self.host, port=3306, user=self.user, password=self.password, db=self.db,
                             charset=self.charset)
        cursor = db.cursor()

        sql = f"INSERT INTO {self.table_name}(title,publishtime,city,money,buyerclass,s_subscopeclass,toptype,subtype,url) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        for data in datalist:
            try:
                cursor.execute(sql, tuple(data))
                db.commit()
            except Exception as e:
                print(e)
                db.rollback()
                playsound.playsound('dbbase.mp3')
                with open('schedule/SQL_Error.txt', 'a', encoding='utf-8') as f:
                    f.write(str(starttime) + "--" + str(stop_the_time) + ' 写入失败！！' + '\n')
                if self.printf == True:
                    print('写入数据库~~~~~~~~失败')
        if self.printf == True:
            print('数据库保存成功！')
        cursor.close()
        db.close()

    def Schedule(self, city, today, stop_the_time, index):
        nowtime = round(time.time(), 1)
        c_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(nowtime))
        with open('schedule/爬虫进度.txt', 'a', encoding='utf-8') as f:
            f.write('\n' + c_time + '写入日期为:' + today + "下标为：" + str(index) + '#' + city)
            f.close()
        with open('schedule/'+city + '.txt', 'a', encoding='utf-8') as f:
            f.write('\n' + c_time + '写入日期为:' + today + '#' + city + '*' + str(stop_the_time))
            f.close()
        print(f'==当前时间>{c_time}<==数据抓取进度已保存=={city, stop_the_time}==', index)

    def ReadSchedule(self,filename):
        if os.path.exists(filename) == False:
            if filename == '爬虫进度.txt':
                return self.citys[0]
            else:
                return 315504000
        with open(filename, 'r', encoding='utf-8') as fp:
            lines = fp.readlines()
            last_line = lines[-1]
            if filename == '爬虫进度.txt':
                content = last_line.split('#')[1]
                fp.close()
            else:
                content = int(last_line.split('*')[1]) + self.__oneday
                fp.close()
            return content

    def jianyu(self, city,d):
        indexs = self.citys.index(city)
        posturl = 'https://www.jianyu360.com/front/pcAjaxReq'

        for i in range(len(self.citys)):
            starttime = self.ReadSchedule(city + '日志.txt')
            if city in self.exceedpage:
                stop_the_time = starttime
            else:
                stop_the_time = starttime + self.jumptime
            t1 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(starttime))
            t2 = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(stop_the_time))
            if self.printf == True:
                print('\n正在抓取', city, '======进入内层没有异常=========')
                print('抓取日期起止：', t1, '-', t2)
            datelist, dict_list = [], []
            for page in range(1, 11):
                data = {
                    'pageNumber': page,
                    'reqType': 'lastNews',
                    'searchvalue': '',
                    'area': city,
                    'subtype': '',
                    'publishtime': str(starttime) + '_' + str(stop_the_time),
                    'selectType': 'title',
                    'minprice': '',
                    'maxprice': '',
                    'industry': self.industry_get
                }
                if self.printf == True:
                    print(f'正在抓取第{page}页！！！')
                    print(starttime, stop_the_time)
                response = requests.post(posturl, data=data, headers=self.headers, stream=True)
                if d == 1:
                    cookie = response.headers.get('Set-Cookie').split(',')[:1][0]
                    with open('schedule/cookie.txt', 'w', encoding='utf-8') as f:
                        f.write(cookie)
                        f.close()
                response_post = response.json()
                if response_post['list'] == None:
                    if self.printf == True:
                        print('---此页面没有数据----')
                    break
                lenth = len(response_post['list'])
                dict_list = response_post['list']
                s4 = round(random.uniform(26, 30), 1)
                time.sleep(s4)
                # 取数据
                for li in dict_list:
                    date = []
                    if len(li['title']) > 1024:
                        date.append(li['title'][0:1024])
                    else:
                        date.append(li['title'])
                    date.append(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(li['publishtime'])))
                    if 'area' and 'city' in li:
                        date.append(li['area'] + li['city'])
                    else:
                        date.append(li['area'] + '/--')

                    field = ['bidamount', 'buyerclass', 's_subscopeclass', 'toptype', 'subtype']

                    for i in field:
                        if i in li:
                            if len(i) > 1024:
                                date.append(i[0:1024])
                            else:
                                date.append(li[i])
                        else:
                            date.append('--')

                    date.append('https://www.jianyu360.com/article/content/' + li['_id'] + '.html')
                    datelist.append(date)
                if self.printf == True:
                    print('*' * 14, f'此页数据{lenth}条！')
                if lenth < 50:
                    if self.printf == True:
                        print('---后面页面没有数据----')
                    break
            if self.storage == 'all':
                self.Save_MySQL(datelist, starttime, stop_the_time)
                self.Save_CSV(datelist, city)
            elif self.storage == 'csv':
                self.Save_CSV(datelist, city)
            elif self.storage == 'mysql':
                self.Save_MySQL(datelist, starttime, stop_the_time)
            self.Schedule(city, t2, stop_the_time, indexs)
            # 循环城市列表，当长度大于列表时列表下标归零
            indexs += 1
            if indexs + 1 > len(self.citys):
                indexs = 0
            city = self.citys[indexs]

    def Start(self,maxtimestamp = 1577808000):
        """
        :param maxtimestamp: 爬取的最后时间日期
        """

        city = self.ReadSchedule('爬虫进度.txt')
        # 上一次爬取的最后城市
        indexs = self.citys.index(city)

        if os.path.isfile('schedule/cookie.txt'):
            self.cookie = open('schedule/cookie.txt', 'r', encoding='utf-8').read()

        if indexs + 1 >= len(self.citys):
            indexs = 0
            next_city = self.citys[indexs]
        else:
            next_city = self.citys[indexs + 1]
        starttime = self.ReadSchedule(next_city + '日志.txt')

        d = 1
        now_runtime1, now_runtime2, now_runtime3 = time.time(), time.time(), time.time()
        while starttime < maxtimestamp:
            start_runtime = time.time()
            self.jianyu(next_city,d)
            # 起始天数是函数内层终止天数的后一天，不会造成数据重复
            starttime = starttime + self.jumptime
            if self.printf == True:
                print(f'==到达while加天数循环===第{d}次加天数查询==\n\n')
            # 生成随机休眠时间小数
            s0 = round(random.uniform(28.0, 32.0), 1)
            time.sleep(s0)
            d += 1
            if start_runtime - now_runtime3 > self.runtime:
                break
            if start_runtime - now_runtime1 > 720:
                depth_sleep = round(random.uniform(140.0, 160.0), 1)
                if self.printf == True:
                    print(f'====跑虚了，休息{depth_sleep}秒=====\n')
                time.sleep(depth_sleep)
                now_runtime1 = time.time()
            elif start_runtime - now_runtime2 > 480:
                depth_sleep = round(random.uniform(100.0, 130.0), 1)
                if self.printf == True:
                    print(f'====跑累了，歇会{depth_sleep}秒=====\n')
                time.sleep(depth_sleep)
                now_runtime2 = time.time()


def TimedTask(function,args:list = None,hour=8,minute=30):
    """
    :param function:执行函数

    :param args:执行该函数需要的参数

    :param hour: 小时 (int or str)

    :param minute:分钟 (int or str)
    """
    scheduler = BlockingScheduler()
    scheduler.add_job(function, 'cron', hour=hour, minute=minute, args=args)
    scheduler.start()

