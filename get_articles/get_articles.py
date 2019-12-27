# -*- coding: utf-8 -*-
'''
步骤：
1、从数据库提取前提信息
2、根据信息获取所有文章列表，并根据url去重
3、根据url获取所有文章的内容
4、将内容处理之后，存入数据库
5、使用flask框架，读取数据库，将数据库中数据展示在网页中。
'''

import numpy as np
import requests
from lxml import etree
from sqlalchemy import create_engine
import pandas as pd
import logging
from retrying import retry
import datetime
import http.client
import hashlib
import urllib
import re
import random
import time
import pymysql
'''
接口：用于设置日志
'''
class Logging:
    def __init__(self,logfile_path):
        self.logfile_path=logfile_path
        
    def create_log(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(level=logging.INFO)
        logger.handlers = []
        # StreamHandler
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(level=logging.INFO)
        logger.addHandler(stream_handler)
 
        # FileHandler
        file_handler=logging.FileHandler(self.logfile_path, encoding='utf-8')
        file_handler.setLevel(level=logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger,file_handler,stream_handler

'''
接口：调用数据库，数据读取与写入
'''
class UseDatabase:
    def __init__(self,user,password,database,host,port,logger):
        self.user=user
        self.password=password
        self.database=database
        self.host=host
        self.port=port
        self.logger=logger
        
    #连接数据库
    def con_db(self):
        try:
            engine = create_engine('mysql+pymysql://{one}:{two}@{three}:{four}/{five}'.format(one=self.user,two=self.password,three=self.host,four=self.port,five=self.database)) #create_engine
        except:
            self.logger.error('数据库连接失败：', exc_info=True)
        return engine
    
    #读取数据库信息
    def read_db(self,sql):
        engine=self.con_db()
        #conn=engine.connect().connection #创建连接
        try:
            df_db_data=pd.read_sql(sql,con=engine)
        except:
            self.logger.error('从数据库获取数据失败：', exc_info=True)
        return df_db_data
    
    #将信息写入数据库
    def to_db(self,table,info):
        engine=self.con_db()
        try:
            pd.io.sql.to_sql(info,con=engine,name=table,if_exists='append',index=False)
        except:
            self.logger.error('数据插入数据库失败：', exc_info=True)

    def insert_db(self,sql,list_tuple):#info的数据类型为：[(),(),()]元组套列表
        conn = pymysql.connect(database=self.database,user=self.user,password=self.password,host=self.host)
        cur = conn.cursor()
        try:
            cur.executemany(sql,list_tuple)
            conn.commit()
        except:
            conn.rollback()
            self.logger.error('插入数据失败：', exc_info=True)
        cur.close()
        conn.close()

'''
接口:用于爬取文章的列表
'''
class GetArticleTitleList:
    def __init__(self,data_list,logger):
        self.data_list=data_list
        self.logger=logger
    
    #用于拼接网址
    def deal_url(self,url,arg):
        if arg=='OakTable':
            url='http://www.oaktable.net'+url
        elif arg=='Severalnines':
            url='https://severalnines.com'+url
        elif arg=='CitusData':
            url='https://www.citusdata.com'+url
        elif arg=='IDUG':
            url='https://www.idug.org/'+url
        elif arg=='TusaCentral':
            url='http://www.tusacentral.com'+url
        elif arg=='OracleBase':
            url='https://oracle-base.com'+url
        elif arg=='MongoBlog':
            url='https://www.mongodb.com'+url
        elif arg=='OracleAskTom':
            url='https://asktom.oracle.com/pls/apex/'+url
        elif arg=='DARKReading':
            url='https://www.darkreading.com'+url
        elif arg =='eWEEK':
            url='https://www.eweek.com'+url
        return url
    
    @retry(stop_max_attempt_number=3)
    def get_title(self,name,url,xpath_title,xpath_href):
        headers={
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
                }
        response=requests.get(url,headers=headers,timeout=20)
        try:
            html=response.content.decode("utf8","ignore")
        except:
            html=response.content.decode('gbk')
        element=etree.HTML(html)
        title_li_1=element.xpath(xpath_title)
        title_li = []
        for ele in title_li_1:
            title = ele.xpath('string(.)')
            title_li.append(title)
        href_li=element.xpath(xpath_href)
        if len(title_li)>=10:
            title_li=title_li[:10]
        if len(href_li)>=10:
            href_li=href_li[:10]
        df_url_info=pd.DataFrame([title_li,href_li]).T
        df_url_info.columns=['title','url']
        df_url_info['name']=name
        df_url_info['title']=df_url_info['title'].str.strip()
        kw_list=['OakTable','Severalnines','CitusData','IDUG','TusaCentral','OracleBase','MongoBlog','OracleAskTom','DARKReading','eWEEK']
        for kw in kw_list:
            if name==kw:
                df_url_info['url']=df_url_info['url'].apply(self.deal_url,args=(kw,))
        return df_url_info
    
    def get_title_list(self):
        df_title_list=pd.DataFrame([])
        for data in self.data_list:
            name=data['name']
            self.logger.info(name)
            try:
                xpath_title=data['xpath_title']
                xpath_href=data['xpath_href']
                url=data['url_total']
                df_url_info=self.get_title(name,url,xpath_title,xpath_href)#调用get_info函数
                self.logger.info(len(df_url_info.index))
                self.logger.info('条数据获取成功')
            except:
                self.logger.error('网页的url列表获取失败：', exc_info=True)
                continue
            df_title_list=pd.concat([df_title_list,df_url_info])
        df_title_list.drop_duplicates(subset='url',inplace=True)
        df_title_list = df_title_list[~df_title_list['title'].isin([np.NaN,None])]
        return df_title_list
        
'''
接口：用于根据文章列表爬取文章内容
'''
class GetArticleInfoList:
    def __init__(self,df_title_list,logger):
        self.df_title_list=df_title_list
        self.logger=logger
    
    @retry(stop_max_attempt_number=3)
    def get_article(self,name,title,url):
        headers={
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
                }
        response=requests.get(url,headers=headers,timeout=20)
        try:
            html=response.content.decode("utf8","ignore")
        except:
            html=response.content.decode('gbk')
        element=etree.HTML(html)
        article_li=element.xpath("//p//text()")
        img_li=element.xpath("//img/@src")
        article="".join(article_li)
        info_dict={'name':name,'title':title,'url':url,'article':article,'img_li':img_li}
        return info_dict
    
    def get_article_list(self):
        info_dict_list=[]
        for info in self.df_title_list:
            name=info['name']
            title=info['title']
            url=info['url']
            try:
                info_dict=self.get_article(name,title,url)
                self.logger.info(name)
                self.logger.info('的新闻获取成功！')
                info_dict_list.append(info_dict)
            except:
                self.logger.info(url)
                self.logger.error('获取失败!', exc_info=True)
                continue
        df_article_info=pd.DataFrame(info_dict_list)
        df_article_info['get_time']=datetime.datetime.now()
        return df_article_info

'''
接口：用于翻译英文文章标题
'''    
class BaiDuFanYi:
    def __init__(self, logger):
        self.logger=logger
    
    def call_api(self, appid, secretKey, httpClient, myurl, q, fromLang, toLang, salt):
        sign = appid+q+str(salt)+secretKey
        m1 = hashlib.new('md5')
        m1.update(sign.encode())
        sign = m1.hexdigest()
        myurl = myurl+'?appid='+appid+'&q='+urllib.parse.quote(q)+'&from='+fromLang+'&to='+toLang+'&salt='+str(salt)+'&sign='+sign
        try:
            httpClient = http.client.HTTPConnection('api.fanyi.baidu.com')
            httpClient.request('GET', myurl)
         
            #response是HTTPResponse对象
            response = httpClient.getresponse()
            info = response.read().decode('unicode-escape')
        except:
            self.logger.error('获取失败!', exc_info=True)
        finally:
            if httpClient:
                httpClient.close()
        return info
    
    def analysis_info(self, info):
        info_list = re.findall('"dst":"(.*?)"}', info)
        
        info_result = '\n'.join(info_list)
        return info_result
    
    def run(self, appid, secretKey, httpClient, myurl, q, fromLang, toLang, salt):
        
        info1 = self.call_api(appid, secretKey, httpClient, myurl, q, fromLang, toLang, salt)
        info = self.analysis_info(info1)
        return info, info1
    
    
    def fanyi(self, q):
        appid = '20190924000336879' #你的appid
        secretKey = 'SyTX0vDbJZF3cBUrK7LC' #你的密钥
        httpClient = None
        myurl = '/api/trans/vip/translate'
          
        fromLang = 'en'
        toLang = 'zh'
        salt = random.randint(32768, 65536)
        bdfy = BaiDuFanYi(self.logger)
        info_result, info1 = bdfy.run(appid, secretKey, httpClient, myurl, q, fromLang, toLang, salt)
        time.sleep(1)
        #print(info_result)
        if len(info_result) == 0:
            self.logger.info(info1)
        return info_result
    
