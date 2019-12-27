# -*- coding: utf-8 -*-
import requests
from lxml import etree
from sqlalchemy import create_engine
import pandas as pd
import re
import time
import datetime
from retrying import retry
import pymysql
import json
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
接口:用于处理可以用xpath提取到的网站的{['name':'~','title':'~','url':'~'],[],[]]]}
'''
class SpiderNewsUrl:
    def __init__(self,data_list,logger):
        self.data_list=data_list
        self.logger=logger
    
    #拼接谷歌新闻网址
    @retry(stop_max_attempt_number=3)
    def deal_url1(self,a):
        a=re.sub('\.','https://news.google.com',a)#拼接为完整链接
        headers={
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
        }
        response=requests.get(a,headers=headers,timeout=5)
        try:
            html=response.content.decode()
        except:
            html=response.content.decode('gbk')
        ret=etree.HTML(html)
        data= "".join(ret.xpath("//c-wiz/@data-p"))
        a=re.search(r'(http[s]?:.*?)\"', data, re.S).group(1)
        return a
    
    def deal_urldzone(self,b):
        b='https://dzone.com'+b
        return b    

    @retry(stop_max_attempt_number=3)    
    def get_info(self,name,url,xpath_title,xpath_href):
        headers={
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
                }
        response=requests.get(url,headers=headers,timeout=15)
        try:
            html=response.content.decode()
        except:
            html=response.content.decode('gbk')
        if name == 'infoq':
            element=etree.XML(html)
        else:
            element=etree.HTML(html)
        try:
            title_li_element=element.xpath(xpath_title)
            title_li=[]
            for i in title_li_element:
                title=i.xpath("string(./a)")
                title_li.append(title)
        except:
            title_li=element.xpath(xpath_title)
        href_li=element.xpath(xpath_href)
        if name == 'infoq':
            title_li=title_li[1:]
            href_li=href_li[1:]
        df_url_info=pd.DataFrame([title_li,href_li]).T
        df_url_info.columns=['title','url']
        df_url_info['name']=name
        if name=='谷歌新闻':
            df_url_info['url']=df_url_info['url'].apply(self.deal_url1)
        if name=='dzone':
            df_url_info['url']=df_url_info['url'].apply(self.deal_urldzone)
        return df_url_info
    
    #根据name，循环调用get_info,获得{['name':'','title':'','url':''],[],[]}
    def get_url_info(self):
        df_url_list=pd.DataFrame([])
        for data in self.data_list:
            name=data['name']
            xpath_title=data['xpath_title']
            xpath_href=data['xpath_href']
            url=data['url']
            try:
                df_url_info=self.get_info(name,url,xpath_title,xpath_href)#调用get_info函数
                df_url_list=pd.concat([df_url_list,df_url_info])
                self.logger.info(name)
                self.logger.info('获取成功')
            except:
                self.logger.info(name)
                self.logger.info('time_out')
                continue
        df_url_list.drop_duplicates(subset='title',inplace=True)
        return df_url_list
    
'''
接口：用于处理第一段爬取的信息
1、处理成合适的格式
2、标题去重(未完成)
3、把新的数据放入去重表
'''

class DealUrlInfo:
    def __init__(self,df_url_list,logger):
        self.df_url_list=df_url_list
        self.logger=logger
    
    #处理获取到的信息
    def deal_info(self):
        df_url_list=self.df_url_list
        #处理到合适的格式
        df_url_list['title']=df_url_list['title'].str.strip()
        
        #与之前新闻对比去重
        
        return df_url_list
    
'''
接口：对获取到的网址进行爬取文章内容
'''
class SpiderNews:
    def __init__(self,df_url_list,logger):
        self.df_url_list=df_url_list
        self.logger=logger
    
    #获取单个url信息：{‘name’:'title':'','url':'','info':''}
    @retry(stop_max_attempt_number=3)
    def get_info(self,name,title,url):
        if name == 'infoq':
            try:
                headers={
                'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36',
                'Referer': url
                }
                uuid = re.findall('https://www.infoq.cn/article/(.*?)\?',url)[0]
                data = json.dumps({"uuid": uuid})
                response=requests.post('https://www.infoq.cn/public/v1/article/getDetail',data=data,headers=headers)
                try:
                    html=response.content.decode()
                except:
                    html=response.content.decode('GB18030')
                html = json.loads(html)['data']['content']
                img_url=[]
                #清洗
                info = html.replace('<p>','').replace('</p>','\n')#换行
                info = re.sub('<(.*?)>','',html)#去除图片链接
                info = "".join([s for s in info.splitlines(True) if s.strip()])#删除空行
            except Exception as e:
                print(e)
                img_url=[]
                info=''
        else:
            try:
                headers={
                        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.90 Safari/537.36'
                        }
                response=requests.get(url,headers=headers,timeout=15)
                try:
                    html=response.content.decode()
                except:
                    html=response.content.decode('GB18030')
                element=etree.HTML(html)
                if name=='新浪新闻':
                    info_element=element.xpath('//p/font')
                    if len(info_element)==0:
                        info_element=element.xpath('//p')
                else:
                    info_element=element.xpath('//p')
                info_li=[]
                for i in info_element:
                    info=i.xpath("string(.)")
                    info_li.append(info)
                img_url=element.xpath('//img/@src')
                info="\n".join(info_li)
                info = "".join([s for s in info.splitlines(True) if s.strip()])#删除空行
            except:
                img_url=[]
                info=''
        
        info_dict={'name':name,'title':title,'url':url,'info':info,'img_url':img_url}
        return info_dict
    
    #循环遍历获得所有的信息
    def get_news_info(self):
        info_dict_list=[]
        for info in self.df_url_list:
            time.sleep(0)
            name=info['name']
            title=info['title']
            url=info['url']
            try:
                info_dict=self.get_info(name,title,url)
                self.logger.info(name)
                self.logger.info('的新闻获取成功！')
                info_dict_list.append(info_dict)
            except:
                self.logger.info(url)
                self.logger.error('获取失败!', exc_info=True)
                continue
        df_news_info=pd.DataFrame(info_dict_list)
        df_news_info['get_time']=datetime.datetime.now()
        return df_news_info
    

