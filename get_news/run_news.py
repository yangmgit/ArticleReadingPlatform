# -*- coding: utf-8 -*-
from get_news import UseDatabase
from get_news import SpiderNewsUrl
from get_news import DealUrlInfo
from get_news import SpiderNews
import logging
import pandas as pd
import sys
import os
import warnings
warnings.filterwarnings('ignore')#忽视warning的信息

if __name__=='__main__':
#********************************创建日志类型****************************************
    root_path=sys.path[0]
    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)
    logger.handlers = []
     
    # StreamHandler
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level=logging.INFO)
    logger.addHandler(stream_handler)
     
    # FileHandler
    file_handler = logging.FileHandler(os.path.join(root_path,'output.log'), encoding='utf-8')
    file_handler.setLevel(level=logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    

#*******************************读取配置文件*************************************
    filename=os.path.join(root_path,'configure.txt')
    with open(filename, 'r') as file_object:
        a=eval(file_object.read())
    
    user=a['database']['user']
    password=a['database']['password']
    database=a['database']['database']
    host=a['database']['host']
    port=a['database']['port']       
     
#*******************************从数据库获得原始数据*******************************
    ud=UseDatabase(user,password,database,host,port,logger)
    sql='select * from news_info'
    df_info=ud.read_db(sql)
    
#*******************************获得各个网站新闻链接*******************************    
    #转化成函数所需类型
    data_list=df_info.to_dict(orient='records')
    snu=SpiderNewsUrl(data_list,logger)
    df_url_list=snu.get_url_info()
    

#*******************************对新闻链接进行处理****************************    
    #对文章链接进行处理
    dui=DealUrlInfo(df_url_list,logger)
    df_url_list=dui.deal_info()

#******************************获得各个网站新闻内容********************************
    #将数据处理成函数所需类型
    df_url_list=df_url_list.to_dict(orient='records')
    #调用类的函数，得到每篇文章的内容
    sn=SpiderNews(df_url_list,logger)
    df_news_info=sn.get_news_info()

#****************************将数据存入数据库*******************************
    sql = "insert IGNORE into news_data (name,title,url,info,img_url,get_time) values(%s,%s,%s,%s,%s,%s)"
    info1=df_news_info[['name','title','url','info','img_url','get_time']]
    info1.to_csv('news.csv')
    info1 = pd.read_csv('news.csv')[['name','title','url','info','img_url','get_time']]
    info1 = info1.astype(object).where(pd.notnull(info1), None)
    list_tuple = [tuple(x) for x in info1.values]
    ud. insert_db(sql,list_tuple)

