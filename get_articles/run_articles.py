# -*- coding: utf-8 -*-
from get_articles import Logging
from get_articles import UseDatabase
from get_articles import GetArticleTitleList
from get_articles import GetArticleInfoList
from get_articles import BaiDuFanYi

import sys
import os
import pandas as pd
import datetime
import warnings
warnings.filterwarnings('ignore')#忽视warning的信息

if __name__=='__main__':
    #配置当前环境地址
    root_path=os.path.split(os.path.abspath(sys.argv[0]))[0] 
    #配置日志
    logfile_path=os.path.join(root_path,'output.log')#设置日志存储位置
    log=Logging(logfile_path)#创建日志实例
    logger,file_handler,stream_handler=log.create_log()#创建日志
    
    #读取配置文件
    configure_path=os.path.join(root_path,'configure.txt')
    with open(configure_path, 'r') as file_object:
        a=eval(file_object.read())
    user=a['database']['user']
    password=a['database']['password']
    database=a['database']['database']
    host=a['database']['host']
    port=a['database']['port']
    
    #连接数据库
    ud=UseDatabase(user,password,database,host,port,logger)
    sql='select * from article_info_start'
    df_info=ud.read_db(sql)
    
    #获取文章的列表
    data_list=df_info.to_dict(orient='records')
    gatl=GetArticleTitleList(data_list,logger)
    df_title_list=gatl.get_title_list()
    df_title_list['get_time']=datetime.datetime.now()
    print(df_title_list.columns)

    #存入数据库
    sql = "insert IGNORE into articles_data (name,title,url,get_time) values(%s,%s,%s,%s)"
    info1=df_title_list[['name','title','url','get_time']]
    info1.to_csv('articles.csv')
    info1 = pd.read_csv('articles.csv')[['name','title','url','get_time']]
    info1 = info1.astype(object).where(pd.notnull(info1), None)
    list_tuple = [tuple(x) for x in info1.values]
    print(list_tuple[0])
    ud. insert_db(sql,list_tuple)

'''
以下内容是为了将英文标题翻译为中文标题的，目前先不用
    #存入数据库
    df_title_list['get_time']=datetime.datetime.now()
    table='articles_info_data'
    #info1=df_title_list[['name','title','url','get_time']]
        #调用翻译，对title进行翻译
    bdfy = BaiDuFanYi(logger)
    info_result = bdfy.fanyi('hello,world!')
    if len(info_result) != 0:
        try:
            logger.info('开始对title进行翻译,大概需要:')
            logger.info(len(df_title_list['title']))
            logger.info('秒')
            df_title_list['title_translate'] = df_title_list['title'].apply(bdfy.fanyi)
            logger.info('翻译成功!!!')
        except:
            logger.error('翻译失败：', exc_info=True)
    else:
        logger.info('百度翻译IP设置有误！！！')

    df_title_list.to_excel('test.xls')
    info=pd.read_excel('test.xls')[['name','title','title_translate','url','get_time']]
    ud.to_db(table,info)
    logger.info('数据已成功传入数据库！！！')
'''    
    
    
    
'''
以下内容是为了获取文章内容，但是由于获取要很多时间，所以先不启用。
  
    #获取文章内容
    df_title_list=df_title_list.to_dict(orient='records')
    gail=GetArticleInfoList(df_title_list,logger)
    df_article_info=gail.get_article_list()
    


    #存入数据库
    table='article_info_data'
    info1=df_article_info[['img_li','article','name','title','url','get_time']]
    df_article_info.to_excel('test.xls')
    info=pd.read_excel('test.xls')[['img_li','article','name','title','url','get_time']]
    ud.to_db(table,info)
    logger.info('数据已成功传入数据库！！！')

'''  

    

    
    
    
