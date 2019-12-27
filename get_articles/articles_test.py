# -*- coding: utf-8 -*-
'''
脚本，用于测试新的网址是否能直接加入数据库
1、根据格式将数据传入data_list
2、需要拼接url的，添加eli后，下面有个kw_list中，也要加入name
'''
import requests
from lxml import etree
import pandas as pd
from retrying import retry
import numpy as np
class GetArticleTitleList:
    def __init__(self,data_list):
        self.data_list=data_list

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
        except :
            html=response.content.decode('gbk')
        element=etree.HTML(html)
        elements=element.xpath(xpath_title)
        title_li=[]
        for element in elements:
            title = element.xpath('string(.)')
            title_li.append(title)
        href_li=element.xpath(xpath_href)
        if len(title_li)>=10:
            if name=='2ndQuadrant':
                title_li=title_li[::2]
                title_li=title_li[:10]
            else:
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
            print(name)
            try:
                xpath_title=data['xpath_title']
                xpath_href=data['xpath_href']
                url=data['url_total']
                df_url_info=self.get_title(name,url,xpath_title,xpath_href)#调用get_info函数
                print(len(df_url_info.index))
                print('条数据获取成功')
            except Exception as e:
                print('网页的url列表获取失败：', e)
                continue
            df_title_list=pd.concat([df_title_list,df_url_info])
        df_title_list.drop_duplicates(subset='url',inplace=True)
        df_title_list = df_title_list[~df_title_list['title'].isin([np.NaN,None])]
        return df_title_list

if __name__ == '__main__':
    data_list = [{'name':'2ndQuadrant','url_total':'https://blog.2ndquadrant.com/','xpath_title':"//h2/a",'xpath_href':"//h2/a/@href"}]
    gatl = GetArticleTitleList(data_list)
    df_title_list = gatl.get_title_list()
