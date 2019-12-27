'''
用于下载到本地，测试新加的网址是否可以用来获取新闻
需要：
    1、网址
    2、xpath:title和href的xpath
该程序测试通过即可将以上信息插入数据库
'''
import requests
from lxml import etree
import pandas as pd
def get_info(name,url,xpath_title,xpath_href):
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
    return df_url_info


if __name__ == '__main__':
    name = 'leiphone'
    url = 'https://www.leiphone.com/'
    xpath_title = "//ul[@class='clr']/li//h3/a//text()"
    xpath_href = "//ul[@class='clr']/li//h3/a//@href"
    df_url_info = get_info(name,url,xpath_title,xpath_href)