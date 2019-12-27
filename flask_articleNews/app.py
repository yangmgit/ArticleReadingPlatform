from flask import Flask,render_template,flash, request, redirect, url_for
from sqlalchemy import create_engine
import pandas as pd

app = Flask(__name__)
app.secret_key='yang'

def con_db():
    engine = create_engine('mysql+pymysql://root:yangroot@106.52.176.64:3306/enmo_project')
    return engine


def read_db(sql):
    engine = con_db()
    df_db_data = pd.read_sql(sql, con=engine)
    return df_db_data

def update_table(u_sql):
    engine = con_db()
    conn = engine.connect()
    cur = conn.begin()
    try:
        conn.execute(u_sql)
        cur.commit()
    except Exception as e:
        print(e)
        flash(u'更改出错')
        cur.rollback()
    cur.close()
    conn.close()

#定义根路由
@app.route('/')
def index():
    return render_template('index.html')

#定义文章显示路由
@app.route('/article',methods=['GET','POST'])
def article_list():
    sql="select id,name,title,url,min(get_time) get_time  from articles_data group by id,name,title,url  order by 5 desc limit 500"
    df_article_data=read_db(sql)
    df_article_data=df_article_data.to_dict(orient='records')#页面不支持df所以要转换
    return render_template('ArticleList.html',df_article_data=df_article_data)

#定义资讯显示路由
@app.route('/news',methods=['GET','POST'])
def news_list():
    sql="select id,name,title,url,min(get_time) get_time  from news_data  group by id,name,title,url  order by 5 desc limit 500"
    df_news_data = read_db(sql)
    df_news_data=df_news_data.to_dict(orient='records')
    return render_template('NewsList.html',df_news_data=df_news_data)

#定义待整理路由
@app.route('/ToDoList',methods=['GET','POST'])
def todo_list():
    sql="select id,name,title,url,get_time  from articles_data where state=1"
    df_article_data=read_db(sql)
    df_article_data['type']='article'
    sql2 = "select id,name,title,url,get_time  from news_data where state=1"
    df_news_data = read_db(sql2)
    df_news_data['type']='news'
    df_todo_data = pd.concat([df_article_data,df_news_data])
    #print(df_todo_data)
    df_todo_data=df_todo_data.to_dict(orient='records')#页面不支持df所以要转换
    return render_template('ToDoList.html',df_todo_data=df_todo_data)

#定义已经整理完毕的路由
@app.route('/DoneList',methods=['GET','POST'])
def done_list():
    sql = "select id,name,title,url,get_time  from articles_data where state=2"
    df_article_data = read_db(sql)
    df_article_data['type'] = 'article'
    sql2 = "select id,name,title,url,get_time  from news_data where state=2"
    df_news_data = read_db(sql2)
    df_news_data['type'] = 'news'
    df_done_data = pd.concat([df_article_data, df_news_data])
    df_done_data=df_done_data.to_dict(orient='records')#页面不支持df所以要转换
    return render_template('DoneList.html',df_done_data=df_done_data)

#定义更新技术文章的sate
@app.route('/UpdateArticleData/<num>/<id>',)
def update_articlre_data(num,id):
    #更新数据库
    u_sql = 'update articles_data set state={} where id={}'.format(num,id)
    try:
        update_table(u_sql)
        flash(u'添加到待整理成功')
    except Exception as e:
        print(e)
        flash(u'添加到待整理失败')
    return redirect(url_for('article_list'))

#定义更新新闻文章的sate
@app.route('/UpdateNewsData/<num>/<id>',)
def update_news_data(num,id):
    #更新数据库
    u_sql = 'update news_data set state={} where id={}'.format(num, id)
    try:
        update_table(u_sql)
        flash(u'添加到待整理成功')
    except Exception as e:
        print(e)
        flash(u'添加到待整理失败')
    return redirect(url_for('news_list'))

#定义更新ToDoList的sate
@app.route('/ToDoListData/<num>/<id>/<type>',)
def update_todo_data(num,id,type):
    #更新数据库
    if type == 'news':
        u_sql = 'update news_data set state={} where id={}'.format(num, id)
    elif type == 'article':
        u_sql = 'update articles_data set state={} where id={}'.format(num, id)
    try:
        update_table(u_sql)
        flash(u'添加到待整理成功')
    except Exception as e:
        print(e)
        flash(u'添加到待整理失败')
    return redirect(url_for('todo_list'))

#定义更新DoneList的sate
@app.route('/DoneListData/<num>/<id>/<type>',)
def update_done_data(num,id,type):
    #更新数据库
    if type == 'news':
        u_sql = 'update news_data set state={} where id={}'.format(num, id)
    elif type == 'article':
        u_sql = 'update articles_data set state={} where id={}'.format(num, id)
    try:
        update_table(u_sql)
        flash(u'添加到待整理成功')
    except Exception as e:
        print(e)
        flash(u'添加到待整理失败')
    return redirect(url_for('done_list'))



if __name__ == '__main__':
    app.run(host = '0.0.0.0',
        port = 5000,
        debug = True)
