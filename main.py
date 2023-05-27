from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from flask_cors import CORS
from util import log
import time
app = Flask(__name__)
CORS(app)
HOSTNAME = "100.81.9.75"
HOSTNAME = "192.168.0.106"
PORT = 3306
USERNAME = "hive"
PASSWORD = "shizb1207"
DATABASE = "bi"

app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}"

db = SQLAlchemy(app)

# 1、对单个新闻的生命周期的查询，可以展示单个新闻在不同时间段的流行变化

# 对headline进行模糊查询
@app.route('/news', methods=['GET'])
def getHeadline():
    data = request.args
    headline = data.get('headline')
    amount = data.get('amount')

    sql = f'''select news_id,headline from t_news where headline like '%{headline}%' limit {amount};'''
    start = time.time()
    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = [{'news_id': row.news_id, 'headline': row.headline} for row in rows]
        return jsonify(result)

# 获取某个新闻的生命周期
@app.route('/news/fashion', methods=['GET'])
def getSingleNewsFashion():
    data = request.args
    start_ts = data.get('start_ts')
    end_ts = data.get('end_ts')
    news_id = data.get('news_id')

    sql = f'''select count(*), from_unixtime(tnbr.start_ts,"%Y-%m-%d") from t_news_browse_record tnbr where {start_ts} <= tnbr.start_ts and tnbr.start_ts <= {end_ts} and news_id = {news_id} group by tnbr.news_id ,from_unixtime(tnbr.start_ts,"%Y-%m-%d");'''

    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = [{'count': row[0], 'date': row[1]} for row in rows]
        return jsonify(result)


# 2、对某些种类的新闻的变化情况的统计查询，可以展示不同类别的

# 获取所有的新闻类别
@app.route('/category', methods=['GET'])
def getAllCategories():
    sql = f"select distinct(category) from t_news_daily_category;"
    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = [row[0] for row in rows]
        return jsonify(result)

# 获取某个类别的新闻的变化情况
@app.route('/news/category', methods=['GET'])
def getCategoryNewsChanging():
    data = request.args
    category = data.getlist('categorys[]')
    where_clause = ' or '.join([f"category = '{c}'" for c in category])
    start_ts = int(data.get('start_ts'))
    end_ts = int(data.get('end_ts'))
    start_day = start_ts // 86400
    end_day = min(end_ts // 86400, 18088)
    sql = f"select sum(tndc.browse_count), sum(tndc.browse_duration), tndc.day_stamp from t_news_daily_category tndc where tndc.day_stamp>={start_day} and tndc.day_stamp<={end_day} and tndc.category = '{category}' group by tndc.day_stamp;"
    new_sql = f"""select day_stamp,category,browse_count from t_news_daily_category where day_stamp>={start_day} and day_stamp<={end_day} and ({where_clause}) group by day_stamp,category;"""
    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(new_sql)).fetchall()
        end = time.time()
        log(text(new_sql), end - start)
        result={}
        for row in rows:
            try:
                if(row[1] not in result):
                    result[row[1]] = []
                    for i in range(start_day,end_day+1):
                        result[row[1]].append(0)
                result[row[1]][int(row[0])-start_day] = row[2]
            except:
                print(row)
        return result


# 3、对用户兴趣变化的统计查询
@app.route('/user/interest', methods=['GET'])
def getUserInterestChanging():
    data = request.args
    user_id = data.get('user_id')
    start_ts = data.get('start_ts')
    end_ts = data.get('end_ts')
    
    sql = f"""select count(*),n.category from t_news as n join (select tnbr.news_id,tnbr.start_ts from t_news_browse_record as tnbr where tnbr.user_id={user_id} and tnbr.start_ts>={start_ts} and tnbr.start_ts<={end_ts}) as t on n.news_id=t.news_id group by n.category;"""
    # sql = f"""select count(*),n.category,from_unixtime(t.start_ts,"%Y-%m-%d") from t_news as n join (select tnbr.news_id,tnbr.start_ts from t_news_browse_record as tnbr where tnbr.user_id={user_id} and tnbr.start_ts>={start_ts} and tnbr.start_ts<={end_ts}) as t on n.news_id=t.news_id group by n.category,from_unixtime(t.start_ts,"%Y-%m-%d");"""

    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        # result = [{'count': row[0], 'category': row[1], 'date': row[2]} for row in rows]
        result = [{'count': row[0], 'category': row[1]} for row in rows]
        return jsonify(result)


# 4、可以按照时间/时间段、新闻主题（模糊匹配）、新闻标题长度、新闻长度、特定用户、特定多个用户等多种条件和组合进行统计查询

# 获取新闻标题、内容的长度范围
@app.route('/range/length', methods=['GET'])
def getLengthRange():
    sql = f"""select min(user_id),max(user_id) from t_news_browse_record;"""
    with db.engine.connect() as conn:
        sql = f"""select min(length(headline)),max(length(headline)),min(length(content)),max(length(content)) from t_news;"""
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        row = rows[0]
        result = []
        result.append({'min_headline_length': row[0], 'max_headline_length': row[1], 'min_content_length': row[2], 'max_content_length': row[3]})
        return jsonify(result)

# 获取用户id的范围
@app.route('/range/userid', methods=['GET'])
def getUserIdRange():
    sql = f"""select min(user_id),max(user_id) from t_news_browse_record;"""
    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = [{'min_user_id': row[0], 'max_user_id': row[1]} for row in rows]
        return jsonify(result)

# 获取所有topic，降序排列
# @app.route('/topic', methods=['GET'])
# def getAllTopics():
#     sql = f"select count(*),topic from t_news group by topic order by count(*) desc;"
#     with db.engine.connect() as conn:
#         start = time.time()
#         rows = conn.execute(text(sql)).fetchall()
#         end = time.time()
#         log(text(sql), end - start)
#         result = [{'count': row[0],'topic':row[1]} for row in rows]
#         return jsonify(result)

# 通过category获取topic
@app.route('/topic', methods=['GET'])
def getTopicByCategory():
    data = request.args
    category = data.get('category')
    sql = f"select distinct(n.topic) from t_news n where n.category='{category}';"
    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = [row[0] for row in rows]
        return jsonify(result)

# 通过newsid获取content
@app.route('/news/content', methods=['GET'])
def getContent():
    data = request.args
    news_id = data.get('news_id')
    sql = f"select n.content from t_news n where n.news_id='{news_id}';"
    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = dict({"content":rows[0][0]}) 
        return result

# 组合查询
@app.route('/comprehensive', methods=['GET'])
def getConprehensiveInfo():
    data = request.args
    min_user_id = data.get('min_user_id')
    max_user_id = data.get('max_user_id')
    start_ts = data.get('start_ts')
    end_ts = data.get('end_ts')
    min_headline_length = data.get('min_headline_length')
    max_headline_length = data.get('max_headline_length')
    min_content_length = data.get('min_content_length')
    max_content_length = data.get('max_content_length')
    topic = data.get('topic')
    where_min_user_id = ""
    where_max_user_id = ""
    where_start_ts = ""
    where_end_ts = ""
    where_min_headline_length = ""
    where_max_headline_length = ""
    where_min_content_length = ""
    where_max_content_length = ""
    where_topic = ""
    temp = ""
    if start_ts:
        where_start_ts = f"tnbr.start_ts >= {start_ts}"
        temp = "and "
    if end_ts:
        where_end_ts = temp + f"tnbr.start_ts <= {end_ts}"
        temp = "and "
    if min_user_id:
        where_min_user_id = temp + f"user_id >= {min_user_id}"
        temp = "and "
    if max_user_id:
        where_max_user_id = temp + f"user_id <= {max_user_id}"
        temp = "and "
   
    temp2 = ""
    if min_headline_length:
        where_min_headline_length = f"LENGTH(n.headline) >= {min_headline_length} "
        temp2 = "and "
    if max_headline_length:
        where_max_headline_length = temp2 + f"LENGTH(n.headline) <= {max_headline_length}"
        temp2 = "and "
    if min_content_length:
        where_min_content_length = temp2 + f"LENGTH(n.content) >= {min_content_length}"
        temp2 = "and "
    if max_content_length:
        where_max_content_length = temp2 + f"LENGTH(n.content) <= {max_content_length}"
        temp2 = "and "
    if topic:
        where_topic = temp2 + f"n.topic like '%{topic}%'"

    sql = f"""select distinct new.headline, new.news_id from t_news_browse_record as tnbr join t_news as new on tnbr.news_id = new.news_id where {where_start_ts} {where_end_ts} {where_min_user_id} {where_max_user_id} {temp} tnbr.news_id in (select news_id from t_news n where {where_min_headline_length} {where_max_headline_length} {where_min_content_length} {where_max_content_length} {where_topic});"""
    # get_sql = f"""select new.headline, new.news_id from t_news_browse_record as tnbr join t_news as new on tnbr.news_id = new.news_id where tnbr.start_ts >= {start_ts} AND tnbr.start_ts <= {end_ts}  and user_id >= {min_user_id} and user_id <= {max_user_id} and tnbr.news_id in (select news_id from t_news n where LENGTH(n.headline) <= {max_headline_length} and length(n.headline) >= {min_headline_length} and length(n.content) >= {min_content_length} and length(n.content) <= {max_content_length} and n.topic like '%{topic}%');"""
    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = [{'headline': row[0], 'news_id': row[1]} for row in rows]
        return jsonify(result)


if __name__ == '__main__':
    with app.app_context():
        app.run(debug=True)
