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
PORT = 3306
USERNAME = "hive"
PASSWORD = "shizb1207"
DATABASE = "bi"

app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}"

db = SQLAlchemy(app)

# 1、对单个新闻的生命周期的查询，可以展示单个新闻在不同时间段的流行变化
# TODO:headline

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

@app.route('/category', methods=['GET'])
def getAllCategories():
    sql = f"select distinct(category) from t_news_daily_category;"
    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = [{'category': row[0]} for row in rows]
        return jsonify(result)

@app.route('/news/category', methods=['GET'])
def getCategoryNewsChanging():
    data = request.args
    category = data.get('category')
    start_ts = int(data.get('start_ts'))
    end_ts = int(data.get('end_ts'))
    start_day = start_ts // 86400
    end_day = end_ts // 86400
    sql = f"select sum(tndc.browse_count), sum(tndc.browse_duration), tndc.day_stamp from t_news_daily_category tndc where tndc.day_stamp>={start_day} and tndc.day_stamp<={end_day} and tndc.category = '{category}' group by tndc.day_stamp;"

    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = [{'browse_count': row[0], 'browse_duration': row[1], 'day_stamp': row[2]} for row in rows]
        return jsonify(result)


# 3、对用户兴趣变化的统计查询(问题不大)
@app.route('/user/interest', methods=['GET'])
def getUserInterestChanging():
    data = request.args
    user_id = data.get('user_id')
    start_ts = data.get('start_ts')
    end_ts = data.get('end_ts')

    sql = f"""select count(*),n.category,from_unixtime(t.start_ts,"%Y-%m-%d") from t_news as n join (select tnbr.news_id,tnbr.start_ts from t_news_browse_record as tnbr where tnbr.user_id={user_id} and tnbr.start_ts>={start_ts} and tnbr.start_ts<={end_ts}) as t on n.news_id=t.news_id group by n.category,from_unixtime(t.start_ts,"%Y-%m-%d");"""

    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(sql)).fetchall()
        end = time.time()
        log(text(sql), end - start)
        result = [{'count': row[0], 'category': row[1], 'date': row[2]} for row in rows]
        return jsonify(result)


# 4、可以按照时间/时间段、新闻主题（模糊匹配）、新闻标题长度、新闻长度、特定用户、特定多个用户等多种条件和组合进行统计查询(比较快)
# TODO topic,userid范围
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

    get_sql = f"""select new.headline, new.news_id from t_news_browse_record as tnbr join t_news as new on tnbr.news_id = new.news_id where tnbr.start_ts >= {start_ts} AND tnbr.start_ts <= {end_ts}  and user_id >= {min_user_id} and user_id <= {max_user_id} and tnbr.news_id in (select news_id from t_news n where LENGTH(n.headline) <= {max_headline_length} and length(n.headline) >= {min_headline_length} and length(n.content) >= {min_content_length} and length(n.content) <= {max_content_length} and n.topic like '%{topic}%');"""
    with db.engine.connect() as conn:
        start = time.time()
        rows = conn.execute(text(get_sql)).fetchall()
        end = time.time()
        log(text(get_sql), end - start)
        result = [{'headline': row[0], 'news_id': row[1]} for row in rows]
        return jsonify(result)

# 5、能够实时按照用户浏览的内容进行新闻推荐
# TODO topic,userid范围
@app.route('/recommendation', methods=['GET'])
def getUserRecommendation():
    return 'hhhhhhh'

if __name__ == '__main__':
    with app.app_context():
        app.run(debug=True)
