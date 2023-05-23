from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy import text
from flask_cors import CORS

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

@app.route('/news/fashion', methods=['GET'])
def getSingleNewsFashion():
    data = request.args
    start_ts = data.get('start_ts')
    end_ts = data.get('end_ts')
    news_id = data.get('news_id')

    sql = f'''select news_id, count(*), from_unixtime(tnbr.start_ts,"%Y-%m-%d") 
        from t_news_browse_record tnbr 
        where {start_ts} <= tnbr.start_ts and tnbr.start_ts <= {end_ts} and news_id = {news_id} 
        group by tnbr.news_id ,from_unixtime(tnbr.start_ts,"%Y-%m-%d");'''

    with db.engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
        # res = [list(row) for row in rows]
        return str(rows)


# 2、对某些种类的新闻的变化情况的统计查询，可以展示不同类别的
@app.route('/news/category', methods=['GET'])
def getCategoryNewsChanging():
    data = request.args
    category = data.get('category')

    sql = f"select sum(tndc.browse_count), sum(tndc.browse_duration), tndc.day_stamp from t_news_daily_category tndc where tndc.category = '{category}' group by tndc.day_stamp;"

    with db.engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
        print(rows)
        return str(rows)


# 3、对用户兴趣变化的统计查询(问题不大)
@app.route('/user/interest', methods=['GET'])
def getUserInterestChanging():
    data = request.args
    user_id = data.get('user_id')
    start_ts = data.get('start_ts')
    end_ts = data.get('end_ts')

    sql = f"""
        select count(1),n.category
        from t_news as n
        where news_id in 
            (select tnbr.news_id from t_news_browse_record as tnbr 
            where tnbr.user_id = {user_id} and {start_ts} <= tnbr.start_ts 
            and tnbr.start_ts <= {end_ts})
        group by n.category;"""

    with db.engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()
        return str(rows)


# 4、可以按照时间/时间段、新闻主题（模糊匹配）、新闻标题长度、新闻长度、特定用户、特定多个用户等多种条件和组合进行统计查询(比较快)
# TODO topic,userid范围
@app.route('/comprehensive', methods=['GET'])
def getConprehensiveInfo():
    data = request.args
    user_id = data.get('user_id')
    start_ts = data.get('start_ts')
    end_ts = data.get('end_ts')
    headline_length = data.get('headline_length')
    content_length = data.get('content_length')

    sql = f"""select * from t_all
        where user_id = {user_id} 
            and {start_ts} <= start_ts and start_ts <= {end_ts} 
            and headline_length <= {headline_length} and content_length >= {content_length};"""
    get_sql = f"""select new.headline,new.news_id
            from t_news_browse_record as tnbr join t_news as new on tnbr.news_id = new.news_id
            where tnbr.start_ts>={start_ts} and tnbr.start_ts<={end_ts}  and user_id={user_id} 
            and tnbr.news_id in (select news_id from t_news n where length(n.headline)<={headline_length} and length(n.content)>={content_length});"""
    with db.engine.connect() as conn:
        rows = conn.execute(text(get_sql)).fetchall()
        return str(rows)

# 5、能够实时按照用户浏览的内容进行新闻推荐
# TODO topic,userid范围
@app.route('/recommendation', methods=['GET'])
def getUserRecommendation():
    return 'hhhhhhh'

if __name__ == '__main__':
    with app.app_context():
        app.run(debug=True)
