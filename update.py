from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import create_engine
from sqlalchemy import text
import tqdm
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
HOSTNAME = "100.81.9.75"
PORT = 3306
USERNAME = "hive"
PASSWORD = "shizb1207"
DATABASE = "bi_test"

app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOSTNAME}:{PORT}/{DATABASE}"

db = SQLAlchemy(app)


if __name__ == '__main__':
    start_day = 18060
    with app.app_context(), db.engine.connect() as conn:
        for start_day in tqdm.tqdm(range(18060, 18074)):
            sql = f"""insert into t_news_daily_category
                    (select tnbr.start_day,n.category,count(tnbr.news_id),sum(tnbr.duration)
                    from (t_news_browse_record tnbr join (select news_id,category from t_news) as n on tnbr.news_id = n.news_id)
                    where tnbr.start_day={start_day}
                    group by n.category);"""
            try:
                res = conn.execute(text(sql))
                conn.commit()
            except Exception as e:
                print(e)
        # id, user_id, news_id, start_ts, start_day, duration, category, topic, headline_length, content_length
        # print(res)
