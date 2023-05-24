import datetime

def log(sql, time):
    with open('query.log', 'a') as f:
        now_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        f.write(f"[{now_time}] {sql} {time}s\n")