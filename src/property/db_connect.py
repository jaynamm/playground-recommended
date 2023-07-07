import pymysql

def connect():
    conn = pymysql.connect(
        host = 'localhost',
        port = 33060,
        user = 'data',
        password = 'data',
        database = 'recommend_system',
        charset='utf8mb4'
    )

    return conn
