import pymysql

def connect():
    conn = pymysql.connect(
        host = 'svc.sel4.cloudtype.app',
        port = 31196,
        user = 'data',
        password = 'data',
        database = 'recommend_system',
        charset='utf8mb4'
    )

    return conn
