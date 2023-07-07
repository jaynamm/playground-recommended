import csv
from datetime import date

import pandas as pd
import numpy as np
import pymysql

from src.property import db_connect
from object import create_table

def crawling_data_CSV_to_DB():
    conn = db_connect.connect()

    cursor = conn.cursor()

    create_table_query = create_table.combined_crawling_data

    cursor.execute(create_table_query)

    # CSV 파일 읽기 및 데이터베이스에 삽입
    with open("/Users/jhnam/workspace/playground-data/fastapi/app/data/combined_crawling_data.csv", encoding="utf-8") as f:
        reader = csv.reader(f)

        # 첫 번째 행은 헤더이므로 건너뜁니다.
        next(reader)  

        for row in reader:
            id_value = int(row[0])
            url_value = row[1]
            job_list_value = row[2]
            title_value = row[3]
            company_value = row[4]
            location_value = row[5]
            tags_value = row[6]
            qualifications_value = row[7]
            preferred_value = row[8]
            benefits_value = row[9]
            skills_value = row[10]
            workplace_value = row[11]
            crawled_date_value = date.today()

            insert_query = '''
                INSERT INTO combined_crawling_data (id, url, job_list, title, company, location, tags, qualifications, preferred, benefits, skills, workplace, crawled_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            insert_values = (id_value, url_value, job_list_value, title_value, company_value, location_value, tags_value, qualifications_value, preferred_value, benefits_value, skills_value, workplace_value, crawled_date_value)

            cursor.execute(insert_query, insert_values)

    # 변경 내용을 커밋하여 데이터베이스에 반영
    conn.commit()

    # 연결 및 커서 종료
    cursor.close()
    conn.close()

crawling_data_CSV_to_DB()