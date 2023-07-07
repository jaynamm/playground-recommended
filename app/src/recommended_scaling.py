import pandas as pd
import re
import ast

from sqlalchemy import create_engine

from src.property import db_connect


"""
1. get combined_crawling_data by mysql
"""
    
def get_combined_quals_prefs_skills_data():
    conn = db_connect.connect()
    
    qps_query = "SELECT * FROM combined_quals_prefs_skills"
    
    combined_qps_df = pd.read_sql(qps_query, conn)
    
    conn.close()

    return combined_qps_df


def get_skillset():
    conn = db_connect.connect()
    
    ss_query = "SELECT * FROM skillset"
    
    skillset_df = pd.read_sql(ss_query, conn)
    
    conn.close()

    return skillset_df


def scaling_congbined_data():
    # qauals, prefs, skills 결합 리스트 데이터 가져오기
    combined_qps_df = get_combined_quals_prefs_skills_data()

    # skillset 데이터 가져오기
    skillset_df = get_skillset()

    # scaling_df 생성 후 헤더 설정
    scaling_df = pd.DataFrame(columns=['id'] + list(skillset_df['group'].tolist()))


    for _, row in combined_qps_df.iterrows():
        qps_id = row['id']
        quals = re.findall(r"'(.*?)'", row['qualifications'])
        prefs = re.findall(r"'(.*?)'", row['preferred'])
        skills = re.findall(r"'(.*?)'", row['skills'])
        
        skill_cols = dict()
        
        for _, skillset in skillset_df.iterrows():
            key = skillset['group']
            value = ast.literal_eval(skillset['skill'])
            
            skill_cols[key] = 0
            
            if len(quals) > 0:
                for q in quals:
                    if q in value:
                        skill_cols[key] += 2
                        break

            if len(prefs) > 0:
                for p in prefs:
                    if p in value:
                        skill_cols[key] += 1
                        break

            if len(skills) > 0:
                for s in skills:
                    if s in value:
                        skill_cols[key] += 1
                        break
            
        # scaling_df에 데이터 추가
        scaling_df = pd.concat([ scaling_df, 
            pd.DataFrame([[qps_id] + list(skill_cols.values())], columns=scaling_df.columns)], 
            ignore_index=True)
        
    return scaling_df


def dataframe_to_db(df, table_name):
    engine = create_engine('mysql+pymysql://data:data@localhost:33060/recommend_system?charset=utf8mb4')
    conn = engine.connect()
    
    # 데이터프레임을 데이터베이스 테이블에 삽입
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    
    conn.close()

# scalig 된 채용공고
scaling_congbined_df = scaling_congbined_data()

# 데이터베이스에 저장
dataframe_to_db(scaling_congbined_df, "scaled_quals_prefs_skills")