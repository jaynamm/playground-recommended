import ast
import pandas as pd
import numpy as np

from src.property import db_connect


def get_result_for_ccd_loc(conn, location):
    ccd_query = "SELECT id, url, job_list, title, company, location FROM combined_crawling_data WHERE location LIKE %s"
    ccd_df = pd.read_sql(ccd_query, conn, params=(f"%{location}%"))

    return ccd_df

def get_result_for_ccd_job_loc(conn, job_name, location):
    ccd_query = "SELECT id, url, job_list, title, company, location FROM combined_crawling_data WHERE job_list LIKE %s AND location LIKE %s"
    ccd_df = pd.read_sql(ccd_query, conn, params=(f"%{job_name}%", f"%{location}%"))

    return ccd_df

def get_scaled_feature(conn):
    qps_query = "SELECT * FROM scaled_quals_prefs_skills"
    feature = pd.read_sql(qps_query, conn)

    return feature


def post_recommended_data(input_data):
    job_name = input_data['job_name']
    location = input_data['location']
    skills = input_data['skills']
    
    conn = db_connect.connect()
    
    feature = get_scaled_feature(conn)

    filtered_df = pd.DataFrame()

    if job_name == "all":
        filtered_df = get_result_for_ccd_loc(conn, location)
    else:
        filtered_df = get_result_for_ccd_job_loc(conn, job_name, location)
        
    conn.close()

    # 전체 채용 데이터에서 filtering 된 id 값에 포함되는 데이터만 정리
    feature_filtered = feature[feature['id'].isin(filtered_df['id'].tolist())]

    # filtering 된 데이터에서 user가 가진 skill 데이터 컬럼만 선택 (id 포함)
    skill_df = feature_filtered[['id']+list(skills.keys())]
    
    # id 제외한 user의 skill 셋
    user_profile = pd.DataFrame(0, index=[0], columns=list(skills.keys())[1:]) 

    # 유저 프로필 데이터 입력
    for skill, grade in skills.items():
        skill = skill.upper()
        user_profile[skill] = grade

    # 유사도 검증
    for column in skill_df.columns[1:]:
        user_grade = user_profile[column].iloc[0].astype(str)
        skill_df[column] = np.where(skill_df[column].astype(str) > user_grade, 0, int(skill_df[column]) / int(user_grade))

    # 유사도 저장 컬럼 초기화
    skill_df['similarity_score'] = 0
    
    # 유사도 집계 후 저장
    for column in skill_df.columns[1:-1]:
        skill_df["similarity_score"] += skill_df[column]
    
    skill_df["similarity_score"] /= len(skill_df.columns)

    # 유사도 검증
    # skill_df['similarity_score'] = skill_df.apply(
    #     lambda row: sum(0 if (row[column].astype(str) > user_profile[column].astype(str)).all() else (int(row[column]) / int(user_profile[column])) for column in skill_df.columns[1:]), 
    #     axis=1
    # )
    
    recommended_jobs = skill_df.nlargest(20, 'similarity_score')
    recommended_job_lst = recommended_jobs['id'].tolist()
    
    print("="*10, "recommended_jobs", "="*10)
    print(recommended_jobs)
    
    # 딕셔너리 형태로 결과 담기
    result = dict()
    
    for i in range(20):
        job_detail = filtered_df[filtered_df['id'] == recommended_job_lst[i]].values.tolist()[0]
        result[i+1] = {
            "id": job_detail[0],
            "url": job_detail[1], 
            "job_list": ast.literal_eval(job_detail[2]), 
            "title": job_detail[3], 
            "company": job_detail[4], 
            "location": ast.literal_eval(job_detail[5])
            }

    return result