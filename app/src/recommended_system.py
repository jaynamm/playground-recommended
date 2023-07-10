import ast
import pandas as pd
from sklearn.metrics.pairwise import cosine_similarity

from src.property import db_connect


def get_result_for_ccd():
    conn = db_connect.connect()
    
    ccd_query = "SELECT id, url, job_list, title, company, location FROM combined_crawling_data"
    
    ccd_df = pd.read_sql(ccd_query, conn)
    
    conn.close()

    return ccd_df

def get_scaled_feature():
    conn = db_connect.connect()
    
    qps_query = "SELECT * FROM scaled_quals_prefs_skills"
    
    feature = pd.read_sql(qps_query, conn)
    
    conn.close()

    return feature


def post_recommended_data(input_data):
    
    feature = get_scaled_feature()
    
    job_name = input_data['job_name']
    location = input_data['location']
    skills = input_data['skills']

    # filtering location and job
    filtered_df = pd.DataFrame()
    jd_list = get_result_for_ccd()
    
    # job 이 전체 선택이면, loc만 필터링
    job_filter = jd_list['job_list'].str.contains(job_name)
    loc_filter = jd_list['location'].str.contains(location)

    if job_name == "all":
        filtered_df = jd_list[loc_filter]
    else:
        filtered_df = jd_list[job_filter & loc_filter]


    # 전체 채용 데이터에서 filtering 된 id 값에 포함되는 데이터만 정리
    feature_filtered = feature[feature['id'].isin(filtered_df['id'].tolist())]

    # filtering 된 데이터에서 user가 가진 skill 데이터 컬럼만 선택 (id 포함)
    skill_df = feature_filtered[['id']+list(skills.keys())]
    
    # id 제외한 user의 skill 셋
    user_profile = pd.DataFrame(0, index=[0], columns=list(skills.keys())[1:]) 

    # 유저 프로필 데이터 입력
    for skill, grade in skills.items():
        skill = skill.upper()
        # if skill in user_profile.columns:
        user_profile[skill] = grade
    
    # 유사도 검증
    skill_df['similarity_score'] = skill_df.apply(lambda row: sum(0 if (row[column].astype(str) > user_profile[column].astype(str)).all() else (row[column] / user_profile[column]) for column in skill_df.columns[1:]) / len(skill_df.columns), axis=1)

    recommended_jobs = skill_df.nlargest(20, 'similarity_score')
    
    # 딕셔너리 형태로 결과 담기
    result = dict()
    
    for rid in recommended_jobs['id'].tolist():
        job_detail = jd_list[jd_list['id'] == rid].values.tolist()[0]
        
        result[str(job_detail[0])] = {
            'url': job_detail[1], 
            'job_list': ast.literal_eval(job_detail[2]), 
            'title': job_detail[3], 
            'company': job_detail[4], 
            'location': ast.literal_eval(job_detail[5])
            }

    return result