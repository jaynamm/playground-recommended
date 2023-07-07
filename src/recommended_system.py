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
    
    user_profile = pd.DataFrame(-1, index=[0], columns=feature.columns[1:])
    
    # 유저 프로필 데이터 입력
    for skill, grade in skills.items():
        skill = skill.upper()
        if skill in user_profile.columns:
            user_profile[skill] = grade
            
    feature = feature.replace(0, -1)
    
    similarity_scores = cosine_similarity(user_profile, feature.iloc[:, 1:])

    feature['similarity_score'] = similarity_scores[0]

    recommended_jobs = feature.nlargest(20, 'similarity_score')

    recommended_job_ids = recommended_jobs['id'].tolist()
    # recommended_score = recommended_jobs['similarity_score'].tolist()

    # 딕셔너리 형태로 결과 담기
    result = dict()
    jd_list = get_result_for_ccd()
    
    for rid in recommended_job_ids:
        job_detail = jd_list[jd_list['id'] == rid].values.tolist()[0]
        
        # print(len(job_detail), job_detail)
        
        result[job_detail[0]] = {
            'url': job_detail[1], 
            'job_list': ast.literal_eval(job_detail[2]), 
            'title': job_detail[3], 
            'company': job_detail[4], 
            'location': ast.literal_eval(job_detail[5])
            }


    return result