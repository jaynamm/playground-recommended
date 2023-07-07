import os
import re
import csv

import pandas as pd
import ast

from konlpy.tag import Komoran
from sqlalchemy import create_engine

from src.property import db_connect
from object import preprocessing_data



"""
1. get combined_crawling_data by mysql
"""
    
def get_combined_crawling_data():
    conn = db_connect.connect()
    
    ccd_query = "SELECT * FROM combined_crawling_data"
    
    ccd_df = pd.read_sql(ccd_query, conn)
    
    conn.close()

    return ccd_df



"""
2. skills list 추출해서 set 함수로 중복 제거 - skillset 반환
"""

def extract_skillset():
    skills_list = []
    
    ccd_df = get_combined_crawling_data()
    
    ccd_skills_list = ccd_df['skills'].tolist()
    
    for row in ccd_skills_list:
        # row 리스트의 10번째 값을 가져와 string 을 '' 안의 단어들로 나눠서 리스트로 만들기
        skills = re.findall(r"'(.*?)'", row)
        skills_upper = [word.upper().strip() for word in skills]

        # 나누어진 리스트를 추가
        skills_list += skills_upper

    skillset = set(skills_list)

    return skillset



"""
3. 데이터프레임을 데이터베이스에 저장
"""

def dataframe_to_db(df, table_name):
    engine = create_engine('mysql+pymysql://data:data@localhost:33060/recommend_system?charset=utf8mb4')
    conn = engine.connect()
    
    # 데이터프레임을 데이터베이스 테이블에 삽입
    df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
    
    conn.close()

    return "데이터베이스에" + table_name + "적재 완료하였습니다."


"""
4. 각 채용 공고에서 qualifications, preferred, skills 에서 skillset 에 있는 skill 만 추출하기
"""

def extract_quals_prefs_skills_by_skillset():
    com_set = extract_skillset()
    com_list = list(com_set)
    extract_dict = dict()

    ccd_df = get_combined_crawling_data()

    # 특정 컬럼만 가져오기
    column_names = ['id', 'qualifications', 'preferred', 'skills']
    ccd_list = ccd_df[column_names].values.tolist()

    for row in ccd_list:

        # id = 0, qualifications = 1, preferred = 2, skills = 3
        id = row[0]
        quals_data = row[1].upper()
        prefs_data = row[2].upper()
        skills_data = row[3].upper()
        

        for data in quals_data, prefs_data:
            # remove symbols
            sentence = re.sub(r'[^\w\s]', '', data)

            # 확인해야하는 qualification data
            phrases = com_list

            # re.escape 로 특수문자를 벗어나고, | 로 단어 연결
            pattern = r'\b(' + '|'.join(map(re.escape, phrases)) + r')\b'   

            # 한글 정렬 후 나누기
            komoran = Komoran()
            sentence = komoran.morphs(sentence)

            # 한글 정렬 후 나누고 다시 문장으로 합쳐주기
            sentence = " ".join(sentence)
            
            # phrases 에 있는 단어를 가져와서 sentence 속 빈도 확인하기
            matches = re.findall(pattern, sentence)
            
            # freq_dict[id] = list(set(matches))
            if data == quals_data:
                quals_freq = str(list(set(matches)))
            elif data == prefs_data:
                prefs_freq = str(list(set(matches)))

        skill_freq = str(ast.literal_eval(skills_data) if skills_data else [])
        
        extract_dict[id] = {
            "qualifications": quals_freq, 
            "preferred": prefs_freq, 
            "skills": skill_freq
        }
        
        
    # 추출된 스킬 리스트가 있는 딕셔너리를 가져와서 데이터프레임으로 변환
    extract_qps_skill_list = pd.DataFrame(extract_dict)

    # id 값이 인덱스가 되도록 행렬 바꾸기
    combined_quals_prefs_skills = extract_qps_skill_list.T

    # index 를 id 컬럼으로 변환
    combined_quals_prefs_skills.reset_index(inplace=True)
    combined_quals_prefs_skills.rename(columns={'index': 'id'}, inplace=True)
    
    return combined_quals_prefs_skills


"""
5. 스킬셋 전처리
"""

def skillset_preprocessing():
    # 스킬셋을 저장할 딕셔너리
    skillset_dict = dict()
    
    # 중복 제거된 스킬 셋
    skills_set = extract_skillset() 
    
    # 전처리를 위한 리스트 가져오기
    skill_mapping = preprocessing_data.skill_mapping
    drop_list = preprocessing_data.drop_list
    
    mapping_values = [item for sublist in list(skill_mapping.values()) for item in sublist]
    
    for skill in skills_set:
        if skill in mapping_values:
            for key, mappings in skill_mapping.items():
                if skill in mappings:
                    if key in skillset_dict:
                        skillset_dict[key].append(skill)
                    else:
                        skillset_dict[key] = [skill]
        else:
            if skill not in drop_list:
                skillset_dict[skill] = [skill]
                
    skillset_dict = {k: str(v) for k, v in skillset_dict.items()}

    skillset_df = pd.DataFrame.from_dict(skillset_dict, orient='index', columns=['values'])
    skillset_df.reset_index(inplace=True)
    skillset_df.rename(columns={'index': 'group', 'values': 'skill'}, inplace=True)

    return skillset_df


# skillset 데이터베이스에 저장
skillset_df = skillset_preprocessing()
dataframe_to_db(skillset_df, "skillset")

# combined_quals_prefs_skills 데이터베이스에 저장
combined_quals_prefs_skills = extract_quals_prefs_skills_by_skillset()
dataframe_to_db(combined_quals_prefs_skills, "combined_quals_prefs_skills")