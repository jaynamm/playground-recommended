from fastapi import FastAPI, Body
from pydantic import BaseModel
from typing import Optional

from src import recommended_system

app = FastAPI()

"""
{
    "job_name" : "데이터 엔지니어",
    "location": "서울",
    "skills": {
        "HADOOP": 2,
        "PYTHON": 2,
        "SPARK": 1,
        "TABLEAU": 1,
        "JAVA": 3,
        "SPRING FRAMEWORK": 2,
        "MYSQL": 1
    }
}
"""

class Data(BaseModel):
    job_name: Optional[str] = None
    location: Optional[str] = None
    skills: Optional[dict]

@app.post("/recommend")
async def recommend(data: Data):
    input_data = dict(data)
    result = recommended_system.post_recommended_data(input_data)
    
    return result
