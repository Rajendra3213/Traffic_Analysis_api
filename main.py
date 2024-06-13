from fastapi import FastAPI
from celery_worker import analyzeData, singleSessionAnalysis
from pydantic import BaseModel
import json

app = FastAPI()

@app.get('/')
def allAnalysisData():
    try:
        response = analyzeData.delay()
        return response.get()
    except Exception as e:
        return {"error": str(e)}

class sessionBody(BaseModel):
    sessionId:str
    

@app.post('/singleSession')
def singleAnalysisData(sessionData: sessionBody):
    try:
        response = singleSessionAnalysis.delay(sessionData.sessionId)
        return response.get()
    except Exception as e:
        return {"error": str(e)}
