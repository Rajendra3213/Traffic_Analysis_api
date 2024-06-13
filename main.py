from fastapi import FastAPI
from celery_worker import analyzeData, singleSessionAnalysis
import json

app = FastAPI()

@app.get('/')
def allAnalysisData():
    try:
        response = analyzeData.delay()
        return response.get()
    except Exception as e:
        return {"error": str(e)}

@app.post('/singleSession')
def singleAnalysisData(sessionID: str):
    try:
        response = singleSessionAnalysis.delay(sessionID)
        return response.get()
    except Exception as e:
        return {"error": str(e)}
