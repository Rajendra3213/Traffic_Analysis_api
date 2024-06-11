from fastapi import FastAPI
from celery_worker import analyzeData,singleSessionAnalysis

app=FastAPI()

@app.get('/')
def allAnalysisData():
    response=analyzeData.delay()
    return response.get()

@app.post('/singleSession')
def singleAnalysisData(sessionID:str):
    response=singleSessionAnalysis.delay(sessionID)
    return response.get()