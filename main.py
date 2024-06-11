from fastapi import FastAPI
from celery_worker import analyzeData

app=FastAPI()

@app.get('/')
def hello():
    response=analyzeData.delay()
    return response.get()