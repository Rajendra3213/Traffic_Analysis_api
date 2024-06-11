from celery import Celery
from celery.schedules import crontab

app=Celery('Analyze',broker='redis://localhost:6379/0', backend='redis://localhost:6379/0')

app.conf.timezone='Asia/Kathmandu'

@app.on_after_configure.connect
def schedule_task(sender,**kwargs):
    sender.add_periodic_task(
        crontab(minute=1,hour=0),
        getLog.s()
    )

@app.task
def getLog():
    print("Downloading Log file..")

@app.task
def analyzeData():
    print("Analyzing data..")
    return {"output":"Hello world!"}