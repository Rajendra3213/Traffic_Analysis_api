This application is used for analysis the network traffic.

#Guide to install and run the application

##Install redis and configure the port to default 6379, if other then change the port in celery_worker.py file

```python
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
celery -A celery_worker worker --beat
fastapi dev main.py
```