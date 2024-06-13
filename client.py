# client.py

import requests

# Assuming sessionID is already defined
sessionID = "AMRITA.PANTA-%5BOPENVPN_L3%5D"
url = "http://192.168.88.54:8000/singleSession"

try:
    response = requests.post(url, json={"sessionID": sessionID})
    data = response.json()
    print(data)
except Exception as e:
    print("Error:", e)
