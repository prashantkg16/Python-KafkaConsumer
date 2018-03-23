from kafka import KafkaConsumer
import json
import requests

### VizGems REST END point
vizgems_rest_url = "http://135.25.224.231:8000/cgi-bin/vg_rest.cgi"

### Creating Instance KafKa Consumer
cdp_receiver = KafkaConsumer('vizgems_in',
                         group_id='vizgems',
                         enable_auto_commit=False,
                         bootstrap_servers=['135.25.224.229:9092'])

### consuming Massages
payload = dict()
for message in cdp_receiver:
   ### Formating received data in JSON format
   payload = json.loads(message.value.replace("'", "\""))
   try:
      ### Sending the request to VizGems app for Config the Services
      response = requests.post(vizgems_rest_url, data=payload)
      #print(response.json())
   except:
      print("fail")
