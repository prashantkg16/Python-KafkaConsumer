from kafka import KafkaConsumer
import json
import requests
import logging


### VizGems REST END point
vizgems_rest_url = "http://135.25.224.231:8000/cgi-bin/vg_rest.cgi"

### Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
 
handler = logging.FileHandler('/export/home/swift/log/kafkaconsumer')
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


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
		logger.info(payload + ' Response: '+response.json())

	except Exception as e:      
		logger.error(e.message)
