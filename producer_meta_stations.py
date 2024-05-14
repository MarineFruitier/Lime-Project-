# Code inspired from Confluent Cloud official examples library
# https://github.com/confluentinc/examples/blob/7.1.1-post/clients/cloud/python/producer.py

from confluent_kafka import Producer
import ccloud_lib # Library not installed with pip but imported from ccloud_lib.py
import time
import requests
import json
from datetime import datetime, timedelta

limit_records = 100 #refixer à 100 si on prend le maximum

url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=" + str(limit_records) + "&order_by=stationcode"
timepoll = 0
# Initialize configurations from "python.config" file
CONF = ccloud_lib.read_ccloud_config("python_meta_stations.config")
TOPIC = "Velib-meta_stations_topic"
# Create Producer instance
producer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
producer = Producer(producer_conf)
# Create topic if it doesn't already exist
ccloud_lib.create_topic(CONF, TOPIC)
delivered_records = 0


def _getTotalResult():    
    response_gettotalres = requests.get(url)
    total_count = int(response_gettotalres.json().get('total_count'))
    max_page = (total_count // limit_records)+1
    timepoll = max_page
    return max_page

def _getVelib_Meta_stations_api():
    #Initialisation des variables
    current_page =0
    current_offset = 0
    total_result = _getTotalResult() #Activer cette option si on veut récupérer les résultats de toutes les bornes. Attention grosse volumétrie.
    velib_values = []  # Liste pour stocker les données de toutes les pages

    while current_page < total_result:
        
        temp_url = url + "&offset=" + str(current_offset)
        response_velibapi = requests.get(temp_url)
        # Extrait les données du champ results
        results = response_velibapi.json().get('results', [])

        for result in results:
            datetime_now = datetime.now()
            datetime_plus_one_hour = datetime_now + timedelta(hours=1)
            coordonnees_geo = result.get('coordonnees_geo', {})
            results_value = {
                "stationcode": str(result.get('stationcode')),
                "name": str(result.get('name')),
                "capacity": str(result.get('capacity')),
                "longitude": str(coordonnees_geo.get('lon')),
                "latitude": str(coordonnees_geo.get('lat')),
                "request_timestamp": datetime_plus_one_hour.isoformat()
            }
            velib_values.append(results_value)

        current_offset += limit_records
        current_page +=1

    return json.dumps(velib_values)


# Callback called acked (triggered by poll() or flush())
# when a message has been successfully delivered or
# permanently failed delivery (after retries).
def acked(err, msg):
    global delivered_records
    # Delivery report handler called on successful or failed delivery of message
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
                .format(msg.topic(), msg.partition(), msg.offset()))

#Execution du code
try:
    while True:
        record_key = "Velib_Meta_Stations"
        record_value = _getVelib_Meta_stations_api()

        # This will actually send data to your topic
        producer.produce(
            TOPIC,
            key=record_key,
            value=record_value,
            on_delivery=acked
        )
        # from previous produce() calls thanks to acked callback
        producer.poll(0)
        #L'API ne prend en charge qu'un appel par seconde, il faut relancer la fonction qu'une fois tous les appels réalisés
        time.sleep(86400)
        #time.sleep(10)
except KeyboardInterrupt:
    pass
finally:
    producer.flush() # Finish producing the latest event before stopping the whole script

