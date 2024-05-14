# Example written based on the official
# Confluent Kakfa Get started guide https://github.com/confluentinc/examples/blob/7.1.1-post/clients/cloud/python/consumer.py
from confluent_kafka import Consumer
import subprocess
# Installer les dépendances depuis le fichier requirements.txt
subprocess.run(['pip', 'install', '-r', 'requirements.txt'])

# Importer les modules après l'installation
from sqlalchemy import create_engine
import json
import ccloud_lib
import time
import pandas as pd
import psycopg2


# Remplacez ces valeurs par les informations de connexion de votre base de données RDS
db_username = 'user'
db_password = 'password'
db_endpoint = 'jedha-lime-eu-west-3.rds.amazonaws.com' #endpoint
db_port = '5432'
db_name = 'velib'
table_fleet_name = 'fleet'
table_fleet_history_name = 'fleet_history'


# Initialize configurations from "python_static.config" file
CONF = ccloud_lib.read_ccloud_config("python_realTime.config")
TOPIC = "Velib-RealTime_topic"

# Create Consumer instance
# 'auto.offset.reset=earliest' to start reading from the beginning of the
# topic if no committed offsets exist
consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(CONF)
consumer_conf['group.id'] = 'python_realTime_consumer'
consumer_conf['auto.offset.reset'] = 'earliest' # This means that you will consume latest messages that your script haven't consumed yet!
consumer = Consumer(consumer_conf)

# Subscribe to topic
consumer.subscribe([TOPIC])

def _pushFleet(record_value):
    # Connect to RDS
    connection = psycopg2.connect(
        host=db_endpoint,
        port=db_port,
        user=db_username,
        password=db_password,
        database=db_name
    )
    cursor = connection.cursor()

    # Insert data into the PostgreSQL table
    data = json.loads(record_value)
    for item in data:
        stationcode = item.get('stationcode')
        request_timestamp = item.get('request_timestamp')
        ebike = item.get('ebike')
        mechanical = item.get('mechanical')

        # Vérifier si le stationcode existe déjà dans la table
        cursor.execute("SELECT stationcode FROM fleet WHERE stationcode = %s", (stationcode,))
        existing_station = cursor.fetchone()

        if existing_station:
            # Le stationcode existe déjà, effectuer une mise à jour
            cursor.execute("""
                UPDATE fleet 
                SET 
                    request_timestamp = %s,
                    ebike = %s,
                    mechanical = %s
                WHERE stationcode = %s;
            """, (request_timestamp, ebike, mechanical, stationcode))
        else:
            # Le stationcode n'existe pas encore, effectuer une insertion
            cursor.execute("""
                INSERT INTO fleet (stationcode, request_timestamp, ebike, mechanical)
                VALUES (%s, %s, %s, %s);
            """, (stationcode, request_timestamp, ebike, mechanical))

    # Assurez-vous de valider (commit) les modifications
    connection.commit()

def _pushFleetHistory(record_value):
    # Connect to RDS
    connection = psycopg2.connect(
        host=db_endpoint,
        port=db_port,
        user=db_username,
        password=db_password,
        database=db_name
    )
    cursor = connection.cursor()

    # Insert data into the PostgreSQL table
    data = json.loads(record_value)   
    for item in data:
        fleet_hisotry_query = cursor.execute("""
        INSERT INTO fleet_history (stationcode, request_timestamp, ebike, mechanical)
        VALUES (%s, %s, %s, %s);
        """,
        (item.get('stationcode'), item.get('request_timestamp'), item.get('ebike'), item.get('mechanical'))
        )
    # Commit and close
    connection.commit()
    connection.close()

# Process messages
try:
    while True:
        msg = consumer.poll(1.0) # Search for all non-consumed events. It times out after 1 second
        if msg is None:
            # No message available within timeout.
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print("Waiting for message or event/error in poll()")
            continue
        elif msg.error():
            print('error: {}'.format(msg.error()))
        else:
            
            # Check for Kafka message
            record_key = msg.key()
            record_value = msg.value()
            record_offset = msg.offset() 

            _pushFleet(record_value)
            _pushFleetHistory(record_value)
            

            print("Delete offset " + str(record_offset) + " in record_key " + str(record_key))
            time.sleep(0.1) # Wait half a second
except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()
