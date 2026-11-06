import pika
import json
import psycopg
import time
import signal
import sys
from psycopg_pool import ConnectionPool

def sigterm_handler(signal, frame):
    print("Atempting shutdown....")
    channel.stop_consuming()
    connection.close()
    pool.close()
    sys.exit("Process killed gracefully")

signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigterm_handler)

# RABBITMQ 
exchange_str = 'amq.topic'
queue_str = 'ekco_telemetry_queue'
routing_key_str = 'ekco.*.v1.telemetry.up'
rabbit_user = 'ekco_worker_telemetry'
rabbit_pass = '9kmvH7gOUc24NHK'
vhost = '/'

DB_CONNECTION_STRING = "host=34.135.24.247 dbname=ekco_2028_testdb user=ekco_admin password=milkshakes port=5436"
MIN_CONN = 2
MAX_CONN = 10

try:
    # Initialize the pool
    pool = ConnectionPool(
        conninfo=DB_CONNECTION_STRING,
        min_size=MIN_CONN,
        max_size=MAX_CONN
    )
    print(f"Connection pool initialized: min={MIN_CONN} max={MAX_CONN}")

except Exception as e:
    print(f"Error creating connection pool: {e}")


def callback(ch, method, properties, body):
    db_connection_fail = False
    print(f" [x] {method.routing_key}: {body}")
    #Work task..........
    #Parse json
    try:
        routing_key_elements = method.routing_key.split(".")
        device_serial = int(routing_key_elements[1])
        #print(device_serial)
        x = json.loads(body)
    except Exception as e:
        print("Error: ", e)    
    
    #Get alert
    try:
        alert_str = json.dumps(x["alert"])
        alert_dict = json.loads(alert_str)

        alert = alert_dict["alertName"]
        alert_ts = alert_dict["ts"]

        if alert_ts == 0:
            alert_ts = int(time.time())
            print(alert_ts)
    except Exception as e:
        print("Error no alert: ", e)
    else:
        print("Storing alert data in DB")
        #Store in DB
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO alerts (raw_date, device_serial, "AlertText", timestamp_utc) VALUES (%s, %s, %s, NOW())',
                    (alert_ts, device_serial, alert,))
        except Exception as e:
            print("Error storing in DB: ", e)
            #db_connection_fail = True

    #Get trips
    try:
        trip_str = json.dumps(x["trip"])
        trip_dict = json.loads(trip_str)

        startLat = trip_dict["startLat"]
        startLong = trip_dict["startLong"]
        endLat = trip_dict["endLat"]
        endLong = trip_dict["endLong"]
        kilometres = trip_dict["kilo"]
        startTimestamp = trip_dict["startTs"]
        endTimestamp = trip_dict["endTs"]
        start_location = "POINT({longitude} {latitude})".format(longitude=startLong,latitude=startLat)
        end_location = "POINT({longitude2} {latitude2})".format(longitude2=endLong,latitude2=endLat)
    except Exception as e:
        print("Error no trip: ", e)
    else:
        print("Storing trip data in DB")
        #Store in DB
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO trips (device_serial, start_location, end_location, kilometres, start_time, end_time, timestamp_utc) VALUES (%s, %s, %s, %s, %s, %s, NOW());""",
                    (device_serial, start_location, end_location, kilometres, startTimestamp, endTimestamp,))
        except Exception as e:
            print("Error storing in DB: ", e)
            #db_connection_fail = True

    #Get engine status
    try:
        trip_str = json.dumps(x["engine"])
        trip_dict = json.loads(trip_str)

        ign_status = trip_dict["ignitionStatus"]
        timestamp_engine = trip_dict["ts"]
        
        if timestamp_engine == 0:
            timestamp_engine = int(time.time())
            print(timestamp_engine)          
    except Exception as e:
        print("Error no engine: ", e)
    else:
        print("Storing engine data in DB")
        #Store in DB
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO engine (time, device_serial, ignition_status, timestamp_utc) VALUES (%s, %s, %s, NOW());""",
                    (timestamp_engine, device_serial, ign_status))
        except Exception as e:
            print("Error storing in DB: ", e)
            #db_connection_fail = True

    #Get geofence status
    try:
        trip_str = json.dumps(x["geofenceAlert"])
        trip_dict = json.loads(trip_str)

        alert_geofence = trip_dict["alert"]
        id_geofence = trip_dict["id"]
        timestamp_geofence = trip_dict["ts"]

        
        if timestamp_geofence == 0:
            timestamp_geofence = int(time.time())
            print(timestamp_geofence)          
    except Exception as e:
        print("Error no geofence alert: ", e)
    else:
        print("Storing geofence alert data in DB")
        #Store in DB
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO geofence_alert_ts (time, geofence_id, alert, device_serial) VALUES (%s, %s, %s, %s);""",
                    (timestamp_geofence, id_geofence, alert_geofence, device_serial))
        except Exception as e:
            print("Error storing in DB: ", e)
            #db_connection_fail = True
    
    if db_connection_fail == True:
        print("Requeuing message")
        ch.basic_nack(delivery_tag = method.delivery_tag, requeue=True)
    else:
        ch.basic_ack(delivery_tag = method.delivery_tag)


def rabbitmq_connect():
    # RABBITMQ CONNECTION
    try:
        global connection
        global channel
        credentials = pika.PlainCredentials(rabbit_user, rabbit_pass) #Require username,password
        connection = pika.BlockingConnection(pika.ConnectionParameters('34.135.24.247',5672,vhost,credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange_str, exchange_type='topic', durable=True)
        channel.queue_declare(queue=queue_str, arguments={'x-queue-type': 'quorum'}, durable=True)
        channel.queue_bind(exchange=exchange_str, queue=queue_str, routing_key=routing_key_str)

        channel.basic_consume(queue=queue_str, on_message_callback=callback)
    except Exception as e:
        print("Error: ",e)

    else:
        try:
            print('Waiting for telemetry data')
            channel.start_consuming()
        except Exception as e:
            print("Error: ", e)

while True:                
    rabbitmq_connect()
    time.sleep(1)