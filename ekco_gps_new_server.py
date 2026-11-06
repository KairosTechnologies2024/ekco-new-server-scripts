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
queue_str = 'ekco_gps_001'
routing_key_str = 'ekco.*.v1.gps.up'
rabbit_user = 'ekco_worker_gps'
rabbit_pass = '2sPML8JeUVD6uYD'
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
    
    #Get gps
    try:
        gps_str = json.dumps(x["gps"])
        gps_dict = json.loads(gps_str)

        lat = gps_dict["lat"]
        long = gps_dict["long"]
        speed = gps_dict["speed"]
        #engine = gps_dict["engine"]
        timestamp_gps = gps_dict["ts"]
        point_data = "POINT({longitude} {latitude})".format(longitude=long,latitude=lat)

        if timestamp_gps == 0:
            timestamp_gps = int(time.time())
            print(timestamp_gps)

        #print(lat)
        #print(long)
        #print(speed)
        #print(engine)
        #print(timestamp_gps)
    except Exception as e:
        print("Error: ", e)
    else:
        print("Storing GPS data in DB")
        #Store in DB
        try:
            with pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute('INSERT INTO gps (raw_date, device_serial, "Location", "Speed", timestamp_utc) VALUES (%s, %s, %s, %s, NOW())',
                    (timestamp_gps, device_serial, point_data, speed,))
        except Exception as e:
            print("Error: ", e)


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

        channel.basic_consume(queue=queue_str, on_message_callback=callback, auto_ack=True)
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