import pika
import json
import psycopg
import requests
import string
import secrets
from requests.auth import HTTPBasicAuth
import time
import signal
import sys


def sigterm_handler(signal, frame):
    print("Atempting shutdown....")
    channel.stop_consuming()
    connection.close()
    sys.exit("Process killed gracefully")

signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigterm_handler)

# RABBITMQ 
exchange_str = 'amq.topic'
queue_str = 'ekco_processAuth_001'
routing_key_str_recieve = 'ekco.*.v1.requestCredentials'
rabbit_user = 'ekco_worker_processAuth'
rabbit_pass = 'IeYtzBG7R4'
rabbit_url = '34.135.24.247:15672'

# POSTGRES
DB_NAME = "ekco_2028_testdb"
DB_USER = "ekco_admin"
DB_PASS = "milkshakes"
DB_HOST = "34.135.24.247"
DB_PORT = "5436"

permissions_str = {"configure":".*","write":".*","read":".*"}
permissions_topic_str = {"exchange":"amq.topic","write":".*","read":".*"}
vhost = "%2F"
vhost_pika = '/'
rabbitmq_admin = HTTPBasicAuth('testuser', 'testpassword')

# RABBITMQ CONNECTION
#credentials = pika.PlainCredentials(rabbit_user, rabbit_pass) #Require username,password
#connection = pika.BlockingConnection(pika.ConnectionParameters('ekco-tracking.co.za',5672,vhost,credentials))
#channel = connection.channel()

#channel.exchange_declare(exchange=exchange_str, exchange_type='topic', durable=True)
#channel.queue_declare(queue=queue_str)
#channel.queue_bind(exchange=exchange_str, queue=queue_str, routing_key=routing_key_str_recieve)
# channel.queue_declare(queue='device_telemetry_queue_1', durable=True) 

def callback(ch, method, properties, body):
    print(f" [x] {method.routing_key}: {body}")
    #Work task..........
    try:
        #Get serial number
        routing_key_elements = method.routing_key.split(".")
        device_serial_str = routing_key_elements[1]
        device_serial = int(routing_key_elements[1])

        print(device_serial)

        body_data_dict = json.loads(body)
        ekco_board = body_data_dict["board"]
        ekco_revision = body_data_dict["rev"]    
    except Exception as e:
        print("Error: ", e)
    else:
        #Search database        
        try:
            #print(device_serial)
            with psycopg.connect(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=DB_PORT) as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT "BlackListed","Commissioned" FROM "device_profiles" WHERE "DeviceSerial" = %s',[device_serial])
                    result = cur.fetchone()
        except Exception as e:
            print("Error: ", e)      
        else:
            if result == None: #Serial not found in DB
                print("Serial number not found in DB")
                rabbit_user = 'null'
                rabbit_pass = 'null'
                message_body_str = {"user":rabbit_user,"pass":rabbit_pass,"success":False}
                routing_key_str_send = 'ekco.' + device_serial_str + '.v1' + '.credentials'
                message_body_json = json.dumps(message_body_str)
                channel.basic_publish(exchange=exchange_str, routing_key=routing_key_str_send, body=message_body_json)
            else:
                print("Serial number found in DB")
                blacklist = result[0]
                commission = result[1]
                print("Is blacklisted: " + str(blacklist))
                print("Is commissioned: " + str(commission))

                if blacklist == False and commission == True:
                    try:
                        #Create credentials
                        alphabet = string.ascii_letters + string.digits
                        password = ''.join(secrets.choice(alphabet) for i in range(12))
                        password_str = {"password":password,"tags":"device"}
                        rabbit_username = device_serial_str
                        rabbit_username_device = "{serial}".format(serial=device_serial_str)
                        
                        #Create user on rabbitmq
                        #url_add_user = 'http://152.53.82.236:15672/api/users/' + rabbit_username
                        #url_set_permissions = 'http://152.53.82.236:15672/api/permissions/' + vhost + '/' + rabbit_username
                        #url_set_topic_permissions = 'http://152.53.82.236:15672/api/topic-permissions/' + vhost + '/' + rabbit_username

                        url_add_user = "http://{address}/api/users/{username}".format(address=rabbit_url, username=rabbit_username)
                        url_set_permissions = "http://{address_2}/api/permissions/{virtualHost}/{username_2}".format(address_2=rabbit_url, virtualHost=vhost, username_2=rabbit_username)
                        url_set_topic_permissions = "http://{address_3}/api/topic-permissions/{virtualHost_2}/{username_3}".format(address_3=rabbit_url, virtualHost_2=vhost, username_3=rabbit_username)

                        x = requests.put(url=url_add_user, json=password_str , auth=rabbitmq_admin) #Create user
                        print(x)
                        x = requests.put(url=url_set_permissions, json=permissions_str , auth=rabbitmq_admin) #Set permissions
                        print(x)
                        x = requests.put(url=url_set_topic_permissions, json=permissions_topic_str , auth=rabbitmq_admin) #Set topic permissions
                        print(x)

                        message_body_str = {"user":rabbit_username_device,"pass":password,"success":True}
                        message_body_json = json.dumps(message_body_str)

                        #Send to device
                        routing_key_str_send = 'ekco.' + device_serial_str + '.v1' + '.credentials'
                        channel.basic_publish(exchange=exchange_str, routing_key=routing_key_str_send, body=message_body_json)
                    except Exception as e:
                        print("Error: ", e)
                else:
                    print("Cannot commission device")
                    rabbit_user = 'null'
                    rabbit_pass = 'null'
                    message_body_str = {"user":rabbit_user,"pass":rabbit_pass,"success":False}
                    routing_key_str_send = 'ekco.' + device_serial_str + '.v1' + '.credentials'
                    message_body_json = json.dumps(message_body_str)
                    channel.basic_publish(exchange=exchange_str, routing_key=routing_key_str_send, body=message_body_json)
    try:
        board_success = body_data_dict["blacklist"]
    except Exception as e:
        print("Error: ", e)
    else:
        try:
            with psycopg.connect(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=DB_PORT) as conn:
                with conn.cursor() as cur:
                    cur.execute('UPDATE "DeviceProfiles" SET "BlackListed" = %s WHERE "DeviceSerial" = %s',[board_success],[device_serial])
                    result = cur.fetchone()
        except Exception as e:
            print("Error: ", e)
                
    ch.basic_ack(delivery_tag = method.delivery_tag)

#channel.basic_qos(prefetch_count=1)
#channel.basic_consume(queue=queue_str, on_message_callback=callback) 

#print('Waiting for telemetry data')

#channel.start_consuming()

def rabbitmq_connect():
    # RABBITMQ CONNECTION
    try:
        global channel
        global connection
        credentials = pika.PlainCredentials(rabbit_user, rabbit_pass) #Require username,password
        connection = pika.BlockingConnection(pika.ConnectionParameters('34.135.24.247',5672,vhost_pika,credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange=exchange_str, exchange_type='topic', durable=True)
        channel.queue_declare(queue=queue_str, arguments={'x-queue-type': 'quorum'}, durable=True)
        channel.queue_bind(exchange=exchange_str, queue=queue_str, routing_key=routing_key_str_recieve)

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
