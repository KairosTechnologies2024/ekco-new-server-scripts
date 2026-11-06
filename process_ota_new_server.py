import time
import signal
import sys
import pika
import psycopg
import json

def sigterm_handler(signal, frame):
    print("Atempting shutdown....")
    channel.stop_consuming()
    connection.close()
    sys.exit("Process killed gracefully")

signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigterm_handler)

# RABBITMQ 
exchange_str = 'amq.topic'
queue_str = 'ekco_requestOTA_001'
routing_key_str_recieve = 'ekco.*.v1.requestOTA'
rabbit_user = 'ekco_worker_request_ota'
rabbit_pass = 'UKotPSO904hj7Ru'
rabbit_url = '34.135.24.247'
vhost = '/'

# POSTGRES
DB_NAME = "ekco_2028_testdb"
DB_USER = "ekco_admin"
DB_PASS = "milkshakes"
DB_HOST = "34.135.24.247"
DB_PORT = "5436"


def callback(ch, method, properties, body):
    print(f" [x] {method.routing_key}: {body}")
    #Work task..........
    try:
        #Get serial number
        routing_key_elements = method.routing_key.split(".")
        device_serial_str = routing_key_elements[1]
        device_serial = int(routing_key_elements[1])
        routing_key_str_send_url = 'ekco.' + device_serial_str + '.json.v1' + '.OTA'

        body_data_dict = json.loads(body)
        designation = body_data_dict["designation"]
   
    except Exception as e:
        print("Error 1: ", e)
    else:
        #Search database        
        try:
            #print(device_serial)
            with psycopg.connect(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=DB_PORT) as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT "AllowOta" FROM device_profiles WHERE "DeviceSerial" = %s',[device_serial])
                    result = cur.fetchone()
                    
                    if result != None:
                        ota_flag = result[0]

                        if ota_flag == True:
                            print("Device allowed to update !")
                            #Search firmware table
                            cur.execute("""SELECT firmware_version,firmware_url FROM device_firmware WHERE firmware_designation = %s;""",[designation])
                            result_firmware = cur.fetchone()

                            if result_firmware != None:
                                firmware_version = result_firmware[0]
                                firmware_url = result_firmware[1]
                                    
                                print("Firmware version server: " + str(firmware_version))
                                print("Firmware URL server: " + str(firmware_url))

                                #Send to device
                                message_body_str = {"link":firmware_url,"version":firmware_version,"success":True}
                                message_body_json = json.dumps(message_body_str)
                                channel.basic_publish(exchange=exchange_str, routing_key=routing_key_str_send_url, body=message_body_json)
                                
                            else:
                                print("Could not find firmware that matches designation !")
                                #Send to device
                                message_body_str = {"link":'null',"version":0,"success":False}
                                message_body_json = json.dumps(message_body_str)
                                channel.basic_publish(exchange=exchange_str, routing_key=routing_key_str_send_url, body=message_body_json)

                        else:
                            print("Device not allowed to update !")
                            #Send to device
                            message_body_str = {"link":'null',"version":0,"success":False}
                            message_body_json = json.dumps(message_body_str)
                            channel.basic_publish(exchange=exchange_str, routing_key=routing_key_str_send_url, body=message_body_json)

                    else:
                        print("Serial number not found in DB")

        except Exception as e:
            print("Error 2: ", e)      
                
    ch.basic_ack(delivery_tag = method.delivery_tag)


def rabbitmq_connect():
    # RABBITMQ CONNECTION
    try:
        global channel
        global connection
        credentials = pika.PlainCredentials(rabbit_user, rabbit_pass) #Require username,password
        connection = pika.BlockingConnection(pika.ConnectionParameters('34.135.24.247',5672,vhost,credentials))
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
