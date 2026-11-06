import time
import signal
import sys
import pika
import psycopg


def sigterm_handler(signal, frame):
    print("Atempting shutdown....")
    channel.stop_consuming()
    connection.close()
    sys.exit("Process killed gracefully")

signal.signal(signal.SIGTERM, sigterm_handler)
signal.signal(signal.SIGINT, sigterm_handler)

# RABBITMQ 
exchange_str = 'amq.topic'
queue_str = 'ekco_processDevProfile_001'
routing_key_str_recieve = 'ekco.*.v1.requestDevProfile'
rabbit_user = 'request_device_profile'
rabbit_pass = 'iCmQiobbKvWvxeu'
rabbit_url = '34.135.24.247:15672'
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
   
    except Exception as e:
        print("Error: ", e)
    else:
        #Search database        
        try:
            #print(device_serial)
            with psycopg.connect(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=DB_PORT) as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT "InputOne", "InputTwo", "InputThree", "InputOneInvert", "InputTwoInvert", "InputThreeInvert", "SubscriptionPlan" FROM "device_profiles" WHERE "DeviceSerial" = %s',[device_serial])
                    result = cur.fetchone()
                    
                    if result != None:
                        #for record in result:
                        #print(record[0])
                        #print(record[1])
                        input_one_db = result[0]
                        input_two_db = result[1]
                        input_three_db = result[2]
                        input_one_invert_db = result[3]
                        input_two_invert_db = result[4]
                        input_three_invert_db = result[5]
                        ekco_basic_db = result[6]

                        input_one_db_int = 0
                        input_two_db_int = 0
                        input_three_db_int = 0
                        input_one_invert_db_int = 0
                        input_two_invert_db_int = 0
                        input_three_invert_db_int = 0
                        ekco_basic_db_int = 0

                        print(result[0])
                        print(result[1])
                        print(result[2])
                        print(result[3])
                        print(result[4])
                        print(result[5])
                        print(result[6])

                        if input_one_db == True:
                            input_one_db_int = 1
                        if input_two_db == True:
                            input_two_db_int = 1
                        if input_three_db == True:
                            input_three_db_int = 1
                        if input_one_invert_db == True:
                            input_one_invert_db_int = 1
                        if input_two_invert_db == True:
                            input_two_invert_db_int = 1
                        if input_three_invert_db == True:
                            input_three_invert_db_int = 1
                        if ekco_basic_db == 1:
                            ekco_basic_db_int = 1

                        message_body = str(input_one_invert_db_int) + ':' + str(input_one_db_int) + ':' + str(input_two_invert_db_int) + ':' + str(input_two_db_int) + ':' + str(input_three_invert_db_int) + ':' + str(input_three_db_int) + ':' + str(ekco_basic_db_int)

                        #Send to device
                        routing_key_str_send = 'ekco.' + device_serial_str + '.v1' + '.devProfile'
                        channel.basic_publish(exchange=exchange_str, routing_key=routing_key_str_send, body=message_body)

                    else:
                        print("Serial number not found in DB")

        except Exception as e:
            print("Error: ", e)      
                
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