import pika
import json
import psycopg
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
queue_str = 'ekco_health_queue'
routing_key_str = 'ekco.*.v1.health'
rabbit_user = 'ekco_worker_health'
rabbit_pass = 'PFrFxo3XVqN3wtF'
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
    #Parse json
    try:
        routing_key_elements = method.routing_key.split(".")
        device_serial = int(routing_key_elements[1])
        #print(device_serial)
        x = json.loads(body)
    except Exception as e:
        print("Error: ", e)    
    
    #Get deviceHealth params
    try:
        deviceHealth_str = json.dumps(x["deviceHealth"])
        deviceHealth_dict = json.loads(deviceHealth_str)

        bat_status = deviceHealth_dict["batStatus"]
        device_volt = deviceHealth_dict["deviceVolt"]
        firmware_version = deviceHealth_dict["firmwareVersion"]
        board_rev = deviceHealth_dict["boardRev"]
        timestamp_dev_health = deviceHealth_dict["ts"]
        bat_status_bool = False

        if timestamp_dev_health == 0:
            timestamp_dev_health = int(time.time())
            print(timestamp_dev_health)

        if bat_status == 1:
            bat_status_bool = True

    except Exception as e:
        print("Error: ", e)
    else:
        print("Storing device health data in DB")
        #Store in DB
        try:
            with psycopg.connect(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=DB_PORT) as conn:
                with conn.cursor() as cur:
                    cur.execute('UPDATE device_health SET "BatteryStatus"=%s, "BatteryVoltage"=%s, "FirmwareVersion"=%s, "Revision"=%s, "UpdatedAt"=NOW() WHERE device_serial=%s',
                    (bat_status, device_volt, firmware_version, board_rev, device_serial))
        except Exception as e:
            print("Error: ", e)

    #Get motorHealth params
    try:
        motorHealth_str = json.dumps(x["motorHealth"])
        motorHealth_dict = json.loads(motorHealth_str)

        cycle_count = motorHealth_dict["cycleCount"]
    except Exception as e:
        print("Error: ", e)
    else:
        print("Storing motor health data in DB")
        print("Cycle count: " + str(cycle_count))
        
        if cycle_count > 0:
            #Store in DB
            try:
                with psycopg.connect(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=DB_PORT) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""SELECT motor_cycles, motor_serial FROM actuators WHERE device_serial=%s;""",
                        [device_serial])
                        
                        result = cur.fetchall()
                    
                        for record in result:
                            #print(record[0])
                            #print(record[1])
                            motor_cycles_db = record[0]
                            motor_serial_number = record[1]
                            updated_motor_cycles = cycle_count + motor_cycles_db
                            print("UPDATED MOTOR CYCLES: " + str(updated_motor_cycles))
                            cur.execute("""UPDATE actuators SET motor_cycles=%s WHERE motor_serial=%s;""",
                            (updated_motor_cycles, motor_serial_number))
            except Exception as e:
                print("Error: ", e)

    #Get gyroHealth params
    try:
        gyroHealth_str = json.dumps(x["gyroHealth"])
        gyroHealth_dict = json.loads(gyroHealth_str)

        gyroHealth_status = gyroHealth_dict["status"]
        gyro_health_status_bool = False

        if gyroHealth_status == 1:
            gyro_health_status_bool = True
    except Exception as e:
        print("Error: ", e)
    else:
        print("Storing gyro health status in DB")
        print("Gyro health: " + str(gyroHealth_status))
        #Store in DB
        try:
            with psycopg.connect(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=DB_PORT) as conn:
                with conn.cursor() as cur:
                    cur.execute("""UPDATE device_health SET gyro_health_status=%s WHERE device_serial=%s;""",
                    (gyro_health_status_bool, device_serial))
        except Exception as e:
            print("Error: ", e)

    #Get lockHealth params
    try:
        lockHealth_str = json.dumps(x["lockHealth"])
        lockHealth_dict = json.loads(lockHealth_str)

        lockHealth_status = lockHealth_dict["status"]
        lock_health_status_bool = False

        if lockHealth_status == 1:
            lock_health_status_bool = True
    except Exception as e:
        print("Error: ", e)
    else:
        print("Storing lock health status in DB")
        print("lock health: " + str(lockHealth_status))
        #Store in DB
        try:
            with psycopg.connect(dbname=DB_NAME,user=DB_USER,password=DB_PASS,host=DB_HOST,port=DB_PORT) as conn:
                with conn.cursor() as cur:
                    cur.execute("""UPDATE device_health SET lock_health=%s WHERE device_serial=%s;""",
                    (lock_health_status_bool, device_serial))
        except Exception as e:
            print("Error: ", e)          

    ch.basic_ack(delivery_tag = method.delivery_tag)

#channel.basic_qos(prefetch_count=1)
#channel.basic_consume(queue=queue_str, on_message_callback=callback)

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