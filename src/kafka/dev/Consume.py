from confluent_kafka import Consumer, KafkaError
import urllib.parse as up
import psycopg2
import datetime
import pytz

SOURCE = "www.opeanweather.com"


conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'test_group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
topic = 'quickstart-events'

consumer.subscribe([topic])

while True:
    try:
        while True:
            msg = consumer.poll(1.0) 

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            print(msg.value().decode('utf-8'))

            up.uses_netloc.append("postgres")

            conn = psycopg2.connect(database="gdnjevix",
                                        user="gdnjevix",
                                        password="71O_c8x-OaR_yFJNiatPmqh3N0KjjZhm",
                                        host="floppy.db.elephantsql.com",
                                        port="5432")

            cur = conn.cursor()
            
            decoded_data = msg.value().decode('utf-8')
            datadict = eval(decoded_data)

            execute_timestamp = datetime.datetime.now(pytz.utc)
            ist_timezone = pytz.timezone('Asia/Kolkata')
            ist_execute_timestamp = execute_timestamp.astimezone(ist_timezone)

            #Landing zone data
            lzsql = f'INSERT into LZ_API_DATA(api_data, data_source, sys_insert_datetime) VALUES(\'{decoded_data}\', \'{SOURCE}\',\'{ist_execute_timestamp}\') RETURNING id'
            cur.execute(lzsql)
            conn.commit() 
            api_id = cur.fetchone()[0]

            #weather 
            weather = datadict['weather'][0]

            dim_weather_sql = f'CALL SP_DIM_WEATHER_INSERT_V8(\'{weather["id"]}\', \'{weather["main"]}\', \'{weather["description"]}\', \'{weather["icon"]}\', \'{api_id}\', \'{ist_execute_timestamp}\', null)'
            cur.execute(dim_weather_sql)
            conn.commit()
            weather_id = cur.fetchone()[0]

            #location
            sys = datadict['sys'] #dict
            timezone = datadict['timezone']
            id = datadict['id']
            loc_name = datadict['name']
            coord = datadict['coord'] #dict

            dim_location_sql = f'CALL DIM_LOCATION_INSERT_V5(\'{sys["country"]}\',\'{int(timezone)}\', \'{int(id)}\', \'{loc_name}\', \'{coord["lon"]}\', \'{coord["lat"]}\', \'{api_id}\', \'{ist_execute_timestamp}\', null)'
            cur.execute(dim_location_sql)
            conn.commit()
            location_id = cur.fetchone()[0]
            
            #fact main
            main = datadict['main'] #dict
            wind = datadict['wind'] #dict

            fact_main_sql = f'CALL FACT_MAIN_INSERT_V2(\'{location_id}\', \'{weather_id}\', \'{main["temp"]}\', \'{main["feels_like"]}\', \'{main["temp_min"]}\', \'{main["temp_max"]}\', \'{main["pressure"]}\', \'{main["humidity"]}\', \'{main["sea_level"]}\', \'{main["grnd_level"]}\', \'{wind["speed"]}\', \'{wind["deg"]}\', \'{wind["gust"]}\', \'{api_id}\', \'{ist_execute_timestamp}\')'
            cur.execute(fact_main_sql)
            conn.commit()

    except Exception as e:
        print(e)
        pass
        
    finally:
        #conn.close()
        #consumer.close()
        pass