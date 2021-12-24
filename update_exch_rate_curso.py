###########################################################################
## Load Libraries
###########################################################################
import unicorn_binance_websocket_api.unicorn_binance_websocket_api_manager as ubwam
import threading
import time
from datetime import datetime, timedelta, date
import warnings
import sys
import glob
import random
import MySQLdb
#warnings.filterwarnings("ignore")

def wsGetPrice(symbol_ws):
    binance_websocket_api_manager = ubwam.BinanceWebSocketApiManager(exchange="binance.com")
    stream_id = binance_websocket_api_manager.create_stream("trade", symbol_ws, output="UnicornFy")
    print("stream_id:", stream_id)

    mydb = MySQLdb.connect(
      host="localhost",
      user="user",
      passwd="passwd",
      database="database"
    )
    mycursor = mydb.cursor()

    mycursor.execute("SELECT currency, currency_vs FROM markets WHERE symbol = %s", (symbol_ws,))
    myresult = mycursor.fetchone()
    currency = myresult[0]
    currency_vs = myresult[1]

    while True:
        data = binance_websocket_api_manager.pop_stream_data_from_stream_buffer()
        try:
            if data['stream_type']:
                current_price = float(data['price'])
                #print("symbol_ws:", symbol_ws, "--price: ", current_price)
                #secs_sleep_extra = random.randint(3, 5)
                mycursor.execute("INSERT INTO exchange_rate(currency, currency_vs, price, datetime ) VALUES (%s, %s, %s, %s)", (currency, currency_vs, current_price, datetime.now(),) )
                mydb.commit()
                time.sleep(4)
                #secs_sleep_extra = random.randint(3, 5)
                #time.sleep(secs_sleep_extra)

                #print(data)
        except KeyError:
            #print("k")
            pass
        except TypeError:
            #print("t")
            pass
        except Exception as e:
            print('msg original de error: ', )
            print(symbol_ws, '- [' + type(e).__name__ + ']', str(e))

            error_type = type(e).__name__
            if error_type == 'DatabaseError':
                print("reiniciando stream:", stream_id, "con el comando: binance_websocket_api_manager.set_restart_request(stream_id)")
                binance_websocket_api_manager.set_restart_request(stream_id)

print("")
print(datetime.now())

mydb_setup = MySQLdb.connect(
  host="localhost",
  user="user",
  passwd="passwd",
  database="database"
)
mycursor_setup = mydb_setup.cursor()

mycursor_setup.execute("SELECT symbol FROM markets")
m = mycursor_setup.fetchall()
print("m:", m)
threads = []

for i in m:
    print(i[0])
    threads.append(threading.Thread(target=wsGetPrice, args=(i[0],)))

i = 0
for t in threads:
    print("i:",i)
    i += 1
    t.start()
