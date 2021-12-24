###########################################################################
## Load Libraries
###########################################################################
import time
from datetime import datetime, timedelta, date
import warnings
import MySQLdb
import gc
import ccxt
import dogs
import sys
import random
import numpy as np
import talib as talib

class Bunch(object):
    def __init__(self, adict):
        self.__dict__.update(adict)

warnings.filterwarnings("ignore")

#Identify the pair of cryptos based on the name of the program
name = sys.argv[0].lower()
a_name = name.split('_')
src = a_name[2]
to_a = a_name[3]
a_to = to_a.split('.')
to = a_to[0]

symbol = src.upper() + '/' + to.upper()
now = datetime.now()

try:
    mydb = MySQLdb.connect(
      host="localhost",
      user="user",
      passwd="passwd",
      database="database"
    )
    mycursor = mydb.cursor()

    mycursor.execute("SELECT id FROM global_parameters WHERE currency = %s AND currency_vs = %s", (src, to))
    myresult = mycursor.fetchone()
    gp_id = int(myresult[0])

    ###########################################################################
    # Get current status
    ###########################################################################
    mycursor.execute("SELECT status, diffs, break_flag, break_counter, mins_trnx FROM status WHERE gp_id = %s", (gp_id,))
    myresult = mycursor.fetchone()
    status = int(myresult[0])
    break_db = int(myresult[2])
    break_counter = int(myresult[3])

    #If there is a problem you get some time to solve it and stop the calls to the Exchange API
    if break_db > 0:
        if break_counter >= 1000:
            mycursor.execute("UPDATE status SET break_flag = 0, break_counter = 0 WHERE gp_id = %s", (gp_id,))
            mydb.commit()
        else:
            break_counter += 1
            mycursor.execute("UPDATE status SET break_counter = %s WHERE gp_id = %s", (break_counter, gp_id,))
            mydb.commit()
            print('break_counter: ', break_counter)
            secs_sleep = 90
            secs_sleep_db = 20
            secs_sleep_trnx = 4
    else:
        #Check if it is the first execution of the program
        try:
            mycursor.execute("SELECT id FROM historical WHERE gp_id = %s", (gp_id,))
            myresult = mycursor.fetchone()
            hist_counter = int(myresult[0])
            if hist_counter is None:
                hist_counter = 0
        except:
            hist_counter = 0

        if hist_counter > 0:
            option = 0
        else:
            option = 102
            sar = 0

        ###########################################################################
        # Get global parameters
        ###########################################################################
        mycursor.execute("SELECT secs_resell, secs_sleep, secs_sleep_trnx, partial_buy_amt, partial_sell_amt, \
                            earning_pct_pt, id, FROM global_parameters WHERE id = %s LIMIT 1", (gp_id,))
        myresult = mycursor.fetchone()
        secs_resell = int(myresult[2])
        secs_sleep = int(myresult[3])
        secs_sleep_db = secs_sleep
        partial_buy_amt = float(myresult[6])
        partial_sell_amt = float(myresult[7])
        earning_pct_pt = float(myresult[10])
        id_curr = int(myresult[15])

        #Check if there are funds to make a transaction
        try:
            mycursor.execute("SELECT saldo FROM saldo_total WHERE currency = %s AND DATE(datetime) = CURDATE() ORDER BY id DESC LIMIT 1", (to,))
            myresult = mycursor.fetchone()
            total_reserve = float(myresult[0])
        except Exception as e:
            print('[2] msg original de error: ', )
            print('[' + type(e).__name__ + ']', str(e))
            print('error al consultar el saldo en:', to)

        try:
            ###########################################################################
            # Get current price
            ###########################################################################
            dt_limit = datetime.now() - timedelta(minutes=5)
            mycursor.execute("SELECT count(*) FROM exchange_rate where currency = %s and currency_vs = %s and datetime > %s", (src, to, dt_limit,))
            myresult = mycursor.fetchone()
            count_current_price = int(myresult[0])
            if count_current_price > 0:
                mycursor.execute("SELECT price FROM exchange_rate where currency = %s and currency_vs = %s ORDER BY datetime DESC LIMIT 1", (src, to,))
                myresult = mycursor.fetchone()
                current_price = float(myresult[0])
            else:
                print("Consultando el current_price usando la API")
                if (dogs.exchange.has['fetchTicker']):
                    ticker = Bunch( dogs.exchange.fetch_ticker(symbol) )
                    current_price =  float(ticker.close)
        except Exception as e:
            print("se asigno el ultimo precio del historico")
            mycursor.execute("SELECT real_price FROM historical WHERE real_price != 0 and gp_id = %s ORDER BY datetime DESC LIMIT 1", (gp_id,))
            myresult = mycursor.fetchone()
            current_price = float(myresult[0])

            print('[2] msg original de error: ', )
            print('[' + type(e).__name__ + ']', str(e))
            print('error al consultar el precio actual')

            error_type = type(e).__name__
            if error_type == 'DDoSProtection':
                secs_sleep_extra = random.randint(100, 200)
                print("Durmiendo extra:", secs_sleep_extra)
                time.sleep(secs_sleep_extra)

        if current_price == 0:
            print("Los precios actuales tienen valor cero, se asigna el ultimo valor del historico diferente de cero")
            mycursor.execute("SELECT real_price FROM historical WHERE real_price != 0 and gp_id = %s ORDER BY id DESC LIMIT 1", (gp_id,))
            myresult = mycursor.fetchone()
            current_price = float(myresult[0])

        #Get the price for the second crypto in usdt
        if to == 'usdt':
            price_usdt = current_price
        else:
            try:
                dt_limit = datetime.now() - timedelta(minutes=5)
                mycursor.execute("SELECT id FROM exchange_rate where currency = %s and currency_vs = 'usdt' and datetime > %s", (src, dt_limit,))
                myresult = mycursor.fetchone()
                count_price_usdt = int(myresult[0])
                if count_price_usdt > 0:
                    ###########################################################################
                    # Get current price in usdt
                    ###########################################################################
                    mycursor.execute("SELECT price FROM exchange_rate where currency = %s and currency_vs = 'usdt' ORDER BY id DESC LIMIT 1", (src,))
                    myresult = mycursor.fetchone()
                    price_usdt = float(myresult[0])
                else:
                    print("Consultando el price_usdt usando la API")
                    symbol_usdt = src.upper() + '/' + 'USDT'
                    if (dogs.exchange.has['fetchTicker']):
                        ticker = Bunch( dogs.exchange.fetch_ticker(symbol_usdt) )
                        price_usdt =  float(ticker.close)
            except Exception as e:
                print("se asigno el ultimo precio del historico para el precio en usdt")
                mycursor.execute("SELECT price_usdt FROM historical WHERE price_usdt != 0 AND gp_id = %s ORDER BY id DESC LIMIT 1", (gp_id,))
                myresult = mycursor.fetchone()
                price_usdt = float(myresult[0])

                print('[2] msg original de error: ', )
                print('[' + type(e).__name__ + ']', str(e))
                print('error al consultar el precio actual en usdt para:', src)

                error_type = type(e).__name__
                if error_type == 'DDoSProtection':
                    secs_sleep_extra = random.randint(100, 200)
                    print("Durmiendo extra:", secs_sleep_extra)
                    time.sleep(secs_sleep_extra)

        if option == 0:

            if status == 0:
                print('========================================================== STATUS0 ====================================================================')
                print('Define the conditions to start trading ')

                mycursor.execute("UPDATE status SET status = 1 WHERE gp_id = %s", (gp_id,))
                mydb.commit()

                print('Start trading')

            if status == 1:
                print('========================================================== BUY / SELL ====================================================================')

                # it is possible to update the purchase parameters dynamically based on statistical indicators by example the Parabolic SAR indicator:
                # The parameters are updated every 10 minutes
                if now.minute % 10 == 0 :

                    #SAR
                    acceleration=0.02
                    maximum=0.20
                    candles = dogs.exchange.fetch_ohlcv(symbol, '1h')
                    high_data = []
                    low_data = []

                    for candle in candles:
                        high_data.append(candle[2])
                        low_data.append(candle[3])

                    hi = np.asarray(high_data)
                    low = np.asarray(low_data)

                    sar_values = talib.SAR(hi, low, acceleration, maximum)
                    sar_1h = sar_values[-1]

                    #If market goes up you can increment the amount to buy and the earning percentage and reduce if market goes down
                    if current_price / sar_1h < 1.01 :
                        amount_buy = amount_buy * 1.01
                        earning_pct = earning_pct * 1.01
                    else:
                        amount_buy = amount_buy * 0.99
                        earning_pct = earning_pct * 0.99

                # Here you should define the condition to buy
                b_confirm_buy = True

                if b_confirm_buy and total_reserve > reserve_amt:
                    secs_sleep = secs_resell
                    try:
                        trnx_price = current_price
                        amount_trnx = round(amount_buy/price_usdt, 8)
                        print('amount_trnx:', amount_trnx, 'trnx_price:', trnx_price)

                        result = Bunch( dogs.exchange.create_limit_buy_order(symbol, amount_trnx, trnx_price) )
                        ex_id_trnx = result.id
                        print('ex_id_trnx:', ex_id_trnx)

                        mycursor.execute("INSERT INTO `compra_subida_trnx`(`gp_id`, `ex_id`, `datetime`, `price`, `amt`, `status`, `substatus`) VALUES (%s, %s, %s, %s, %s, %s, %s)", (gp_id, ex_id_trnx, datetime.now(), float(trnx_price), float(amount_trnx), 0, pb_substatus, ) )
                        mydb.commit()

                    except Exception as e:
                        print('msg original de error: ', )
                        print('[' + type(e).__name__ + ']', str(e))
                        print('error al enviar la compra')

                        error_type = type(e).__name__
                        if error_type == 'DDoSProtection':
                            secs_sleep_extra = random.randint(100, 200)
                            print("Durmiendo extra:", secs_sleep_extra)
                            time.sleep(secs_sleep_extra)

                        if error_type == 'InsufficientFunds':
                            secs_sleep_extra = random.randint(60, 90)
                            print("Compra InsufficientFunds Durmiendo extra:", secs_sleep_extra)
                            time.sleep(secs_sleep_extra)

                        pass
                if total_to_sell > min_usdt_trnx:
                    print('#========================================================== SELL =========================================================================================================#')

                    #Here you check if you have any crypto to sell acording the earning percentage you want to gain
                    limit_price = current_price * earning_pct_pt
                    pt_substatus = 10

                    try:
                        mycursor.execute("SELECT SUM(amt) FROM compra_subida_trnx WHERE gp_id = %s AND status > 0 and price <= %s", (gp_id, limit_price,))
                        myresult = mycursor.fetchone()
                        total_trnx_compra_lim = float(myresult[0])
                    except:
                        total_trnx_compra_lim = 0

                    try:
                        sell_limit_price = current_price
                        #no se consideran los residuos
                        mycursor.execute("SELECT SUM(amt) FROM venta_subida_trnx WHERE gp_id = %s AND price <= %s", (gp_id, sell_limit_price))
                        myresult = mycursor.fetchone()
                        total_trnx_venta_lim = float(myresult[0])
                        if total_trnx_venta_lim is None:
                            total_trnx_venta_lim = 0
                    except Exception as e:
                        total_trnx_venta_lim = 0

                    amount_trnx = round(partial_sell_amt/price_usdt, 8)
                    min_amount_trnx = round(min_usdt_trnx/price_usdt, 8)

                    amt_sell = total_trnx_compra_lim - total_trnx_venta_lim

                    if round(amt_sell, 4) >= round(min_amount_trnx,4):
                        b_confirm_sell = True
                        if round(amt_sell, 4) < round(amount_trnx,4):
                            amount_trnx = amt_sell

                        if b_confirm_sell:
                            secs_sleep = secs_resell
                            try:
                                trnx_price = current_price

                                result = Bunch( dogs.exchange.create_limit_sell_order(symbol, amount_trnx, trnx_price) )
                                ex_id_trnx = result.id

                                mycursor.execute("INSERT INTO `venta_subida_trnx`(`gp_id`, `ex_id`, `datetime`, `price`, `amt`, `status`, `substatus`, `earning_pct_pt`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                                                    (gp_id, ex_id_trnx, datetime.now(), float(trnx_price), float(amount_trnx), 0, pt_substatus, earning_pct_pt, ) )
                                mydb.commit()

                            except Exception as e:
                                print('msg original de error: ', )
                                print('[' + type(e).__name__ + ']', str(e))
                                print('error al enviar la venta')
                                b_confirm_sell = False
                                if b_confirm_buy == False:
                                    secs_sleep = secs_sleep_trnx

                                error_type = type(e).__name__
                                if error_type == 'DDoSProtection':
                                    secs_sleep_extra = random.randint(100, 200)
                                    print("Durmiendo extra:", secs_sleep_extra)
                                    time.sleep(secs_sleep_extra)

                                if error_type == 'InsufficientFunds':
                                    secs_sleep_extra = random.randint(60, 90)
                                    print("Venta InsufficientFunds Durmiendo extra:", secs_sleep_extra)
                                    time.sleep(secs_sleep_extra)

                                pass

        elif option == 102:
            diff = 0
            h_diffs = 0

        sql = ("INSERT INTO historical (gp_id, datetime, real_price, diff, h_diffs, diffs_bd, price_usdt, sar) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
        data = ( gp_id, datetime.now(), current_price, float(diff), float(h_diffs), float(diffs_bd), float(price_usdt), float(sar) )
        mycursor.execute(sql, data)
        mydb.commit()

        print('diff: ', diff, ' current: ', current_price, 'price_usdt:', price_usdt)

    ############################################################################
    # Data sampling is performed while waiting time elapses
    ############################################################################
    try:
        print('secs_sleep:', secs_sleep, 'secs_sleep_db:', secs_sleep_db)
        if secs_sleep > secs_sleep_db:
            print('recolectando los valores dentro del ciclo secs_sleep')
            dt_limit_rec = datetime.now() + timedelta(seconds=secs_sleep)

            while True:
                try:
                    ###########################################################################
                    # Get current price
                    ###########################################################################
                    dt_limit = datetime.now() - timedelta(minutes=5)
                    mycursor.execute("SELECT count(*) FROM exchange_rate where currency = %s and currency_vs = %s and datetime > %s", (src, to, dt_limit,))
                    myresult = mycursor.fetchone()
                    count_current_price = int(myresult[0])
                    if count_current_price > 0:
                        mycursor.execute("SELECT price FROM exchange_rate where currency = %s and currency_vs = %s ORDER BY datetime DESC LIMIT 1", (src, to,))
                        myresult = mycursor.fetchone()
                        current_price = float(myresult[0])
                    else:
                        print("Consultando el current_price usando la API")
                        if (dogs.exchange.has['fetchTicker']):
                            ticker = Bunch( dogs.exchange.fetch_ticker(symbol) )
                            current_price =  float(ticker.close)
                            secs_sleep_extra = random.randint(30, 60)
                            print("Durmiendo extra:", secs_sleep_extra)
                            time.sleep(secs_sleep_extra)
                except Exception as e:
                    print("se asigno el ultimo precio del historico")
                    mycursor.execute("SELECT real_price FROM historical WHERE real_price != 0 AND gp_id = %s ORDER BY datetime DESC LIMIT 1", (gp_id,))
                    myresult = mycursor.fetchone()
                    current_price = float(myresult[0])

                    print('[2] msg original de error: ', )
                    print('[' + type(e).__name__ + ']', str(e))
                    print('error al consultar el precio actual')

                    error_type = type(e).__name__
                    if error_type == 'DDoSProtection':
                        secs_sleep_extra = random.randint(100, 200)
                        print("Durmiendo extra:", secs_sleep_extra)
                        time.sleep(secs_sleep_extra)

                if current_price == 0:
                    print("Los precios actuales tienen valor cero, se asigna el ultimo valor del historico diferente de cero")
                    mycursor.execute("SELECT real_price FROM historical WHERE real_price != 0 AND gp_id = %s ORDER BY datetime DESC LIMIT 1", (gp_id,))
                    myresult = mycursor.fetchone()
                    current_price = float(myresult[0])

                mycursor.execute("SELECT diff, real_price, h_diffs FROM historical WHERE gp_id = %s ORDER BY datetime DESC LIMIT 1", (gp_id,))
                myresult = mycursor.fetchone()
                last_diff = float(myresult[0])
                last_gm_price = float(myresult[1])
                last_diffs = float(myresult[2])

                diff = float(current_price - last_gm_price)
                h_diffs = last_diffs + diff
                diffs = diff + last_diff

                diffs_bd = diffs_bd + diff

                sql = ("INSERT INTO historical (gp_id, datetime, real_price, diff, h_diffs, diffs_bd, price_usdt, sar) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
                data = ( gp_id, datetime.now(), current_price, float(diff), float(h_diffs), float(diffs_bd), float(price_usdt), float(sar) )
                mycursor.execute(sql, data)
                mydb.commit()

                time.sleep(secs_sleep_trnx)

                if datetime.now() > dt_limit_rec:
                    break
        else:
            time.sleep(secs_sleep)

    except Exception as e:
        print('[13] msg original de error: ', )
        print('[' + type(e).__name__ + ']', str(e))
        print('error al ejecutar: time.sleep(secs_sleep), secs_sleep:', secs_sleep)
        time.sleep(20)

    ############################################################################
    # Canceling and / or updating recently launched purchase transactions
    ############################################################################
    if b_confirm_buy or b_after_buy:
        try:
            mycursor.execute("SELECT ex_id FROM compra_subida_trnx WHERE status = 0 and gp_id = %s ORDER BY datetime DESC LIMIT 3", (gp_id,))
            trnxs = mycursor.fetchall()

            for trnxs_id in trnxs:
                for ex_id_buy in trnxs_id:
                    print('Verificando la trnx ex_id_buy:', ex_id_buy)
                    try:
                        order = Bunch(dogs.exchange.fetch_order(ex_id_buy, symbol))
                        print("Order status:", order.status)
                        now = datetime.now()

                        if order.status == 'closed':
                            real_buy_price = order.price
                            mycursor.execute("UPDATE compra_subida_trnx SET price = %s, dt_exec = %s, status = 1 WHERE ex_id = %s", (float(real_buy_price), now, ex_id_buy) )
                            mydb.commit()
                        else:
                            if order.status != 'canceled':
                                try:
                                    print("Cancelando la trnx, ex_id_buy:", ex_id_buy)
                                    mycursor.execute("UPDATE compra_subida_trnx SET dt_exec = %s, status = -1 WHERE ex_id = %s", (now, ex_id_buy) )
                                    mydb.commit()

                                    dogs.exchange.cancel_order (ex_id_buy, symbol)

                                except Exception as e:
                                    print('[11] msg original de error: ', )
                                    print('[' + type(e).__name__ + ']', str(e))
                                    print('error al cancelar la compra con id: ', ex_id_buy)
                            elif order.status == 'canceled':
                                mycursor.execute("UPDATE compra_subida_trnx SET dt_exec = %s, status = -1 WHERE ex_id = %s", (now, ex_id_buy) )
                                mydb.commit()
                    except Exception as e:
                        print('[12] msg original de error: ', )
                        print('[' + type(e).__name__ + ']', str(e))
                        print('error al consultar los datos de la trnx para verificar que se llevo a cabo, con id: ', ex_id_buy)

                        error_type = type(e).__name__
                        if error_type == 'DDoSProtection':
                            secs_sleep_extra = random.randint(100, 200)
                            print("Durmiendo extra:", secs_sleep_extra)
                            time.sleep(secs_sleep_extra)

        except Exception as e:
            print('msg original de error: ', )
            print('[' + type(e).__name__ + ']', str(e))
            print('No hay transacciones a cancelar')

    ############################################################################
    # Canceling and / or updating recently launched sales
    ############################################################################
    if b_confirm_sell or b_after_sell:
        try:
            mycursor.execute("SELECT ex_id FROM venta_subida_trnx WHERE status = 0 and gp_id = %s ORDER BY datetime DESC LIMIT 3", (gp_id,))
            trnxs = mycursor.fetchall()

            for trnxs_id in trnxs:
                for ex_id_sell in trnxs_id:
                    print('Verificando la trnx ex_id_sell:', ex_id_sell)
                    try:
                        order = Bunch(dogs.exchange.fetch_order(ex_id_sell, symbol))
                        print("Order status:", order.status)
                        now = datetime.now()

                        if order.status == 'closed':
                            real_buy_price = order.price
                            mycursor.execute("UPDATE venta_subida_trnx SET price = %s, dt_exec = %s, status = 1 WHERE ex_id = %s", (float(real_buy_price), now, ex_id_sell) )
                            mydb.commit()
                        else:
                            if order.status != 'canceled':
                                try:
                                    print("Cancelando la venta, ex_id_sell:", ex_id_sell)
                                    mycursor.execute("UPDATE venta_subida_trnx SET dt_exec = %s, status = -1 WHERE ex_id = %s", (now, ex_id_sell) )
                                    mydb.commit()

                                    dogs.exchange.cancel_order (ex_id_sell, symbol)

                                except Exception as e:
                                    print('[11] msg original de error: ', )
                                    print('[' + type(e).__name__ + ']', str(e))
                                    print('error al cancelar la venta con id: ', ex_id_sell)
                            elif order.status == 'canceled':
                                mycursor.execute("UPDATE venta_subida_trnx SET dt_exec = %s, status = -1 WHERE ex_id = %s", (now, ex_id_sell) )
                                mydb.commit()
                    except Exception as e:
                        print('[12] msg original de error: ', )
                        print('[' + type(e).__name__ + ']', str(e))
                        print('error al consultar los datos de la venta para verificar que se llevo a cabo, con id: ', ex_id_sell)

                        error_type = type(e).__name__
                        if error_type == 'DDoSProtection':
                            secs_sleep_extra = random.randint(100, 200)
                            print("Durmiendo extra:", secs_sleep_extra)
                            time.sleep(secs_sleep_extra)

        except Exception as e:
            print('msg original de error: ', )
            print('[' + type(e).__name__ + ']', str(e))
            print('No hay ventas a cancelar')

except Exception as e:
    print('[1] msg original de error: ', )
    print('[' + type(e).__name__ + ']', str(e))
    print('error al conectarse a la BD')
    time.sleep(180)

    error_type = type(e).__name__
    if error_type == 'DDoSProtection':
        secs_sleep_extra = random.randint(100, 200)
        print("Durmiendo extra:", secs_sleep_extra)
        time.sleep(secs_sleep_extra)

try:
    gc.collect()
    mycursor.close()
    mydb.close()
    print('Cerrando conexiones')
except Exception as e:
    print('[13] msg original de error: ', )
    print('[' + type(e).__name__ + ']', str(e))
    print('error cerrando las conexiones')
    time.sleep(20)
