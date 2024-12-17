import pandas as pd
import datetime as dt
import time
import mysql.connector as mysql
import requests as rq
from concurrent.futures import ThreadPoolExecutor
import sys
import os

from logger_setup import LoggerSetup
from config import Config
from database_handler import DatabaseHandler

        
global oi_veri,oi_veri_depo        
oi_veri = []  # The data will be collected in this list. Then we will convert it to dataframe and write it to db
oi_veri_depo = [] # Those that couldnt be written to db will be stored here temporarily

# This function gets the open interest data from binance for the given pair
# We will give pairs in batches, each batch having a few pairs. Each batch sends a request on a seperate thread to get all the data fast.
# All the data we get in these seperate threads will be written to "oi_veri" list, before we write all this data to our database.
def get_open_interest_data(coin_df):
    global error_429,api_sayac
    Logger = LoggerSetup(os.path.expanduser('~')+ "/open_interest.log")
    logger = Logger.get_logger()    
    
    for i in range(len(coin_df["parite"])):
        try:
            session = rq.Session()
            veri = session.get(Config.BASE_URL + "/fapi/v1/openInterest", params = {"symbol": str(coin_df.iloc[i]["parite"]).upper()})
            status = veri.status_code
            api_sayac +=1
            if status != 200:
                logger.warning("STATUS: " + str(status)+ " PAIR: "+ str(coin_df.iloc[i]["parite"].upper()) + "  ||  " + str(dt.datetime.now().strftime("%H:%M %d.%m.%Y")))
            if status == 429:
                error_429 = True
                return
            
            veri = veri.json()
            veri["transaction_time"] = veri["time"] // 1000     #Since Binance has a 13-digit timestamp, we divide by 1000 and round up to save in our database
            
            temp_time = dt.datetime.fromtimestamp(veri["time"] // 1000) 
            gerekli = False
            if temp_time.second >= Config.OI_LOWER_LIMIT:
                temp_time = temp_time + dt.timedelta(seconds = (60-temp_time.second))                    # After the second 39 it is rounded to the next minute
                gerekli = True
                veri["time"] = dt.datetime.timestamp(temp_time)
            elif temp_time.second < Config.OI_UPPER_LIMIT: 
                temp_time = temp_time + dt.timedelta(seconds = -temp_time.second)                        # Before 12 seconds it is rounded to 0
                gerekli = True
                veri["time"] = dt.datetime.timestamp(temp_time)
            
            if gerekli:
                oi_veri.append(veri)  
        except Exception as e:
            print(e)
            e = sys.exc_info()[0:2]
        
# Each pair has a pre determined "pool_id" assigned to them. 
# We will create a new thread for each of these pool ids. There are like 20 pools in total, each having only a few pairs
# This function manages pool ids and starts the threads
def multi_thread(coin_df, executor, logger):      
    try:
        coin_df_list = []
        
        min_pool_id = coin_df['oi_pool_id'].min()
        max_pool_id = coin_df['oi_pool_id'].max()
        
        coin_df = coin_df.groupby(['oi_pool_id'])
        for i in range(min_pool_id,max_pool_id+1):
            coin_df_list.append(coin_df.get_group(i))
        
        results = executor.map(get_open_interest_data, coin_df_list)
    except Exception as e:
        print(e)
        e = sys.exc_info()[0:2]
        logger.warning("multi_thread error: " + str(e))

# Main func.
def main():    
    # Set logger
    Logger = LoggerSetup(os.path.expanduser('~')+ "/open_interest.log")
    logger = Logger.get_logger()
    DbHandler = DatabaseHandler(logger = logger)
    
    # Get the coin list
    futures_coinler = DbHandler.coin_list_database()
    pool_size = int(futures_coinler['oi_pool_id'].max()) + 1
    session_list = [rq.Session() for i in range(pool_size)]
    executor = ThreadPoolExecutor(max_workers = pool_size,thread_name_prefix='pool')
    

    # temp variables    
    veri_cek = False
    database_yaz = False
    global oi_veri,oi_veri_depo,api_sayac
    api_sayac = 0
    ts_sayac = 0 # 0-> 1m   1-> 5m    2-> 1h   3-> 1d
    bos_veri_cekildi_sayac = 0
    
    # Main loop
    while True:  
        try:
            right_now = dt.datetime.timestamp(dt.datetime.now())
            if right_now%60 >= 24 and right_now%60 <= 28:
                # Update the coin list
                try:                            
                    if ts_sayac == 3: # We will check this every 3 loops
                        futures_coinler_yeni = DbHandler.coin_list_database()
                        
                        columns_df1 = set(futures_coinler['parite'])
                        columns_df2 = set(futures_coinler_yeni['parite'])                    
    
                        only_in_df1 = columns_df1 - columns_df2
                        only_in_df2 = columns_df2 - columns_df1 # New coins will be here
                        
                        if len(only_in_df2) > 0 or len(only_in_df1) > 0: # sadece yeni gelen coin varsa tablo olusturacaz
                            print("Yeni gelen coinler var")
                            logger.warning("Yeni gelen coinler var:" + str(dt.datetime.now()))
                            futures_coinler = futures_coinler_yeni
                            DbHandler.db_yeni_coin_ekle(futures_coinler["parite"].tolist()) 
                        
                except Exception as e: 
                    print("Coin list update error")
                    e = sys.exc_info()[0:2]
                    logger.warning("Coin list update error: " + str(e))
                        
            # The code will wait between seconds 12-39
            # The reason for this is, we only need the data at the beginning of a minute . Like 11:11:00 or 10:20:00.
            # And the open interest data gets updated every 10 seconds or so. Therefore we only get data between these seconds to
            # not the exceed the api limit and get the data thats closest to this minute mark
            elif (right_now%60 > Config.OI_UPPER_LIMIT and right_now%60 < Config.OI_LOWER_LIMIT):pass 
            
            else:            
                if api_sayac <= Config.API_LIMITER and (right_now%60 < Config.OI_UPPER_LIMIT-Config.DB_WRITE_THRESHOLD or right_now%60 > Config.OI_LOWER_LIMIT):
                    print("Getting Data: ",dt.datetime.now().strftime("%H:%M:%S"))
                    veri_cek = True
                    database_yaz = False
                    
                    session_list = [rq.Session() for i in range(pool_size)]
                    executor = ThreadPoolExecutor(max_workers = pool_size,thread_name_prefix='pool')
                
                elif right_now%60 > Config.OI_UPPER_LIMIT-Config.DB_WRITE_THRESHOLD and len(oi_veri) != 0:
                    print("Got the Data writing to db: ",dt.datetime.now().strftime("%H:%M:%S"))
                    veri_cek = False
                    database_yaz = True
                
                
                ####  Veri cek
                if veri_cek:                
                        multi_thread(futures_coinler,executor, logger)
                        time.sleep(5) # Since the data is updated every 3 seconds (supposedly, actually its like 10 seconds for most pairs), 
                        # We will send it every 5 seconds instead of sending it continuously. There is no point in sending it more than that.
              
                if database_yaz:
                    if len(oi_veri) <= 200 and len(oi_veri) != 0:
                        text = f"Too few datas: Pool size: {pool_size} | Session list size: {len(session_list)} Coin list size: {len(futures_coinler)}"
                        print(text)
                        logger.warning(text)
                        
                        print("Cekilen veri sayisi az: ",str(len(oi_veri)))
                        print("The number of data is low ",str(dt.datetime.now().strftime("%H:%M %d.%m.%Y")))
                        logger.warning("The number of data is low : " + str(dt.datetime.now().strftime("%H:%M %d.%m.%Y")))   
                        
                        futures_coinler = DbHandler.coin_list_database()
                        if len(futures_coinler) > 0: print("Coinlist reset")
                        
                        pool_size = int(futures_coinler['oi_pool_id'].max()) + 1
                        session_list = [rq.Session() for i in range(pool_size)]
                        executor = ThreadPoolExecutor(max_workers = pool_size,thread_name_prefix='pool')
                    
                    elif len(oi_veri) == 0:
                        bos_veri_cekildi_sayac += 1
                        if bos_veri_cekildi_sayac >= 10:
                            bos_veri_cekildi_sayac = 0
                            text = f"No Data: Pool size: {pool_size} | Session list size: {len(session_list)} Coin list size: {len(futures_coinler)}"
                            print(text)
                            logger.warning(text)
                            
                            print("Data could not be retrieved in the last 10 loops.")
                            logger.warning("Data could not be retrieved in the last 10 loops. " + str(dt.datetime.now().strftime("%H:%M %d.%m.%Y")))   
                            
                            futures_coinler = DbHandler.coin_list_database()
                            if len(futures_coinler) > 0: print("Coinlist reset")
                            
                            pool_size = int(futures_coinler['oi_pool_id'].max()) + 1
                            session_list = [rq.Session() for i in range(pool_size)]
                            
                            executor = ThreadPoolExecutor(max_workers = pool_size,thread_name_prefix='pool')                    
                            
                        
                    # Write the data to database    
                    if len(oi_veri) != 0:
                        try:        
                            bos_veri_cekildi_sayac = 0
                            print("Writing to db...")
                            oi_veri_depo.append(oi_veri)
                            oi_veri = []
                            for data in oi_veri_depo:
                                tot_zaman = dt.datetime.now()
                                
                                oi_veri_dataframe = pd.DataFrame(data)

                                anlik_ts = dt.datetime.timestamp(dt.datetime.now())
                                anlik_ts  = int(anlik_ts - anlik_ts %60)
                                
                                oi_veri_dataframe = oi_veri_dataframe.groupby('symbol').apply(DbHandler.closest_to_minute_mark,anlik_ts=anlik_ts)
                                oi_veri_dataframe = oi_veri_dataframe.reset_index(drop=True)
                                oi_veri_dataframe = oi_veri_dataframe.rename(columns={'openInterest': 'open_interest'})
                                oi_veri_dataframe = oi_veri_dataframe.rename(columns={'time': 'timestamp'})
                                oi_veri_dataframe = oi_veri_dataframe.rename(columns={'transaction_time': 'oi_transaction_timestamp'})
                                oi_veri_dataframe['datetime'] = pd.to_datetime(oi_veri_dataframe['timestamp'], unit='s') + pd.to_timedelta(dt.timedelta(hours=Config.GMT_OFFSET))
                                oi_veri_dataframe['oi_transaction_datetime'] = pd.to_datetime(oi_veri_dataframe['oi_transaction_timestamp'], unit='s') + pd.to_timedelta(dt.timedelta(hours=Config.GMT_OFFSET))
                                
                                yazilan_veri = 0
                                
                                # We have seperate tables for 1 minute, 5 minute, 1 hour and 1 day intervals
                                # Each of these tables are necessary for different algorithms we use
                                for idx in range(len(oi_veri_dataframe)):
                                    try:
                                        timestamp = oi_veri_dataframe.loc[idx,'timestamp']
                                        symbol = oi_veri_dataframe.loc[idx,'symbol']
                                        
                                        oi_veri_dataframe['open_interest'] = oi_veri_dataframe['open_interest'].astype(float)
                                        
                                        table_name = symbol.upper()
                                        written_rows = DbHandler.insert_dataframe(table_name = "oi_" + table_name,df = oi_veri_dataframe.iloc[[idx]].drop('symbol',axis=1),upsert = True)
                                        yazilan_veri += 1
                                        
                                        if timestamp%300 == 0: # 5 dk yaz
                                            table_name = symbol.upper()
                                            written_rows = DbHandler.insert_dataframe(table_name = "oi_" + table_name,df = oi_veri_dataframe.iloc[[idx]].drop('symbol',axis=1),upsert = True)
                                            yazilan_veri += 1
                                        if timestamp%3600 == 0: # 1h yaz
                                            table_name = symbol.upper()
                                            written_rows = DbHandler.insert_dataframe(table_name = "oi_" + table_name,df = oi_veri_dataframe.iloc[[idx]].drop('symbol',axis=1),upsert = True)
                                            yazilan_veri += 1
                                        if timestamp%86400 == 0: # 1d yaz
                                            table_name = symbol.upper()
                                            written_rows = DbHandler.insert_dataframe(table_name = "oi_" + table_name,df = oi_veri_dataframe.iloc[[idx]].drop('symbol',axis=1),upsert = True)
                                            yazilan_veri += 1
                                    except Exception as e: 
                                        print("Error writing to database: ",e)
                                        logger.warning("Error writing to database: " + str(e))    

                                api_sayac = 0   
                                veri_sayisi = len(oi_veri_dataframe)
                                print("OI data size = ",yazilan_veri,"         Time = ",dt.datetime.now() - tot_zaman)
                            oi_veri_depo = []
                            
                        except Exception as e: 
                            veri_cek = False
                            database_yaz = False
                            print("db write error: ",e)
                            e = sys.exc_info()[0:2]
                            logger.warning("db write error: " + str(e))      
                            
                        time.sleep(3)
        except Exception as e:
            print("Main loop error: ",e)
            e = sys.exc_info()[0:2]
            logger.warning("Main loop error: " + str(e))        
        
if __name__ == "__main__":    
    main()