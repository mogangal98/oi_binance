import mysql.connector as mysql
import pandas as pd
import sys
import datetime as dt
from config import Config

class DatabaseHandler():    
    def __init__(self, logger):
        self.coin_list_table = "COINS"     # Name of the coin list table
        self.DB_IP = "1.1.1.1"       # Database ip 
        self.DB_USER = "username"
        self.DB_DATABASE = "database"
        self.DB_PASS = "password"
        self.logger = logger
        
        self.db = mysql.connect(host = self.DB_IP, database = self.DB_DATABASE, user = self.DB_USER, password = self.DB_PASS, auth_plugin = "mysql_native_password")  
    
    # Get the coin list from database
    def coin_list_database(self):
        try:                                    
            sql_str = f"SELECT id,parite,oi_pool_id FROM {self.coin_list_table}"
            db_cursor = self.db.cursor()
            db_cursor.execute(sql_str)
            db_coins =  pd.DataFrame(db_cursor.fetchall(),columns = ["id","parite","oi_pool_id"])[:]
            return db_coins
        except Exception as e:
            e = sys.exc_info()[0:2]
            self.logger.warning("(coin_list_database) -->-- " + str(e))
        
    # Used to get tables with a specific prefix
    def get_tables(self, prefix: str = "oi"):
        sql_str = ("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE '" + prefix + "%'")
        sql_str += ";"
        self.db_cursor = self.db_connection.cursor()
        self.db_cursor.execute(sql_str)
        sonuc = self.db_cursor.fetchall()
        results = pd.DataFrame(sonuc)
        self.db_connection.commit()
        self.db_cursor.close()
        return results

    # Will check if ther is a new coin, or a delisted coin
    def check_coins(self): 
        try: 
            db_coinler = [x.replace('R1m_','').upper() for x in self.get_tables(prefix="oi")[0].tolist()]
        except Exception as e:
            db_coinler = []
            e = sys.exc_info()[0:2]
            self.logger.warning("'oi' coin listesi db'den cekilemedi (databasede coin yok): " + str(e))
        
        try:
            eklenen_futures_db = self.coin_list_database()
            guncel_coinler = eklenen_futures_db["parite"].tolist()                  # contains the coins of the coin_list in the database.
            silinen_futures = list(set(db_coinler) - set(guncel_coinler))   # Deleted coins from DB
            eklenen_futures = list(set(guncel_coinler) - set(db_coinler))   # Coins that are added to DB
            return silinen_futures,eklenen_futures    
        except Exception as e:        
            e = sys.exc_info()[0:2]
            self.logger.warning("check_coins - eklenen_futures_db error: " + str(e))
  
    # Turns lists sql query appropriate form
    def list_to_sql(self, parametreler):
        sql_str = "("
        for i in parametreler:
            sql_str += i + ", "
        sql_str = sql_str[:-2]
        sql_str += ")" 
        return sql_str
    
    # Creates table in db
    def create_table(self, table_name: str, columns, second_index = ["none","none"]): 
        # Columns will be given as List, with its parameters
        self.db_cursor = self.db_connection.cursor()
        sql_str = "CREATE TABLE IF NOT EXISTS " + table_name
        sql_str += self.list_to_sql(columns)
        if second_index[0] != "none":
            sql_str = sql_str[:-1]
            sql_str += ", INDEX " + second_index[0] + " (" + second_index[1] + "))"
        sql_str += ";"        
        self.db_cursor.execute(sql_str)  
        self.db_connection.commit()
        self.db_cursor.close()

    #   Creates coin tables with prefixes
    def create_coin_tables(self,coin_name: str,interval: str):
        # bu 1dklık verilerin sutunları
        create_oi_columns = ["timestamp INT UNSIGNED NOT NULL PRIMARY KEY","datetime DATETIME NOT NULL","funding_rate DOUBLE","funding_rate_mean DOUBLE ", 
                         "mark_price_mean FLOAT UNSIGNED","index_price_mean FLOAT UNSIGNED","oi_transaction_timestamp INT UNSIGNED", 
                         "oi_transaction_datetime DATETIME", "open_interest DOUBLE UNSIGNED"]
        try:
            table_name = "oi"
            self.create_table(table_name,create_oi_columns)
        except Exception as e: 
            e = sys.exc_info()[0:2]
            self.logger.warning("create_coin_tables err: " + str(e))
        
    def db_yeni_coin_ekle(self, coin_listesi): 
        for coin in coin_listesi:
            self.create_coin_tables(coin,"1m")
            self.create_coin_tables(coin,"5m")
            self.create_coin_tables(coin,"1h")
            self.create_coin_tables(coin,"1d")

    # Inserts a row, or multipel rows, to database
    def insert_row(self, interval: str,table_name: str,columns,values,multiple_rows = False):
        try:
            db_cursor = self.db.cursor()
            if multiple_rows: # changes the values from list to string
                values_str = "(" 
                for i in range(len(values)): # values should be a dataframe
                    values_str += str(values.iloc[i,0]) + ',"'+str(values.iloc[i,1]) +'"),('
                values_str = values_str[:-2]
            
                #ornek: INSERT IGNORE INTO R1m_1000SHIBUSDT(timestamp, datetime, fundingRateMean, markPriceMean, IndexPriceMean, fundingRate)VALUES (1677844860, "2023-03-07 14:08:00", 0.0001, 0.011000293684210526, 0.011012616842105263, 0.00010000);
                           
                # Bu bos timestampler doldurulacagi zaman calisiyor sadece
                sql_str = "INSERT IGNORE INTO " + table_name + self.list_to_sql(columns) +"VALUES " +values_str+";"
                
            else: 
                sql_str = "INSERT INTO " + table_name + self.list_to_sql(columns) +"VALUES " +self.list_to_sql(values)
                sql_str += "ON DUPLICATE KEY UPDATE "   # Duplicate key, daha once bu keye bi deger yazilmissa oluyor
                for i in range(len(columns)):           # Eger oi degeri null ise yeni degerler yazilcak
                    sql_str += str(columns[i]) + " = IF(" + str(columns[i]) + " IS NULL, " + str(values[i]) + ", " + str(columns[i]) + "),"
                sql_str = sql_str[:-1]
                sql_str += ";"
                
            db_cursor.execute(sql_str)
            self.db.commit()
            db_cursor.close()
            
        except Exception as e:
            e = sys.exc_info()[0:2]
            self.logger.warning("insert_row err: " + str(e))
      
    def insert_dataframe(self, table_name: str, df, upsert = False): # INSERT OR UPSERT
        try:
            # Dataframe columns should e same as database columns
            db_cursor = self.db.cursor()
            sql_str = "INSERT INTO " + table_name + " " + self.list_to_sql(df.columns) + " VALUES "
            for i in range(len(df)):
                sql_str += str(tuple(df.iloc[i,:].values.astype(str)))
                if i != len(df)-1: sql_str += ", "
            sql_str += " ON DUPLICATE KEY UPDATE "   # Duplicate key
            for i in range(len(df.columns)):           # If oi value is null, new data will be written
                if str(df.columns[i]) == "oi_transaction_datetime":sql_str += str(df.columns[i]) + " = IF(" + str(df.columns[i]) + " IS NULL, '" + str(df.iloc[0,i]) + "', " + str(df.columns[i]) + "),"
                elif str(df.columns[i]) == "timestamp" or str(df.columns[i]) == "datetime": continue
                else:sql_str += str(df.columns[i]) + " = IF(" + str(df.columns[i]) + " IS NULL, " + str(df.iloc[0,i]) + ", " + str(df.columns[i]) + "),"
            sql_str = sql_str[:-1]
            sql_str += ";"
            db_cursor.execute(sql_str)
            self.db.commit()
            db_cursor.close()
            return db_cursor.rowcount   
        except Exception as e:
            print("'insert_dataframe' hata: ",e)
            e = sys.exc_info()[0:2]
            self.logger.warning("insert_dataframe hata " + str(e))
            return 0  
           
    # can work with any kind of sql query        
    def genel_sql(self, sql_str: str) -> int:
        db_cursor = self.db.cursor()
        db_cursor.execute(sql_str)
        self.db.commit()
        db_cursor.close()
        return db_cursor.rowcount    
    
    # deletes rows from database
    def delete_rows(self,futures_coinler):
        for coin in futures_coinler['parite']:
            try:
                table_name = "oi_"+str(coin.upper())
                
                baslangic_zamani = dt.datetime.timestamp(dt.datetime.now() - dt.timedelta(hours = 24))
                
                sqlstr = "SELECT timestamp FROM R1m_"+coin+" WHERE timestamp >= "+ str(baslangic_zamani)+";"
                db_cursor = self.db.cursor(buffered = True)
                db_cursor.execute(sqlstr)
                sonuc = pd.DataFrame(db_cursor.fetchall())
                db_cursor.close()
                
                sikintili_tsler = sonuc.loc[sonuc.iloc[:,0] %60 != 0]
                sikintili_tsler = sikintili_tsler.iloc[:,0].tolist()
                
                if len(sikintili_tsler) > 0:
                    print(table_name,"   deleting:   ",len(sikintili_tsler))
                    sikintili_tsler = ", ".join(f"'{item}'" for item in sikintili_tsler)
                    sql = f"DELETE FROM {table_name} WHERE `timestamp` IN "
                    sql += "("+sikintili_tsler
                    sql +=");"
                    
                    self.genel_sql(sql)
                
                # DELETE FROM `whalewatcher`.`R1m_GRTUSDT` WHERE (`timestamp` = '1712049548');
            except Exception as e:
                e = sys.exc_info()[0:2]
                self.logger.warning("delete_rows error:   - --- - - - -- - " + str(e))

    # Function to find the index of the row with the closest 'time' to a minute 
    # The reason for this is, we only need the data at the beginning of a minute . Like 11:11:00 or 10:20:00.
    # And the open interest data gets updated every 10 seconds or so. Therefore we only get data between these seconds to
    # not the exceed the api limit and get the data thats closest to this minute mark
    def closest_to_minute_mark(self, df,anlik_ts):
        # Finds the difference from the nearest minute mark (60 seconds)
        filtered_df = df[df['time'] == anlik_ts]
        if not filtered_df.empty: # Proceed only if the filtered DataFrame is not empty
           # Find the difference from the nearest minute mark (60 seconds) in 'transaction_time'
           minute_diff = filtered_df['transaction_time'].astype(int) % 60
           minute_diff = minute_diff.apply(lambda x: min(x, 60 - x)) # 60 in katlarina en yakin olan seyi bulacqak
           # Find the row with the minimum difference
           return filtered_df.loc[minute_diff.idxmin()]
        else:
            minute_diff = df['transaction_time'].astype(int) % 60
            # Find the row with the minimum difference
            return df.loc[minute_diff.idxmin()]           