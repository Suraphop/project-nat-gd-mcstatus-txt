import os
#import pypyodbc 
import pandas as pd
import sys
import utils.constant as constant
import utils.alert as alert
import pymssql
import json

from datetime import datetime,date, timedelta
from sqlalchemy import create_engine,text,engine


class PREPARE:


    def __init__(self,path,server,database,user_login,password,table,table_columns,table_log,table_columns_log,notify_token):
        self.path = path
        self.server = server
        self.database = database
        self.user_login = user_login
        self.password = password
        self.table_log = table_log
        self.table = table
        self.table_columns = table_columns
        self.table_columns_log = table_columns_log
        self.path_list = None
        self.path_now = None
        self.df = None
        self.df_insert = None
        self.notify_token = notify_token

    def stamp_time(self):
        now = datetime.now()
        print("\nHi this is job run at -- %s"%(now.strftime("%Y-%m-%d %H:%M:%S")))

    def check_floder(self):
        # Check whether the specified path exists or not
        isExist = os.path.exists(self.path)
        if not isExist:
            # Create a new directory because it does not exist
            os.makedirs(self.path)
            self.info_msg(self.check_floder.__name__,f"The {self.path} directory is created!")
        else:
            self.info_msg(self.check_floder.__name__,f"found the directory: {self.path}")

    def check_table(self):
        #connect to db
        cnxn,cursor=self.conn_sql()
        # create table
        try:
            cursor.execute('''
            CREATE TABLE '''+self.table+''' (
                '''+self.table_columns+'''
                )
                ''')
            cnxn.commit()
            cursor.close()
            self.info_msg(self.check_table.__name__,f"create a {self.table_log} table successfully" )
        except Exception as e:
            if 'There is already an object named' in str(e):
                self.info_msg(self.check_table.__name__,f"found a {self.table} table" )
            elif 'Column, parameter, or variable' in str(e):
                self.error_msg(self.check_table.__name__,"define columns mistake" ,e)
            else:
                self.error_msg(self.check_table.__name__,"unknow cannot create table" ,e)

    def check_table_log(self):
        #connect to db
        cnxn,cursor=self.conn_sql()
        # create table
        try:
            cursor.execute('''
            CREATE TABLE '''+self.table_log+''' (
                '''+self.table_columns_log+'''
                )
                ''')
            cnxn.commit()
            cursor.close()
            self.info_msg(self.check_table_log.__name__,f"create a {self.table_log} table successfully" )
        except Exception as e:
            if 'There is already an object named' in str(e):
                self.info_msg(self.check_table_log.__name__,f"found a {self.table} table" )
            elif 'Column, parameter, or variable' in str(e):
                self.error_msg(self.check_table_log.__name__,"define columns log mistake" ,e)
            else:
                self.error_msg(self.check_table_log.__name__,"unknow cannot create table log" ,e)

    def error_msg(self,process,msg,e):
        result = {"status":constant.STATUS_ERROR,"file_name":self.path_now,"process":process,"message":msg,"error":e}
      
        try:
            #self.alert_line(self.alert_error_msg_line(result))
            self.alert_slack(self.alert_error_msg_slack(result))
            self.log_to_db(result)
            sys.exit()
        except Exception as e:
            self.info_msg(self.error_msg.__name__,e)
            sys.exit()
    
    def alert_line(self,msg):
        value = alert.line_notify(self.notify_token,msg)
        value = json.loads(value)  
        if value["message"] == constant.STATUS_OK:
            self.info_msg(self.alert_line.__name__,'send msg to line notify')
        else:
            self.info_msg(self.alert_line.__name__,value)
            
    def alert_slack(self,msg):
        value = alert.slack_notify(self.notify_token,msg) 
        if value == constant.STATUS_OK:
            self.info_msg(self.alert_slack.__name__,'send msg to slack notify')
        else:
            self.info_msg(self.alert_slack.__name__,value)

    def alert_error_msg_slack(self,result):
        if self.notify_token != None:
            return f'\nproject: {self.table}\nfile_name: {self.path_now}\nprocess: {result["process"]}\nmessage: {result["message"]}\nerror: {result["error"]}\n'  

    def alert_error_msg_line(self,result):
        if self.notify_token != None:
            return f'\nproject: {self.table}\nfile_name: {self.path_now}\nprocess: {result["process"]}\nmessage: {result["message"]}\nerror: {result["error"]}\n'

    def info_msg(self,process,msg):
        result = {"status":constant.STATUS_INFO,"file_name":self.path_now,"process":process,"message":msg,"error":"-"}
        print(result)
            
    def ok_msg(self,process):
        result = {"status":constant.STATUS_OK,"file_name":"-","process":process,"message":"program running done","error":"-"}
        try:
            self.log_to_db(result)
            print(result)
        except Exception as e:
            self.error_msg(self.ok_msg.__name__,'cannot ok msg to log',e)
    
    def conn_sql(self):
        #connect to db
        try:
            cnxn = pymssql.connect(self.server, self.user_login, self.password, self.database)
            cursor = cnxn.cursor()
            return cnxn,cursor
        except Exception as e:
            self.alert_line("Danger! cannot connect sql server")
            self.info_msg(self.conn_sql.__name__,e)
            sys.exit()

    def log_to_db(self,result):
        #connect to db
        cnxn,cursor=self.conn_sql()
        try:
            cursor.execute(f"""
                INSERT INTO [{self.database}].[dbo].[{self.table_log}] 
                values(
                    getdate(), 
                    '{result["status"]}', 
                    '{result["file_name"]}',
                    '{result["process"]}', 
                    '{result["message"]}', 
                    '{str(result["error"]).replace("'",'"')}'
                    )
                    """
                )
            cnxn.commit()
            cursor.close()
        except Exception as e:
            self.alert_line("Danger! cannot insert log table")
            self.info_msg(self.log_to_db.__name__,e)
            sys.exit()

    def read_path(self):
        path_list = []
        file_extension = '.txt'
        for root,dirs,files in os.walk(self.path):
            for name in files: 
                if name.endswith(file_extension):    
                    file_path = os.path.join(root,name)
                    path_list.append(file_path)
        if len(path_list) == 0:
            self.error_msg(self.read_path.__name__,"read path function: csv file not found","check csv file")
        else: 
            self.path_list = path_list
            self.info_msg(self.read_path.__name__,f"found: {len(path_list)} file")

    def query_df(self,query):
        try:
            connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER="+self.server+";DATABASE="+self.database+";UID="+self.user_login+";PWD="+self.password+""
            connection_url = engine.URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})
            engine1 = create_engine(connection_url)
            with engine1.begin() as conn:
                query_df = pd.read_sql_query(text(query), conn)
                self.info_msg(self.query_df.__name__,f"query df success")
                return query_df
        except Exception as e:
                self.error_msg(self.query_df.__name__,"cannot select with sql code",e)
            
    def mfg_date(self,date_time):
        mfg_date = date_time - timedelta(hours=7)
        mfg_date_string = mfg_date.strftime("%Y-%m-%d")
        return mfg_date_string
    

class MCSTATUS(PREPARE):

    
    def __init__(self,path,server,database,user_login,password,table,table_columns,table_log,table_columns_log,notify_token=None):
        super().__init__(path,server,database,user_login,password,table,table_columns,table_log,table_columns_log,notify_token)

    def read_data(self):
        try:
            df = pd.read_csv(self.path_now,sep=",")
            df.dropna(inplace=True)

            df['mc_no'] = self.path_now.split("_")[-1].split(".")[0] # add filename to column

            self.df = df
            self.info_msg(self.read_data.__name__,f"csv to pd")
        except Exception as e:
            self.error_msg(self.read_data.__name__,"pd cannot read csv file",e)
    
    def query_duplicate(self):
        mc_no = self.path_now.split("/")[-1].split(".")[0]
        query =  """SELECT TOP(3000)
         CONVERT(VARCHAR, [occurred],20) AS 'occurred',
         CAST([mc_status] AS int),
         [mc_no] 
         FROM ["""+self.database+"""].[dbo].["""+self.table+"""] 
         where [mc_no] = '"""+mc_no+"""' 
         order by [registered_at] desc"""
        df = self.query_df(query)
        df['occurred'] = pd.to_datetime(df.occurred)
        return df
        
    def check_duplicate(self):
        try:
            df_from_db = self.query_duplicate()
            df = self.df
            df['occurred'] = pd.to_datetime(df.occurred)
            df_right_only = pd.merge(df_from_db,df , on = ["occurred","mc_no"], how = "right", indicator = True) 
            df_right_only = df_right_only[df_right_only['_merge'] == 'right_only'].drop(columns=['_merge'])
            if df_right_only.empty:              
                self.info_msg(self.check_duplicate.__name__,f"data is not new for update")
            else:
                self.info_msg(self.check_duplicate.__name__,f"we have data new")
                self.df_insert = df_right_only       
                return constant.STATUS_OK    
        except Exception as e:
            self.error_msg(self.check_duplicate.__name__,"cannot select with sql code",e)
    
    def df_to_db(self):
        #connect to db
        cnxn,cursor=self.conn_sql()
        try:
            df = self.df_insert
            for index, row in df.iterrows():
                cursor.execute(f"""
                INSERT INTO [{self.database}].[dbo].[{self.table}] 
                values(
                    getdate(), 
                    '{row.occurred}', 
                    '{row.mc_status}', 
                    '{row.mc_no}')
                    """
                )
            cnxn.commit()
            cursor.close()
            self.df_insert = None   
            self.info_msg(self.df_to_db.__name__,f"insert data successfully")
            
        except Exception as e:
            self.error_msg(self.df_to_db.__name__,"cannot insert MCSTATUS to sql",e)

    def run(self):
        self.stamp_time()
        self.check_floder()
        self.check_table()
        self.check_table_log()
        self.read_path()
        for i in range(len(self.path_list)):
            self.path_now = self.path_list[i]
            self.read_data()
            if self.check_duplicate() == constant.STATUS_OK:
                  self.df_to_db()    
        self.ok_msg(self.df_to_db.__name__)

if __name__ == "__main__":
    print("must be run with main")