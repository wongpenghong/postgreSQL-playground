import psycopg2
from google.cloud import storage
import queryFC as qfc
import pandas as pd
import json
from datetime import datetime
import sqlalchemy
import sys
import ndjson
import os
from google.cloud import bigquery

with open("/root/etl_prod_notification/config_push_notification.json", "r") as read_file:
    CONFIG = json.load(read_file)['config_push_notification']

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CONFIG['service_account']

class postgres_crawling:
    def __init__(self):
        self.database = CONFIG['database_postgres']
        self.user = CONFIG['user_postgres']
        self.password = CONFIG['password_postgres']
        self.host = CONFIG['host_postgres']
        self.port = CONFIG['port_postgres']
        self.project = CONFIG['project']
        self.table_history = CONFIG['table_history']
        self.dataset = CONFIG['dataset']
        self.datenow = sys.argv[1]
        self.datenow_nodash = sys.argv[2]
        self.file_path_history = CONFIG['path_push_notification'] + 'data/'+ self.table_history +'_'+self.datenow_nodash+'.json'
        self.column_history = CONFIG['columns']
        self.path_bucket = CONFIG['path_bucket']
        self.bucket = CONFIG['bucket']
        self.db = CONFIG['db']
        
        
        # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = CONFIG['service_account']
        
    def _build_connection_postgres(self):
        """
        setting up the MongoDB connection
        :return: database type object
        """
        try: 
            conn = psycopg2.connect(database =self.database,  
                                user = self.user,  
                                password = self.password,  
                                host = self.host,  
                                port = self.port) 
            cur = conn.cursor() 
            print(cur)
        except (Exception, psycopg2.DatabaseError) as error: 
            print ("Error while creating PostgreSQL table", error) 
      
  
        # returing the conn and cur 
        # objects to be used later 
        return conn, cur

    def query_history(self):
        query = "SELECT {query} ".format(query=CONFIG['query_column'])
        query += " FROM {db} ".format(db=self.db)
        query += " WHERE send_at BETWEEN '{datenow} 00:00:00' and '{datenow} 23:59:59'".format(datenow=self.datenow)
        print(query)
        return query
    
    def query_history_delete(self):
        query = "DELETE FROM {db} WHERE send_at BETWEEN '{datenow} 00:00:00' and '{datenow} 23:59:59'".format(datenow=self.datenow,db=self.db)
        
        return query
    
    def upload_blob(self,bucket_name, source_file_name, destination_blob_name):
        """Uploads a file to the bucket."""
        
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_name)

        print('File {} uploaded to {}.'.format(
            source_file_name,
            destination_blob_name))
        
        return True
    
    def list_history_to_df(self,data):
        df = pd.DataFrame(data, columns = self.column_history)
        
        return df
        
    def transform_data_history(self,data):
        for i in self.column_history :
            if i == 'send_at':
                data[i] = pd.to_datetime(data[i], format='%Y-%m-%d %H-%M-%S')
                data[i] = data[i].dt.strftime('%Y-%m-%d %H:%M:%S')
                data[i].loc[data[i] == 'NaT'] = None
        return data
    
    def fetch_data_history(self,verbose=1): 
    
        conn, cur = self._build_connection_postgres()
        
        if verbose:
            print('executing query....')
            start = datetime.now()
        # select all the rows from emp 
        try: 
            query = self.query_history()
            data = cur.execute(query) 
            # iterating over all the  
            # rows in the table 
        
        except: 
            print('error !') 
    
        # store the result in data 
        data = cur.fetchall()
        # print(datas)   
        df = self.list_history_to_df(data)
        last_df = self.transform_data_history(df)
        # print(last_df)
        if verbose:
            print('query executed in ' + str(datetime.now() - start))
        
        return last_df
    
    def delete_history_data(self):
        conn, cur = self._build_connection_postgres()
        
        # select all the rows from emp 
        try: 
            query = self.query_history_delete()
            cur.execute(query) 
            # iterating over all the  
            # rows in the table 

            # Commit the changes to the database
            conn.commit()
            # Close communication with the PostgreSQL database
            cur.close()
            
        except: 
            print('error !') 
        
        return 'SUCCESS'
    
    def df_to_json_history(self):
        df = self.fetch_data_history()
        if(df.empty == False):
            df['data'] = df['data'].astype(str)
            
            datas = df.to_json(orient='records')
            json_data = json.loads(datas)
            with open(self.file_path_history, 'w') as f:
                ndjson.dump(json_data, f)
                print('----->>>>> success Dump to NDJSON')
            return True
        else:
            print('------->>>>> No data')
            return False
    
    def load_history_to_gcs(self):
        if(os.path.exists(self.file_path_history)):
            bucket_name = self.bucket
            
            destination_blob_name = self.path_bucket+self.table_history +'_'+self.datenow_nodash+'.json'
            #Load JSON to GCS
            try:
                self.upload_blob(bucket_name,self.file_path_history,destination_blob_name)
                print('----------->>>>> success load to GCS!')
            except:
                print('------->>>> failed to Load to GCS')
        else:
            print('No Data !!!')
    
    def load_data_to_bq_history(self):

        """
        loads or initalizes the job in BQ from firebase child
        :param df1: dataframe returned from the get_childs_from_db method
        :return: load job success notification
        """
        # Extract and Transform from Local
        if(self.df_to_json_history() == True):
            
            #Dump to GCS
            self.load_history_to_gcs()
            
            bigquery_client = bigquery.Client(self.project)
            # print(bigquery_client)
            destination_dataset_ref = bigquery_client.dataset(self.dataset)
            destination_table_ref = destination_dataset_ref.table(self.table_history + '$' + self.datenow_nodash)
            job_config = bigquery.LoadJobConfig()
            job_config.create_disposition = bigquery.CreateDisposition.CREATE_IF_NEEDED
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            job_config.time_partitioning = bigquery.TimePartitioning(type_=bigquery.TimePartitioningType.DAY)
            with open(self.file_path_history, 'rb') as f:
                job = bigquery_client.load_table_from_file(f, destination_table_ref, job_config=job_config)
                job.result()
                print('----->>>> '+self.file_path_history+' has success to load!')
                os.remove(self.file_path_history)
                print('------->>>> success delete File')
                #Delete from notification.history
                self.delete_history_data()
                print('success delete data at {date}'.format(date=self.datenow))
        else:
            print('-------- No data!!')