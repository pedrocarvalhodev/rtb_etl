# coding: utf-8

# website : https://panel.rtbhouse.com/
# python3 ./rtb_etl.py -s "/home/pgarcia/Downloads/"
import os
import pandas
import argparse
from boto3.session import Session
from io import StringIO
import pandas_redshift as pr
import datetime
from configparser import ConfigParser

PATH = '/home/pgarcia/repos/prod/rtb_etl'

config = ConfigParser()
config.read( PATH + '/.config.ini' )

s3_credentials_AWS_ACCESS_KEY=config['AWS_S3']['AWS_ACCESS_KEY']
s3_credentials_AWS_SECRET_KEY=config['AWS_S3']['AWS_SECRET_KEY']
s3_credentials_BUCKET=config['AWS_S3']['BUCKET']

redshift_credentials_dbname=config['AWS_REDSHIFT']['DBNAME']
redshift_credentials_host=config['AWS_REDSHIFT']['HOST']
redshift_credentials_port=config['AWS_REDSHIFT']['PORT']
redshift_credentials_user=config['AWS_REDSHIFT']['USER']
redshift_credentials_password=config['AWS_REDSHIFT']['PASSWORD']


parser = argparse.ArgumentParser()
parser.add_argument("-s","--startpath",type=str, nargs='*')
args = parser.parse_args()

ADSOURCE        = "rtb-ad-cost"
BUCKET_FOLDER   = "performance-marketing/import-spend-tracking-data/rtb/prod"
BUCKET_BKP      = "performance-marketing/import-spend-tracking-data/rtb/bkp"

SCHEMA          = "manual_data_sources"
CAMPAIGN        = "lowerfunnel"
SOURCE          = {"desktop":"br_amaro", "app":"br_amaro_inapp"}
REQUIRED_COLS   = ['Date (UTC)','Clicks', 'Imps', 'Cost (BRL)']


class process:
    """Ad Cost ETL process : Attributes:
    startpath : start path of data
    app_path  : app_path
    web_path      : web_path
    """
    
    def __init__(self,startpath):
        self.startpath  = startpath


    def get_start_date(self, file):
        file = file.replace("Retargeting_BR_Amaro_","")
        file = file.replace(".xlsx","")
        file = file.replace("InApp_","")
        file = file.split("_")
        file = file[0]
        return file


    def get_source_files(self, startpath):
      """ Get the most recent file from RTB in Downloads folder """
      check = ["Retargeting_BR_Amaro_", ".xlsx"]
      s  = [x for x in os.listdir(startpath) if all(elem in x for elem in check)]
      print("S_files", s)
      
      filter_web = [f for f in s if "InApp" not in f]
      web_path   = [x for x in filter_web if max(map(self.get_start_date, filter_web)) in x][0]
      
      filter_app = [f for f in s if "InApp" in f]
      app_path   = [x for x in filter_app if max(map(self.get_start_date, filter_app)) in x][0]
      return app_path, web_path


    def read_df(self, source_path):
        path = self.startpath + source_path
        df   = pandas.read_excel(io=path, sheet_name=0)
        return df


    def check_vars(self, app_path, web_path):
        app_cols = self.read_df(app_path).columns
        web_cols = self.read_df(web_path).columns
        a = set(REQUIRED_COLS).issubset(list(app_cols))
        w = set(REQUIRED_COLS).issubset(list(web_cols))
        return (a and w)


    def transform_df(self, app_path, web_path):
        """ Read and transfrom spreadsheet """
        df = self.read_df(source_path=web_path)
        df = df[~df["Date (UTC)"].isnull()].copy()
        df = df[REQUIRED_COLS].copy()
        df["campaign"] = CAMPAIGN
        df["source"] = SOURCE["desktop"]
        df["updated_ts"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        tcols = ["date","adclicks","impressions","adcost","campaign","source","updated_ts"]
        tcols_order = ["date","campaign","source","adclicks","impressions","adcost","updated_ts"]
        
        df.columns = tcols
        df = df[tcols_order].copy()

        df_inapp = self.read_df(source_path=app_path)
        df_inapp = df_inapp[~df_inapp["Date (UTC)"].isnull()].copy()
        df_inapp = df_inapp[REQUIRED_COLS].copy()

        df_inapp["campaign"] = CAMPAIGN
        df_inapp["source"] = SOURCE["app"]
        df_inapp["updated_ts"] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        df_inapp.columns = tcols
        df_inapp = df_inapp[tcols_order].copy()

        df = pandas.concat([df,df_inapp], axis=0, ignore_index=True)
        return df


    def connect_aws(self):
        """ Connect to AWS using pandas_redshift """
        pr.connect_to_s3(aws_access_key_id = s3_credentials_AWS_ACCESS_KEY,
                         aws_secret_access_key = s3_credentials_AWS_SECRET_KEY,
                         bucket = s3_credentials_BUCKET,
                         subdirectory = BUCKET_FOLDER)

        pr.connect_to_redshift(dbname    = redshift_credentials_dbname,
                                host     = redshift_credentials_host,
                                port     = redshift_credentials_port,
                                user     = redshift_credentials_user,
                                password = redshift_credentials_password)

    
    def delete_from_date(self, app_path):
        """ Delete data from table """
        sql_statm = "DELETE FROM manual_data_sources.rtb_ad_cost WHERE date >= '{datef}'".format(datef=self.get_start_date(app_path))
        print("PRINT SQL STATEMENT: ",sql_statm)
        pr.exec_commit(sql_statm)
        return None

    
    def get_s3_full_file_name(self, web_path):
        DATE = str(self.get_start_date(web_path))
        s3_full_file_name = BUCKET_FOLDER+"/"+SCHEMA+"."+ADSOURCE+"-"+DATE+".csv"
        return s3_full_file_name


    def upload_to_s3(self, df, file_name):
        """ Upload to S3 using Boto3 """
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        session = Session(aws_access_key_id=s3_credentials_AWS_ACCESS_KEY, 
                          aws_secret_access_key=s3_credentials_AWS_SECRET_KEY)

        s3 = session.resource('s3')
        bucket = s3.Bucket(s3_credentials_BUCKET)
        s3.Object(s3_credentials_BUCKET, file_name).put(Body=csv_buffer.getvalue())
        return None


    def upload_to_redshift(self, file_name):
        """ Upload from S3 to Redshift """
        REDSHIFT_TABLE_NAME = SCHEMA+"."+ADSOURCE.replace("-","_")

        pr.exec_commit("""
        CREATE TABLE IF NOT EXISTS
            {fn}
                 (id          INT IDENTITY(1,1),
                  date        DATE NOT NULL,
                  campaign    VARCHAR(256) NOT NULL,
                  adclicks    VARCHAR(256) NULL,
                  impressions VARCHAR(256) NULL,
                  adcost      FLOAT NULL,
                  updated_ts  TIMESTAMP NOT NULL
            );""".format(fn=REDSHIFT_TABLE_NAME))

        pr.exec_commit("""
          COPY {fn}
          FROM 's3://amaro-bi/{filepath}'
          ACCEPTINVCHARS 
          delimiter ','
          ignoreheader 1
          csv quote as '"'
          dateformat 'auto'
          timeformat 'auto'
          region 'sa-east-1'
          access_key_id '{acess_key}'
          secret_access_key '{secret_key}';
          """.format(fn=REDSHIFT_TABLE_NAME,
                     filepath=file_name,
                     acess_key=s3_credentials_AWS_ACCESS_KEY,
                     secret_key=s3_credentials_AWS_SECRET_KEY))

        print('Finished processing')

    
    def end_aws_connection(self):
        try:
          pr.close_up_shop()
          print("Connection closed")
        except Exception as e:
          print(e)
        return None


if __name__ == '__main__':
  print("-"*60)
  print("Log Timestamp : ",datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
  c = process(args.startpath[0])#, args.app_path[0], args.web_path[0])
  app_path, web_path = c.get_source_files(args.startpath[0])
  print(app_path, " | ",web_path)
  if (c.get_start_date(app_path) == c.get_start_date(web_path)) and c.check_vars(app_path, web_path):
    print("Ok. Proceding")
    print("1. connect_aws")
    c.connect_aws()
    print("2. upload_to_s3")
    c.upload_to_s3(c.read_df(app_path), BUCKET_BKP+"/"+app_path)
    c.upload_to_s3(c.read_df(web_path), BUCKET_BKP+"/"+web_path)
    c.upload_to_s3(c.transform_df(app_path, web_path), c.get_s3_full_file_name(web_path))
    print("3. delete_from_date")
    c.delete_from_date(app_path)
    print("4. upload_to_redshift")
    c.upload_to_redshift(c.get_s3_full_file_name(web_path))
    print("5. end_aws_connection")
    c.end_aws_connection()
  else:
    print("Error: Both files must have same start date")