import pandas as pd
import datetime
from utils.databse_conn import sql_server_db_conn
from utils.get_config_file import get_gmail_config
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
logging.basicConfig(filename='logs\data_transformation.log',level=logging.INFO, filemode='w',
                    format='%(levelname)s:%(asctime)s:%(name)s:%(message)s',
                    datefmt='%d:%b:%y %H:%M:%S')

def read_dataset():
    try:
        df = pd.read_csv('orders.csv')
        logging.info('dataset read successfully')
        return df
    except Exception as e:
        logging.error(f"failed to read data {e}")

def transformation(df):
    try:
        # handle NAN values
        df = pd.read_csv('orders.csv',na_values=['Not Available', 'unknown'])
        logging.info("ship Mode column value ['Not Available', 'unknown'] replace with NAN")

        # column in lower case
        df.columns = df.columns.str.lower()
        logging.info('making column name in lower case')

        # replace space in columns with '_'
        df.columns = df.columns.str.replace(' ','_')
        logging.info('replace space in col name with _')

        #derive new columns
        df['discount'] = df['list_price']*df['discount_percent']*.01
        logging.info('new discount column is derived')

        df['sale_price'] = df['list_price']-df['discount']
        logging.info('new sale_price column is derived')

        df['profit'] = df['sale_price']-df['cost_price']
        logging.info('new profit column is derived')

        # convert data type of order_date column to datetime
        df['order_date']= pd.to_datetime(df['order_date'],format='%Y-%m-%d')
        logging.info('changing data type of order_date column from object to datetime')
        
        # drop cost price list_price and discount_percent column
        df.drop(columns=['list_price','cost_price','discount_percent'],inplace=True)
        logging.info("dropping ['list_price','cost_price','discount_percent'] columns")
        
        # add lastupdated column
        df['last_updated']= datetime.datetime.now()
        logging.info('adding new column last_updated')
        return df

    except Exception as e:
        logging.error(f"transformation error:{e}")

def load_data_to_SQL_server(df, sql_conn,table_name):
    try: 
        df.to_sql(table_name, con=sql_conn,  if_exists='replace', index=False,)
        logging.info("Data loaded into Sql server successfully using df.to_sql method")
        return True
    except Exception as e:
        logging.error(f"Failed to load data into SQL server: {e}")
        return False


def send_email(status):
    gmail_cred = get_gmail_config()
    logging.info('getting gmail credential from config')
    sender_email = "jaymalakhaire9766@gmail.com"
    receiver_email = "jayukhaire2000@gmail.com"
    password = f"{gmail_cred['apppass']}"

    subject = "Data load Status"

    sql_server_db = "sale.dbo.orders"

    # Email content
    if status:
        body = f"Hi team,\n\n data  is successfully loaded to {sql_server_db} in SQL Server DB.\n\nBest regards,\nNitin"
        logging.info("format data migration success email body")
    else:
        body = f"Hi team,\n\nThe data loading to SQL Server DB {sql_server_db} failed. Please check the logs.\n\nBest regards,\nNitin"
        logging.info('format data migration failure email body')


    # Create the email
    msg = MIMEMultipart()
    msg['From'] = sender_email
    msg['To'] = receiver_email
    msg['Subject'] = subject

    msg.attach(MIMEText(body, 'plain'))

    try:
        # Connect to SMTP server and send email
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.sendmail(sender_email, receiver_email, msg.as_string())
            logging.info("Email sent successfully.")
    except Exception as e:
        logging.info(f"Failed to send email: {e}")

def main():
    df = read_dataset()
    transformed_df = transformation(df)
    sql_conn = sql_server_db_conn()
    logging.info('connecting to sql_server_database')

    table_name = 'orders'
    status = load_data_to_SQL_server(transformed_df,sql_conn,table_name)
    send_email(status)

    # close connection
    sql_conn.close()

if __name__=='__main__':
    main()



    
