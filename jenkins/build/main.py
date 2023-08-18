import utils.constant as constant
import os

from utils.csv_to_db import MCSTATUS
from dotenv import load_dotenv

load_dotenv()

MCSTATUS = MCSTATUS(
        path=constant.MCSTATUS_PATH,
        server=os.getenv('SERVER'),
        database=os.getenv('DATABASE'),
        user_login=os.getenv('USER_LOGIN'),
        password=os.getenv('PASSWORD'),
        table=constant.MCSTATUS_TABLE,
        table_columns=constant.MCSTATUS_TABLE_COLUMNS,
        
        table_log=constant.MCSTATUS_TABLE_LOG,
        table_columns_log=constant.MCSTATUS_TABLE_COLUMNS_LOG,
        notify_token=os.getenv('NOTIFY_TOKEN'),
    )


MCSTATUS.run()