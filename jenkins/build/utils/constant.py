# data MCSTATUS
MCSTATUS_PATH = '/data/data_mcstatus/gd' #'/data/MCSTATUS_ng' ,'D:/data/MCSTATUS_ng'
MCSTATUS_TABLE = 'data_mcstatus'
MCSTATUS_TABLE_COLUMNS ='''
            registered_at datetime,
			occurred datetime,
			mc_status varchar(50),
            mc_no varchar(50),
            '''
            
MCSTATUS_TABLE_LOG = 'log_mcstatus'
MCSTATUS_TABLE_COLUMNS_LOG ='''
            registered_at datetime,
	        status varchar(50),
            file_name varchar(100),
            process varchar(50),
            message varchar(50),
            error varchar(MAX),
            '''

#LOG status
STATUS_OK = 'ok'
STATUS_ERROR = 'error'
STATUS_INFO = 'info'
