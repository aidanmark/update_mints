import pandas as pd
from IPython.display import display

from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas
import warnings
warnings.filterwarnings("ignore")
from load_single_df import *

print('starting...')


def grab_snowflake():
    '''
    Returns: all previous mints
    Returns Type: DataFrame

    Grabs data from most recent jpy_all_mints snowflake table
    '''
    #Establish engines and parameters
    engines = engine_builder()

    snf_engine = engines
    res = snf_engine.cursor() # create a cursor with engine    
    res.execute('USE ROLE INTERN')
    
    res.execute('USE DATABASE PROD_JUPYTER') #set workspace
    #find all past jpy_all_mints tables
    query = """
            SELECT 
                *
            FROM PROD_JUPYTER.INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME like '%JPY_ALL_MINTS%'
            """
    tables = pd.read_sql(query, snf_engine)
    #find jpy_all_mints table that was created most recently
    name = tables['TABLE_NAME'].to_list()# make this a list for traversing 
    dates = {}
    for i in range(len(name)):
        date = str(name[i]) #typeset
        #slice to timestamp as "year/month/day hour:minute:second"
        date = date[14:18]+'/'+date[19:21]+'/'+date[22:24]+' '+date[25:27]+':'+date[28:30]+':'+date[31:33]
        
        new_date = datetime.strptime(date, '%Y/%m/%d %H:%M:%S')
        dates[date] = name[i]
    last_update = dates.get(max(dates))
    #find serials of all past mints from snowflake
    query = f"""
            SELECT
                serial as stored_serials
            FROM PROD_JUPYTER.JPY_CLEAN.{last_update}
            """
    snowflake_df = pd.read_sql(query, snf_engine)
    snowflake_df = snowflake_df.applymap(lambda x: x.replace('"', ''))

    print('snowflake data grabbed...')
    return snf_engine, snowflake_df


def net_new_mints(old_data, new_data):
    '''
    Args: old_data --> old mints, new_data --> all mints (including new)
    Args Type: old_data: DataFrame, new_data: DataFrame
    Returns: net new mints
    Returns Type: DataFrame

    Creates dataframe of latest mints
    '''
    #Establish local variables
    old_df = old_data.copy()
    new_df = new_data.copy()
    #finds difference in sheets and most recent snowflake table
    old_list = old_df['STORED_SERIALS'].to_list()
    old_list = [str(item) for item in old_list]
    new_list = new_df['serial'].to_list()
    test_list = [item for item in new_list if item not in old_list]
    print('\nThere have been '+str(len(test_list))+' new mints since last update.')
    global net_df
    net_df = new_df.loc[new_df['serial'].isin(test_list)]

    print('new mints dataframe created...')
    return net_df


def append_time(engine):
    '''
    Args: engine --> snowflake connector, df --> latest mints
    Args Type: engine: SnowflakeConnection, df: DataFrame
    Returns: new mints with month
    Returns Type: DataFrame

    Joins mint timestamp to latest mints dataframe
    '''
    #establish local vars 
    snf_engine = engine
    #finds mint_time for all mints
    query = """
            SELECT
                identifier as serial,
                mint_time
            FROM PROD_JUPYTER.JPY_CLEAN.JC_MINTS
            WHERE asset not in(1,591)
            """ 
    jc_mints = pd.read_sql(query, snf_engine)
    jc_mints = jc_mints.applymap(lambda x: x.replace('"', ''))
    #joins net new mint dataframe and mint_time
    net_df.rename(columns={'serial':'serial'}, inplace = True)
    jc_mints.rename(columns={'SERIAL':'serial','MINT_TIME':'mint_time'}, inplace = True)
    net_clean_df = pd.merge(net_df, jc_mints, on='serial', how='left')
    net_clean_df = net_clean_df.applymap(lambda x: x.replace('"', ''))
    #truncates timestamp to year and month
    net_clean_df['mint_time'] = net_clean_df['mint_time'].str.slice(0, 7)

    print('timestamp added...')
    return net_clean_df


def to_list(df):
    '''
    Args: df --> latest mints dataframe
    Args Type: df: DataFrame
    Returns: latest mints list
    Returns Type: list

    Converts latest mints dataframe to list form
    '''

    #establish local vars
    net_clean_df = df

    net_df =[]
    for index, rows in net_clean_df.iterrows():
        my_list = [rows.serial,
                   float(rows.amount_to_drop),
                   float(rows.amount_to_originator),
                   float(rows.amount_to_dibbs),
                   float(rows.card_price),
                   rows.originator,
                   rows.mint_time]    
        net_df.append(my_list)

    print('list created...')
    return net_df


def to_dict(lst):
    '''
    Args: listed --> latest mints list
    Args Type: df: list
    Returns: latest mints dictionary
    Returns Type: dict

    Converts latest mints list to dictionary with month as key
    '''
    net_df = lst
    net_recent_mints_dict = {}
    for row in net_df:
        if row[6] not in net_recent_mints_dict:
            net_recent_mints_dict[row[6]] = []
        net_recent_mints_dict[row[6]].append(row[:6])

    print('dictionary created...')
    return net_recent_mints_dict


def export(dct, new_data, engine):
    '''
    Args: dicts --> latest mints dictionary
    Args Type: dicts: dict
    Returns: latest mints clean
    Returns Type: string

    exports data to txt file and updates snowflake table
    '''
    #converts dictionary to string (so that ' becomes ")

    #establish locals
    net_recent_mints_dict = dct
    gsheet_df = new_data
    snf_engine = engine

    time = str(datetime.now()).replace(' ','_').replace('-','_').replace(':','_').replace('.','_')

    recent_mints_clean = str(net_recent_mints_dict).replace("'", '"')
    #exports txt file as 'recent_mints_year_month_day_hour_minute_second_microsecond.txt'
    with open('recent_mints'+'_'+time+'.txt', 'w') as f:
        print(recent_mints_clean, file=f)
    
    # name table: prod_jupyter.jpy_clean.JPY_ALL_MINTS_year_month_day_hour_minute_second_microsecond
    t_name = 'ALL_MINTS'
    t_schema = 'JPY_CLEAN'
    t_db = 'PROD_JUPYTER'

    table_name = t_name+'_'+time
    #exports all mints to snowflake
    frame_to_snowflake(table_name, gsheet_df, t_schema, t_db, snf_engine)

    print('export successful...')
    return recent_mints_clean


def update_mints():
    '''
    exports new mint data to txt file and updates snowflake table accordingly
    '''
    
    #et new data 
    gsheet_df = fetch_gsheet('1n_7Vulppfk8fxETqLHVPt-35x1aUHbYHUJ2j5WxkHzA', 'all_mints')
    print('sheets data loaded...')

    #get old data
    fetch_data = grab_snowflake()
    engine = fetch_data[0]
    cached_data = fetch_data[1]
    latest_mints = net_new_mints(cached_data, gsheet_df)
    net_recent_mints =  append_time(engine)
    list_output = to_list(net_recent_mints)
    dict_output = to_dict(list_output)
    push_and_download = export(dict_output, gsheet_df, engine)
    return 

    
def main():
    update_mints()


if __name__ == "__main__":
    main()