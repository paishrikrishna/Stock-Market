from datetime import datetime,timedelta
import yfinance as yf
from yahoo_fin import stock_info
import time
import pandas as pd




def one_m_hist_load(symbol):
    start_date = datetime.now() - timedelta(days=2)
    end_date = start_date + timedelta(days=7)
    c = 0
    while end_date <= datetime.now() + timedelta(days=10):
        c+=1
        print(f'Fetching Data from {start_date} - {end_date}')
        historical_data(start_date,end_date,symbol,c)
        start_date = end_date
        end_date = start_date + timedelta(days=7)


def hist_load():
    start_date = datetime.now() - timedelta(days=7)
    end_date =  datetime.now()  
    print(f'{start_date} - {end_date}')
    historical_data(start_date,end_date)
    


def historical_data(start_date,end_date,symbol,c):
    data = yf.download(symbol, start=start_date, end=end_date,interval='1m')
    if c == 1:
        data.to_csv(f"{symbol.replace('.','_')}.csv",sep = ',',index=True,mode='a')
    else:
        data.to_csv(f"{symbol.replace('.','_')}.csv",sep = ',',index=True,mode='a',header=None)
    

def inc_load(symbl):
    while True:
        now = datetime.now()
        price = stock_info.get_live_price("AAPL")
        print(now, f'{symbl}:', price)
        data = {
                'Datetime':now,
                'Symbl': f'{symbl}',
                'Price': price
                }
        df = pd.json_normalize(data)
        df.to_csv(f'{symbl}_stock_data.csv',sep=',',mode='a', index=False, header=None)
        time.sleep(1)
    
    #print(real_time_quotes)


def main():
    symbol = 'TCS.NS'
    one_m_hist_load(symbol)

if __name__ == '__main__':
    main()