from datetime import datetime,timedelta
import yfinance as yf
from yahoo_fin import stock_info
import time
import pandas as pd




def one_m_hist_load(symbol):
    start_date = datetime.now() - timedelta(days=29)
    end_date = start_date + timedelta(days=7)
    c = 0
    while end_date <= datetime.now() + timedelta(days=10):
        c+=1
        print(f'Fetching Data from {start_date} - {end_date}')
        historical_data(start_date,end_date,symbol,c)
        start_date = end_date
        end_date = start_date + timedelta(days=7)

    df = pd.read_csv(f"{symbol.replace('.','_')}.csv")
    df = df.drop_duplicates()
    df.to_csv(f"{symbol.replace('.','_')}_1m_data.csv",sep = ',',index=False,mode='w')


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
    c = 0
    while True:
        
        try:
            now = datetime.now()
            price = stock_info.get_live_price(symbl)
            print(now, f'{symbl}:', price)
            data = {
                    'Datetime':now,
                    'Symbl': f'{symbl}',
                    'Price': price
                    }
            df = pd.json_normalize(data)
            if c == 0:
                df.to_csv(f'{symbl}_stock_data.csv',sep=',',mode='a', index=False)
            else:
                df.to_csv(f'{symbl}_stock_data.csv',sep=',',mode='a', index=False, header=None)
            time.sleep(1)
            c+=1
        except:
            time.sleep(10)
            continue
    
    #print(real_time_quotes)


def main():
    symbol = 'TCS.NS'
    #one_m_hist_load(symbol)
    inc_load(symbol)

if __name__ == '__main__':
    main()