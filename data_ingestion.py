from datetime import datetime,timedelta
import yfinance as yf
from yahoo_fin import stock_info
import time
import pandas as pd





def hist_load():
    start_date = datetime(2002, 8, 12)
    end_date = datetime(2023, 9, 22)
    print(f'{start_date} - {end_date}')
    historical_data(start_date,end_date)
    


def historical_data(start_date,end_date):
    data = yf.download('TCS.NS', start=start_date, end=end_date)
    data.to_csv('TCS.csv',sep = ',',index=True)
    

def inc_load():
    symbl = 'AAPL'
    while True:
        now = datetime.now()
        price = stock_info.get_live_price("AAPL")
        print(now, f'{symbl}:', price)
        data = {
                'Time':now,
                'Symbl': f'{symbl}',
                'Price': price
                }
        df = pd.json_normalize(data)
        df.to_csv(f'{symbl}_stock_data.csv',sep=',',mode='a', index=False, header=None)
        time.sleep(1)
    
    #print(real_time_quotes)


def main():
    inc_load()

if __name__ == '__main__':
    main()