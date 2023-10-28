from datetime import datetime,timedelta
import yfinance as yf
from yahoo_fin import stock_info
import time
import pandas as pd
import os.path
import multiprocessing
import requests
import json



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





class Data_Ingestion(multiprocessing.Process):

    def __init__(self,Symbl,Type):

        super(Data_Ingestion, self).__init__()

        self.Symbl = Symbl
        
        self.Type = Type
        
    
    def run(self):
        
        c = self.file_check()

        if self.Type == 'Live':

            self.Live_data(c)

        elif self.Type == 'Day_Wise':

            self.day_wise_data(c,36500)

        elif self.Type == 'One_Min':

            self.one_min_data(c,1)

    
    def file_check(self):
        
        if os.path.isfile(f'{self.Symbl}_{self.Type}.csv'):
            
            return 1 
        
        return 0 

    
    def Live_data(self,flag):
        
        c = flag
        
        while True:
        
            try:

                now = datetime.now()

                price = stock_info.get_live_price(self.Symbl)

                print(now, f'{self.Symbl}:', price)

                data = {
                        'Datetime':now,
                        'Symbl': f'{self.Symbl}',
                        'Price': price
                        }
                
                df = pd.json_normalize(data)

                if c == 0:

                    df.to_csv(f'{self.Symbl}_{self.Type}.csv',sep=',',mode='a', index=False)

                    c+=1

                else:

                    df.to_csv(f'{self.Symbl}_{self.Type}.csv',sep=',',mode='a', index=False, header=None)
                
                print(data)

                time.sleep(1)
           
            except Exception as e:
                
                self.send_slack_alert(f'Ingestion failed for {self.Symbl}\n Load Type = {self.Type}\n err msg => {e} \n Retrying in 10s')
                
                time.sleep(10)
                
                continue


    def day_wise_data(self,flag,d_interval):

        try:
        
            data = yf.download(self.Symbl, start=datetime.now() - timedelta(days=d_interval), end=datetime.now() )

            if flag == 0:
                    
                data.to_csv(f'{self.Symbl}_{self.Type}.csv',sep = ',',index=True,mode='a')
                
            else:
                
                data.to_csv(f'{self.Symbl}_{self.Type}.csv',sep = ',',index=True,mode='a',header=None)

        except Exception as e:

            self.send_slack_alert(f'Ingestion failed for {self.Symbl}\n Load Type = {self.Type}\n err msg => {e} \n Stopping job')




    def one_min_data(self,flag,d_interval):

        start_date = datetime.now() - timedelta(days=d_interval)

        end_date = datetime.now()
        
        #while end_date <= datetime.now() + timedelta(days=10):
        while True:

            try:
            
                print(f'Fetching Data from {start_date} - {end_date}')
                
                data = yf.download(self.Symbl, start=start_date, end=end_date,interval='1m')

                if flag == 0:
                    
                    data.to_csv(f'{self.Symbl}_{self.Type}.csv',sep = ',',index=True,mode='a')
                
                else:
                    
                    data.to_csv(f'{self.Symbl}_{self.Type}.csv',sep = ',',index=True,mode='a',header=None)

                df = pd.read_csv(f'{self.Symbl}_{self.Type}.csv')

                df = df.drop_duplicates()

                df.to_csv(f'{self.Symbl}_{self.Type}.csv',sep = ',',index=False,mode='w')
                
                time.sleep(60)
            
            except Exception as e:

                self.send_slack_alert(f'Ingestion failed for {self.Symbl}\n Load Type = {self.Type}\n err msg => {e} \n Stopping job')

                break


    
    def send_slack_alert(self,msg):

        self.url = 'https://hooks.slack.com/services/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'

        self.headers = {'Content-type': 'application/json'}

        self.data = {"text":msg}

        self.response = requests.post(self.url, headers=self.headers, data=json.dumps(self.data))





if __name__ == '__main__':

    #p2 = Data_Ingestion('TCS.NS','Live')

    #p2.start()

    p2 = Data_Ingestion('TCS.NS','Day_Wise')

    p2.start()

    


