import pandas as pd
import multiprocessing
import time
from datetime import datetime,timedelta
import requests
import json
import numpy as np





class trading_meths(multiprocessing.Process):
    def __init__(self, id,current_time,Task,SL,TP,Trade_Price,Trade_Type,Symbl):
        super(trading_meths, self).__init__()
        self.time_interval = id
        self.current_time = current_time
        self.Task = Task
        self.SL = SL
        self.TP = TP
        self.Trade_Price = Trade_Price
        self.Trade_Type = Trade_Type
        self.Symbl = Symbl
        self.decision_map = {'BULL':0,'BEAR':0,'SIDE':0}
        self.trend_a = ''
        self.trend_p = ''
                 
    def run(self):
        #print(f'Calculating for {self.time_interval} min time interval')
        while True:
            self.decision_map = {'BULL':0,'BEAR':0,'SIDE':0}
            if self.Task == 'ALERT':
                self.price_reach_alert()
            else:
                for i in [3,5,10,15]:
                    self.decision_map[self.bar_cal(i,self.current_time,4)] += 1
                if self.decision_map['BULL'] > self.decision_map['BEAR'] and self.decision_map['BULL'] > self.decision_map['SIDE']:
                    self.trend_a = 'BULL'
                elif self.decision_map['BEAR'] > self.decision_map['BULL'] and self.decision_map['BEAR'] > self.decision_map['SIDE']:
                    self.trend_a = 'BEAR'
                else:
                    self.trend_a = 'SIDE'

                if self.trend_a != self.trend_p:
                    self.msg_val = f'Its a {self.trend_a} Run for {self.Symbl}'
                    self.trend_p = self.trend_a
                    self.send_slack_alert(self.msg_val)

                print(self.decision_map)
                print(self.trend_a)
                print("\n")
            
            time.sleep(1)
                
                
            

    def bar_cal(self,time_interval,current_time,bars):

        print(f'Calculating for {time_interval} interval')
        st = datetime.now() - timedelta(days=7)
        #st = st.strftime('%Y-%m-%d') + ' 09:15:00'



        self.df = pd.read_csv(f'{self.Symbl}_stock_data.csv')

        self.df = self.df.drop_duplicates()

        self.df.sort_values(by='Datetime',ascending=True)

        self.df['Datetime'] = self.df['Datetime'].str.replace('+05:30','',regex=False)

        self.df['updt'] = pd.to_datetime(self.df['Datetime'],format= '%Y-%m-%d %H:%M:%S')

        self.df = self.df[(self.df['updt'] >= st) ]
        
        self.df['date'] = pd.to_datetime(self.df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.date
        self.df['hour'] = pd.to_datetime(self.df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.hour
        self.df['minute'] = pd.to_datetime(self.df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.minute
        self.df['3m'] = self.df['minute']//time_interval
        self.df['grp_3m'] = self.df['updt'].dt.strftime('%Y%m%d%H')+self.df['3m'].astype(str)
        
        self.df = self.df.sort_values(by=['updt'], ascending=True)

        self.temp_df = self.df.groupby('grp_3m').apply(lambda x: x.iloc[[-1]]).reset_index(drop=True).sort_values(by=['updt'], ascending=True)

        self.temp_df = self.temp_df.loc[:,['updt','grp_3m','Price']]
        self.temp_df['Opening'] = self.temp_df['Price'].shift(1)
        self.temp_df['Closing'] = self.temp_df['Price']

        self.temp_df = self.temp_df.where(self.temp_df['Opening'].notna()).dropna()
        self.temp_df['Trend'] = self.temp_df['Closing'] - self.temp_df['Opening']

        self.trend = self.temp_df['Trend'].values[-(bars+1):-1]

        #print(self.temp_df['Trend'].values[-(bars+1):-1])
        
        if np.sum(self.trend) > 4:
            return 'BULL'
        elif np.sum(self.trend) < -4:
            return 'BEAR'
        else:
            return 'SIDE'


    def latest_closing_data(self):
        self.df = pd.read_csv(f'{self.Symbl}_stock_data.csv')

        self.df = self.df.drop_duplicates()
        self.df = self.df.loc[:,['Datetime','Price']]
        
        self.df.sort_values(by='Datetime',ascending=True)

        self.df['Datetime'] = self.df['Datetime'].str.replace('+05:30','',regex=False)

        self.df['updt'] = pd.to_datetime(self.df['Datetime'],format= '%Y-%m-%d')

        self.df = self.df[self.df['updt'] >= self.current_time]

        return self.df['Price'].values[-1]


    def previous_day_closing(self):
        self.prev_dev = pd.read_csv(f"{self.Symbl.replace('.','_')}.csv")
        self.prev_dev = self.prev_dev.drop_duplicates()
        self.prev_dev = self.prev_dev.loc[:,['Datetime','Close']]

        self.prev_dev.sort_values(by='Datetime',ascending=True)

        self.prev_dev['Datetime'] = self.prev_dev['Datetime'].str.replace('+05:30','',regex=False)

        self.prev_dev['updt'] = pd.to_datetime(self.prev_dev['Datetime'],format= '%Y-%m-%d')

        self.prev_dev = self.prev_dev[self.prev_dev['updt'] >= self.current_time]

        return self.prev_dev['Close'].values[-1]



    def send_slack_alert(self,msg):
        self.url = 'https://hooks.slack.com/services/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'

        self.headers = {'Content-type': 'application/json'}

        self.data = {"text":msg}

        self.response = requests.post(self.url, headers=self.headers, data=json.dumps(self.data))

        #print(self.response.status_code)
        #print(self.response.text)


    def price_reach_alert(self):
        self.CP = self.latest_closing_data()

        if self.Trade_Type == 'BUY':
            self.LossVal = self.Trade_Price - self.SL
            self.ProfVal = self.TP - self.Trade_Price
        else:
            self.LossVal =  self.SL - self.Trade_Price
            self.ProfVal = self.Trade_Price - self.TP
        

        if self.CP == self.SL:
            self.msg_val = f'Stop Loss Reached and Trade Has been Called Off to Avoid More Loss \n SL - {self.SL} \n CP - {self.CP} \n Total Loss - {self.LossVal}'
            self.send_slack_alert(self.msg_val)
        elif self.CP == self.TP:
            self.msg_val = f'Take Profit Reached and Trade Has been Called Off \n TP - {self.TP} \n CP - {self.CP} \n Total Loss - {self.ProfVal}'
            self.send_slack_alert(self.msg_val)

        #print(f'{self.CP}')




if __name__ == '__main__':
    curr = datetime.today()
    curr = curr.strftime('%Y-%m-%d')
    p1 = trading_meths(3,curr,'TREND',1553.40,1523.40,1533.15,'SELL','^NSEI')
    p1.start()
    
    
# 19675 Nifty short
#  Nifty Buy