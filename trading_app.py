import pandas as pd
import multiprocessing
import time
from datetime import datetime,timedelta
import requests
import json
import numpy as np
import alpaca_trade_api as tradeapi
import firebase_admin
from firebase_admin import credentials
from firebase_admin import db

cred = credentials.Certificate("documents-2d6d5-firebase-adminsdk-m0cc3-c4d1f5a972.json")
firebase_admin.initialize_app(cred,{'databaseURL': 'https://documents-2d6d5-default-rtdb.asia-southeast1.firebasedatabase.app'})

ref = db.reference('Stock')



class trading_logs():

    def __init__(self,symbl):
        self.ip = 0
        self.cp = 0
        self.act = ''
        self.qty = 0
        self.tp = 0
        self.pl = 0
        self.symbl = symbl

    def call_addition(self):
        self.j_data = {
                        'Symbl': self.symbl,
                        'Act': self.act,
                        'Qty': self.qty,
                        'IP': self.ip,
                        'CP': self.cp,
                        'TP': self.tp,
                        'P&L': self.pl,
                        'Datetime': datetime.now()
                    }
        pd.json_normalize(self.j_data).to_csv('trade_legisure.csv',sep=',',mode='a',header=None,index=False)
        #print(f"BUY Order Placed {self.j_data}")

    def bkp_open_positions(self):
        self.j_data = {
                        'Symbl': self.symbl,
                        'Act': self.act,
                        'Qty': self.qty,
                        'IP': self.ip,
                        'CP': self.cp,
                        'TP': self.tp,
                        'P&L': self.pl,
                        'Datetime': datetime.now()
                    }
        pd.json_normalize(self.j_data).to_csv('open_positions_legisure.csv',sep=',',mode='a',header=None,index=False)





class trading_meths(multiprocessing.Process):
    def __init__(self, id,current_time,Task,SL,TP,Trade_Price,Trade_Type,Symbl,Balance):
        super(trading_meths, self).__init__()
        self.time_interval = id
        self.SOA = []
        self.BOA = []
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
        self.action = ''
        self.rpal = 0
        self.avg_p = 0
        self.qty = 0
        self.cp = 0
        self.balance = Balance
        self.total_pl = 0
        
    def run(self):
        #print(f'Calculating for {self.time_interval} min time interval')
        c = 0
        self.start_of_trade_acts()
        while True:
            c+= 1
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

                #print(self.decision_map)
                #print(self.trend_a)
                #print("\n")

            if 1 <= c <= 4:
                self.place_trade_order(1,'buy')
            elif 5<= c <= 8:
                self.place_trade_order(1,'sell')
            else:
                self.place_trade_order(2,'buy')
            
            self.running_profit()
            
            if c > 8:
                self.end_of_trade_acts()
                break
            
            print(f'Symbl: {self.Symbl} \n Qty: {self.qty} \n Avg Order Filled Price: {self.avg_p} \n P&L {self.rpal}')
            
            time.sleep(1)
            continue

            if self.trend_a == 'BULL' and self.action != 'buy':
                self.action = 'buy'
                self.place_trade_order(1,self.action)

            elif self.trend_a == 'BEAR' and self.action != 'sell':
                self.action = 'sell'
                self.place_trade_order(1,self.action)


            self.running_profit()
                
            time.sleep(1)
             
    def running_profit(self):
        act = ''
        if len(self.BOA) > 0:
            self.cp = self.latest_closing_data()
            avg_p = 0
            self.qty = len(self.BOA)
            for i in self.BOA:
                avg_p += i.ip
            self.avg_p = avg_p / self.qty
            self.rpal = (self.cp - self.avg_p) * self.qty
            act = 'Long'

        elif len(self.SOA) > 0:
            self.cp = self.latest_closing_data()
            avg_p = 0
            self.qty = len(self.SOA)
            for i in self.SOA:
                avg_p += i.ip
            self.avg_p = avg_p / self.qty
            self.rpal = (self.avg_p - self.cp) * self.qty
            act = 'Short'
        
        if len(self.SOA) + len(self.BOA) == 0:
            self.qty = 0 
            self.avg_p = 0
            self.rpal = 0
            self.cp = 0
            ref.child(f'Running_P&L/{self.Symbl}').delete()

        else:
            users_ref = ref.child('Running_P&L')
            users_ref.update({
                self.Symbl: {
                                'Symbol':self.Symbl,
                                'Qty': self.qty,
                                'Avg Price': self.avg_p,
                                'P&L': self.rpal,
                                'Position': act
                            }
                })
        users_ref = ref.child('Trade')
        users_ref.update({
                            self.Symbl:{
                                'Balance': self.balance,
                                'Total P&L': self.total_pl
                            }
                        })

    def buy_call(self):
        obj = trading_logs(self.Symbl)
        lc = self.latest_closing_data()
        
        if len(self.SOA) == 0:
            obj.ip =  lc
            self.BOA.append(obj)
        else:
            obj.ip = self.SOA.pop().ip
               
        obj.cp = lc
        obj.act = 'B'
        obj.qty = 1
        obj.tp = obj.qty*obj.cp
        obj.pl = (obj.ip - obj.cp)
        self.balance -= lc
        self.total_pl += obj.pl
        obj.call_addition()


    def sell_call(self):
        obj = trading_logs(self.Symbl)
        lc = self.latest_closing_data()
        
        if len(self.BOA) == 0:
            obj.ip =  lc
            self.SOA.append(obj)
        else:
            
            obj.ip = self.BOA.pop().ip
           
        obj.cp = lc
        obj.act = 'S'
        obj.qty = -1
        obj.tp = lc
        obj.pl = (obj.ip - obj.cp)
        self.balance += lc
        self.total_pl += obj.pl
        obj.call_addition()


        
            



    def bar_cal(self,time_interval,current_time,bars):

        #print(f'Calculating for {time_interval} interval')
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
        #print(msg)

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


    def place_trade_order(self,qty,type):
        for i in range(qty):
            if type == 'buy':
                self.buy_call()
            else:
                self.sell_call()

    def end_of_trade_acts(self):
        for i in self.SOA:
            i.bkp_open_positions()
        
        for i in self.BOA:
            i.bkp_open_positions()

    def start_of_trade_acts(self):
        df = pd.read_csv('open_positions_legisure.csv')
        df = df[df['Symbl'] == self.Symbl]
        
        for index,row in df.iterrows():
            obj = trading_logs(self.Symbl)
            obj.symbl = row['Symbl']
            obj.act = row['Act']
            obj.qty = row['Qty']
            obj.ip = row['IP']
            obj.cp = row['CP']
            obj.tp = row['TP']
            obj.pl = row['P&L']

            if row['Act'] == 'B':
                self.BOA.append(obj)
            else:
                self.SOA.append(obj)

        print(f'{self.Symbl} - Buy = {len(self.BOA)} - Sell = {len(self.SOA)}')
        pd.DataFrame(columns = ['Symbl','Act','Qty','IP','CP','TP','P&L','Datetime']).to_csv('open_positions_legisure.csv',sep=',',header=True,index=False)
        






        





if __name__ == '__main__':
    curr = datetime.today() - timedelta(days=7)
    curr = curr.strftime('%Y-%m-%d')
    p1 = trading_meths(3,curr,'TREND',1553.40,1523.40,1533.15,'SELL','AAPL',100000)
    p1.start()
    p2 = trading_meths(3,curr,'TREND',1553.40,1523.40,1533.15,'SELL','^NSEI',100000)
    p2.start()
    
    
# 19675 Nifty short
#  Nifty Buy