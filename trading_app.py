import pandas as pd
import multiprocessing
import time
from datetime import datetime
import requests
import json





class trading_meths(multiprocessing.Process):
    def __init__(self, id):
        super(trading_meths, self).__init__()
        self.time_interval = id
                 
    def run(self):
        print(f'Calculating for {self.time_interval} min time interval')
        self.bar_cal(self.time_interval)

    def bar_cal(self,min):
        self.df = pd.read_csv('TCS_NS.csv')

        self.df = self.df.drop_duplicates()

        self.df.sort_values(by='Datetime',ascending=True)

        self.df['Datetime'] = self.df['Datetime'].str.replace('+05:30','',regex=False)

        self.df['updt'] = pd.to_datetime(self.df['Datetime'],format= '%Y-%m-%d %H:%M:%S')
        self.df['date'] = pd.to_datetime(self.df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.date
        self.df['hour'] = pd.to_datetime(self.df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.hour
        self.df['minute'] = pd.to_datetime(self.df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.minute
        self.df['3m'] = self.df['minute']//self.time_interval
        self.df['grp_3m'] = self.df['updt'].dt.strftime('%Y%m%d%H')+self.df['3m'].astype(str)
        #df['grp_3m'] = df['grp_3m'].astype(int)

        self.df = self.df.sort_values(by=['updt'], ascending=True)

        self.temp_df = self.df.groupby('grp_3m').apply(lambda x: x.iloc[[-1]]).reset_index(drop=True).sort_values(by=['updt'], ascending=True)

        self.temp_df = self.temp_df.loc[:,['updt','grp_3m','Close']]
        self.temp_df['Opening'] = self.temp_df['Close'].shift(1)
        self.temp_df['Closing'] = self.temp_df['Close']

        self.temp_df = self.temp_df.where(self.temp_df['Opening'].notna()).dropna()
        self.temp_df['Trend'] = self.temp_df['Closing'] - self.temp_df['Opening']

        self.msg_val = f"For time interval of {self.time_interval} Mins latest closing val is {str(self.temp_df['Trend'].values[-1])}"

        self.send_slack_alert(self.msg_val)


    def send_slack_alert(self,msg):
        self.url = 'https://hooks.slack.com/services/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'

        self.headers = {'Content-type': 'application/json'}

        self.data = {"text":msg}

        self.response = requests.post(self.url, headers=self.headers, data=json.dumps(self.data))

        print(self.response.status_code)
        print(self.response.text)




if __name__ == '__main__':
    for i in range(1000):
        print(f'Iteration {i}')
        p1 = trading_meths(3)
        p1.start()
        p2 = trading_meths(5)
        p2.start()
        time.sleep(1)
    
    