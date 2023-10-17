import pandas as pd
import time


def band(df,i):
  support = df.iloc[i-1,1] >= df.iloc[i,1] <= df.iloc[i+1,1] and df.iloc[i-2,1] >= df.iloc[i,1] <= df.iloc[i+2,1]
  resistance = df.iloc[i-1,1] <= df.iloc[i,1] >= df.iloc[i+1,1] and df.iloc[i-2,1] <= df.iloc[i,1] >= df.iloc[i+2,1]
  if support:
    return 'S'
  elif resistance:
    return 'R'
  else:
    return 'N' 
  


def data_prep(symbl,interval):
    df = pd.read_csv(f'{symbl}_stock_data.csv')
    df = df.loc[:,['Datetime','Price']]
    df['Datetime'] = df['Datetime'].str.replace('+05:30','',regex=False)
    df['updt_hr'] = pd.to_datetime(df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.strftime('%Y%m%d%H').astype('int')
    df['updt_m'] = pd.to_datetime(df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.strftime('%M').astype('int')
    df = df.sort_values(by=['updt_hr','updt_m'], ascending=True)
    df = df[pd.to_datetime(df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.strftime('%Y%m%d').astype('int') >= 20231006]
    df['grp'] = (df['updt_hr'].astype('str') + df['updt_m'].astype('str')).astype('int') // interval
    tf = df.loc[:,['grp','Price']]

    mf = tf.groupby('grp').aggregate({'Price':['max','min']}).reset_index()
    wfh = tf.loc[:,['grp','Price']].groupby('grp').head(1)
    wft = tf.loc[:,['grp','Price']].groupby('grp').tail(1)


    wf = pd.merge(wfh,wft,how='left',left_on='grp',right_on='grp')

    tf = pd.merge(mf,wf,how='left',left_on='grp',right_on='grp')

    tf = tf.drop(('grp',''), axis=1)

    tf.columns = ['grp','Close_Max','Close_Min','Start_Price','End_Price']
    tf['Trend'] = tf['End_Price'].astype('int') - tf['Start_Price'].astype('float')

    #print(tf)

    wf = tf.loc[:,['grp','End_Price','Start_Price']]
    wf['row'] = wf.index
    
    rsf = wf.reset_index(drop=True)

    rsf['Flag'] = ''

    

    for index in range(2,len(rsf)-2):
        rsf.iloc[index,-1] = band(rsf,index)

    
    #print(rsf[rsf['Flag']== 'N'])
    return rsf


def band_cal(flag,rsf):
    resistance_df = rsf[ rsf['Flag'] == flag ]
    #print(resistance_df)

    resistance_df = resistance_df.loc[:,['row','End_Price']]

    pts_gph,nt_pts_gph = {},{}
    ind = {}

    for index,row in resistance_df.iterrows():
        pts_gph[row['End_Price'].astype('str')] = {}
        nt_pts_gph[row['End_Price'].astype('str')] = {}

        for i,r in resistance_df[resistance_df['row'] != row['row']].iterrows():
            slope = (r['End_Price'] - row['End_Price']) / (r['row'] - row['row'])
            
            if -1 <= slope <= 1:
                try:
                    pts_gph[row['End_Price'].astype('str')][str(slope)].append(r['End_Price'])
                except:
                    pts_gph[row['End_Price'].astype('str')][str(slope)] = [row['End_Price']]
                    pts_gph[row['End_Price'].astype('str')][str(slope)].append(r['End_Price'])

            else:
                try:
                    nt_pts_gph[row['End_Price'].astype('str')][str(slope)].append(r['End_Price'])
                except:
                    nt_pts_gph[row['End_Price'].astype('str')][str(slope)] = [row['End_Price']]
                    nt_pts_gph[row['End_Price'].astype('str')][str(slope)].append(r['End_Price'])


    #print(pts_gph)
    #print(nt_pts_gph)

    pt,sl,sp = 0,[],0
    pts_gph_keys = list(pts_gph.keys())
    for j in range(1,len(pts_gph_keys)+1):
        i = pts_gph_keys[-j]
        for s in pts_gph[i]:
            if len(pts_gph[i][s]) > pt:
                pt = len(pts_gph[i][s])
                sl = pts_gph[i][s]
                sp = [i,s]


    sr = resistance_df[resistance_df['End_Price'].astype('str') == sp[0]].row.values[0].item()

    c = float(sp[0].item()) - ( float(sp[1]) * int(sr) )

    return float(sp[1]),c



if __name__ == '__main__':
    co = 0
    interval = 10
    rsf = data_prep('TCS.NS',interval)
    
    
    R_slope,R_intercept = band_cal('R',rsf)
    S_slope,S_intercept = band_cal('S',rsf)
    while True:
        rsf = data_prep('TCS.NS',interval)
        rsf = rsf.iloc[-1,:]
        
        if (R_slope > 0  and S_slope > 0):
            print('BULL')
        elif (R_slope < 0  and S_slope < 0):
            print('BEAR')
        else:
            print('Side')

        UL = ( R_slope * rsf['row'] ) + R_intercept
        LL = ( S_slope * rsf['row'] ) + S_intercept
        print(f" Resistance -> {UL}")
        print(f" Support -> {LL}")

        print(f"Current Val = {rsf['End_Price']}")

        time.sleep(10)

        co+= 1

        if co == 10:
            rsf = data_prep('TCS.NS',interval)
            R_slope,R_intercept = band_cal('R',rsf)
            S_slope,S_intercept = band_cal('S',rsf)
            co = 0

        print('\n')




