import pandas as pd



def pts_detect(df,i):
    support = df.iloc[i-1,1] >= df.iloc[i,1] <= df.iloc[i+1,1] and df.iloc[i-2,1] >= df.iloc[i,1] <= df.iloc[i+2,1]
    resistance = df.iloc[i-1,1] <= df.iloc[i,1] >= df.iloc[i+1,1] and df.iloc[i-2,1] <= df.iloc[i,1] >= df.iloc[i+2,1]
    if support:
        return 'S'
    elif resistance:
        return 'R'
    else:
        return 'N' 


# Trend Line Calculation

def band_cal(flag,rsf):
    resistance_df = rsf[ rsf['Flag'] == flag ]
    #print(resistance_df)

    resistance_df = resistance_df.loc[:,['row','End_Price']]

    pts_gph,nt_pts_gph = {},{}
    ind = {}

    for index,row in resistance_df.iterrows():
        pts_gph[row['End_Price']] = {}
        nt_pts_gph[row['End_Price']] = {}

        for i,r in resistance_df[resistance_df['row'] != row['row']].iterrows():
            slope = (r['End_Price'] - row['End_Price']) / (r['row'] - row['row'])
            
            if -10 <= slope <= 10:
                try:
                    pts_gph[row['End_Price']][str(slope)].append(r['End_Price'])
                except:
                    pts_gph[row['End_Price']][str(slope)] = [row['End_Price']]
                    pts_gph[row['End_Price']][str(slope)].append(r['End_Price'])

            #else:
            #    try:
            #        nt_pts_gph[row['End_Price']][str(slope)].append(r['End_Price'])
            #    except:
            #        nt_pts_gph[row['End_Price']][str(slope)] = [row['End_Price']]
            #        nt_pts_gph[row['End_Price']][str(slope)].append(r['End_Price'])


    #print(pts_gph)
    #print(nt_pts_gph)

    pt,sl,sp = 0,0,0
    pts_gph_keys = list(pts_gph.keys())
    for j in range(1,len(pts_gph_keys)+1):
        i = pts_gph_keys[-j]
        for s in pts_gph[i]:
            if len(pts_gph[i][s]) > pt:
                pt = len(pts_gph[i][s])
                sl = s
                sp = [i,s]


    #print(sp,resistance_df)
    
    try:
        slope = float(sp[1])

        sr = resistance_df[resistance_df['End_Price'] == sp[0]].row.values[0].item()

        c = float(sp[0].item()) - ( float(sp[1]) * int(sr) )

    except:

        slope = 100000
        c = 0

    

    return slope,c



def data_prep(symbl,bars):
    df = pd.read_csv(f'{symbl}_Day_Wise.csv')

    df = df.drop_duplicates()

    df['Date'] = pd.to_datetime(df['Date'],format='%Y-%m-%d')

    df['Week_Number'] = df['Date'].dt.strftime('%Y%U').astype('int')

    df = df.sort_values(by=['Date'], ascending=True)

    # Week level Agg

    tf=df

    # Week's Highestm Lowest trading price and Total Vol traded
    mf = tf.groupby('Week_Number').aggregate({'High':['max'],'Low':['min'],'Volume':['sum']}).reset_index()
    mf.columns = ['Week_Number','Max_High','Min_Low','Total_Volume']

    # Week's open and close price
    wfh = tf.loc[:,['Week_Number','Open']].groupby('Week_Number').head(1)
    wft = tf.loc[:,['Week_Number','Close']].groupby('Week_Number').tail(1)

    # Join all the Agg datasets 
    wf = pd.merge(wfh,wft,how='left',left_on='Week_Number',right_on='Week_Number')
    tf = pd.merge(mf,wf,how='left',left_on='Week_Number',right_on='Week_Number')

    tf = tf.rename(columns={'Close':'End_Price','Week_Number':'row'})
    tf = tf.tail(bars)

    wf = tf.loc[:,['row','End_Price','Max_High','Min_Low','Total_Volume','Open']]

    # Add Flag to mark Support and Resistance
    wf['Flag'] = ''


    # Find Support and Resistance Points
    for index in range(2,len(wf)-2):
            wf.iloc[index,-1] = pts_detect(wf,index)
    
    return wf



def detect_bands():
    bar = 0
    m,sm = 100000,100000

    while m == 100000 or sm == 100000:

        bar += 5

        wf = data_prep('TCS.NS',bar)

        # Resistance Cal
        m,c = band_cal('R',wf)

        # Support Cal
        sm,sc = band_cal('S',wf)

        print(f"Resistance Line = Slope -> {m} Intercept - {c}")
        print(f"Support Line = Slope -> {sm} Intercept - {sc}")
        print("\n")




def main():
    detect_bands()




if __name__ == '__main__':
    main()











