import pandas as pd

df = pd.read_csv('/content/TCS_NS.csv')
df = df.loc[:,['Datetime','Close','Volume']]
df['Datetime'] = df['Datetime'].str.replace('+05:30','',regex=False)
df['updt_hr'] = pd.to_datetime(df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.strftime('%Y%m%d%H').astype('int')
df['updt_m'] = pd.to_datetime(df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.strftime('%M').astype('int')
df = df.sort_values(by=['updt_hr','updt_m'], ascending=True)

df = df[pd.to_datetime(df['Datetime'],format= '%Y-%m-%d %H:%M:%S').dt.strftime('%Y%m%d').astype('int') >= 20231006]


def band(df,i):
  support = df.iloc[i-1,1] >= df.iloc[i,1] <= df.iloc[i+1,1] and df.iloc[i-2,1] >= df.iloc[i,1] <= df.iloc[i+2,1]
  resistance = df.iloc[i-1,1] <= df.iloc[i,1] >= df.iloc[i+1,1] and df.iloc[i-2,1] <= df.iloc[i,1] >= df.iloc[i+2,1]
  if support:
    return 'S'
  elif resistance:
    return 'R'
  else:
    return 'N'


df['grp'] = (df['updt_hr'].astype('str') + df['updt_m'].astype('str')).astype('int') // 15

#tf = df[df['grp'] == 13487274061]
tf = df.loc[:,['grp','Close','Volume']]

mf = tf.groupby('grp').aggregate({'Close':['max','min'],'Volume':['sum']}).reset_index()
wfh = tf.loc[:,['grp','Close']].groupby('grp').head(1)
wft = tf.loc[:,['grp','Close']].groupby('grp').tail(1)


wf = pd.merge(wfh,wft,how='left',left_on='grp',right_on='grp')

tf = pd.merge(mf,wf,how='left',left_on='grp',right_on='grp')

tf = tf.drop(('grp',''), axis=1)

tf.columns = ['grp','Close_Max','Close_Min','Total_Volume','Start_Price','End_Price']
tf['Trend'] = tf['End_Price'].astype('int') - tf['Start_Price'].astype('float')

wf = tf.loc[:,['grp','End_Price','Start_Price']]
wf['row'] = wf.index
#wf = wf[wf['row']>70]
print(wf.columns)


rsf = wf.reset_index(drop=True)

rsf['Flag'] = ''

for index in range(2,len(rsf)-2):
  rsf.iloc[index,-1] = band(rsf,index)





import statistics




resistance_df = rsf[ rsf['Flag'] == 'R' ]

resistance_df = resistance_df.loc[:,['row','End_Price']]

pts_gph,nt_pts_gph = {},{}
ind = {}

for index,row in resistance_df.iterrows():
  pts_gph[row['End_Price'].astype('str')] = {}
  nt_pts_gph[row['End_Price'].astype('str')] = {}

  for i,r in resistance_df[resistance_df['row'] != row['row']].iterrows():
    slope = (r['End_Price'] - row['End_Price']) / (r['row'] - row['row'])
    #print(f"({r['End_Price']} - {row['End_Price']}) // ({r['row']} - {row['row']}) = {slope}")
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


print(pts_gph)
print(nt_pts_gph)

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

#print(c)
print(sp)
print(sl)


tp = wf
tp['max'] = ( float(sp[1]) * (tp['row']) ) + c
rp = tp.copy()
rp

print(float(sp[1]))



import statistics
import matplotlib.pyplot as plt
import seaborn as sns



resistance_df = rsf[ rsf['Flag'] == 'S' ]

resistance_df = resistance_df.loc[:,['row','End_Price']]

pts_gph,nt_pts_gph = {},{}
ind = {}

for index,row in resistance_df.iterrows():
  pts_gph[row['End_Price'].astype('str')] = {}
  nt_pts_gph[row['End_Price'].astype('str')] = {}

  for i,r in resistance_df[resistance_df['row'] != row['row']].iterrows():
    slope = (r['End_Price'] - row['End_Price']) / (r['row'] - row['row'])
    #print(f"({r['End_Price']} - {row['End_Price']}) // ({r['row']} - {row['row']}) = {slope}")
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


print(pts_gph)
print(nt_pts_gph)

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

#print(c)
print(sp)
print(sl)


tp = wf
tp['max'] = ( float(sp[1]) * (tp['row']) ) + c

fig, ax1 = plt.subplots(figsize=(12,6))
sns.lineplot(x='row', y='End_Price', data=wf)
sns.scatterplot(x='row', y='End_Price', data=rsf[ rsf['Flag'] == 'S' ], marker='+')
sns.scatterplot(x='row', y='End_Price', data=rsf[ rsf['Flag'] == 'R' ], marker='o')
sns.scatterplot(x='row', y='End_Price', data=rsf[ rsf['Flag'] == 'N' ], marker='o')
sns.scatterplot(x='row', y='End_Price', data=rsf[ rsf['Flag'] == '' ], marker='o')
sns.lineplot(x='row', y='max', data=tp)
sns.lineplot(x='row', y='max', data=rp)




print(float(sp[1]))



