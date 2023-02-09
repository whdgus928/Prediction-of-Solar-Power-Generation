from datetime import datetime, timedelta
import time
import requests
import pandas as pd
import numpy as np

#프로그램 문제점: 과거 날짜로 돌아가면 미래 데이터가 남음

'''
사용법
1. start, last : 시작 일, 마지막 일 설정
2. path: 경로 설정
'''

# ------------------------------설정--------------------------------------
#2022년 10월 24일 ~ 2022년 11월 18일
start = "2022-11-15"
last = "2022-11-15"

path='C:\\Users\\user\\Desktop\\VS code\\test\\태양광 대회\\api_data\\'

# -------------------------------함수-----------------------------------------
def hour_avg(path_):
    df = pd.read_csv(path_)
    df=df.drop_duplicates(['id','time'],keep='last')
    df['time'] = df['time'].apply(lambda x : str(x).split(':')[0] + ':00:00')
    df_process = df.groupby(['id','time']).mean().reset_index()
    df_process['time'] = pd.to_datetime(df_process['time'])
    df_process = df_process.sort_values(by = ['id','time']).reset_index(drop = True)
    path_ = path_.split('/')[-1].split('.')[0] + '_afterProcess.csv'
    #print(path_)
    #df_process.to_csv(path_, index = False, encoding = 'utf-8-sig')
    return df_process

def make_weather1_case(id_, extractor, gens,  weather1, forecast1):
    gens_weather1_merge = pd.merge(gens[gens['id'] == id_], weather1[weather1['id'] == extractor.loc[extractor.index[0], 'wth1_id']], on = 'time')
    gens_weather1_forecast1_merge = pd.merge(gens_weather1_merge, forecast1[forecast1['id'] == extractor.loc[extractor.index[0], 'wth1_id']], on = 'time')
    gens_weather1_forecast1_merge.drop(columns = ['id_y','id','precip_prob'], inplace = True)
    gens_weather1_forecast1_merge.columns = ['id', 'time', 'amount', 'temperature', 'humidity', 'dew_point', 'wind_dir', 'wind_spd', 'uv_idx', 'visibility',
                                            'cloudiness', 'ceiling', 'pressure', 'precip_1h', 'temperature_forcast', 'humidity_forcast', 'dew_point_forcast', 'wind_dir_forcast',
                                            'wind_spd_forcast', 'uv_idx_forcast', 'visibility_forcast', 'ceiling_forcast', 'cloudiness_forcast', 'precip_1h_forcast']
    return gens_weather1_forecast1_merge

def season_maker(x):
    # 겨울
    if x in [12,1,2]:
        x = 4
        return x
    # 봄
    elif x in [3,4,5]:
        x = 1
        return x
    # 여름
    elif x in [6,7,8]:
        x = 2
        return x
    # 가을
    else :
        x = 3
        return x
    
def moving_avg_amount(base_df):
    base_df = base_df.sort_values(by = ['id','month','hour'])
    vin_df = pd.DataFrame(columns = base_df.columns)
    for id_num in range(0,21):
        print(id_num)
        base_df_one_id = base_df[base_df['id'] == id_num]
        base_df_one_id['amount_3days'] = base_df_one_id['amount'].rolling(window=3, min_periods = 1, closed = 'left').mean()
        base_df_one_id['amount_5days'] = base_df_one_id['amount'].rolling(window=5, min_periods = 1, closed = 'left').mean()
        vin_df = pd.concat([vin_df, base_df_one_id], axis = 0)
    vin_df = vin_df.sort_index()
    #amount 이동평균 결측치 자기 자신으로 채우기
    vin_df.loc[vin_df['amount_3days'].isnull(),'amount_3days']=vin_df.loc[vin_df['amount_3days'].isnull(),'amount']
    vin_df.loc[vin_df['amount_5days'].isnull(),'amount_5days']=vin_df.loc[vin_df['amount_5days'].isnull(),'amount']

    return vin_df

def column_maker(base_df):
    vin_df = pd.DataFrame(columns = base_df.columns)
    for id_num in range(0,21):
        print(id_num)
        base_df_one_id = base_df[base_df['id'] == id_num]
        base_df_one_id['uv_mean'] = base_df_one_id['uv_idx'].rolling(window=4, min_periods = 1, closed = 'left').mean()
        base_df_one_id['humidity_mean'] = base_df_one_id['humidity'].rolling(window=4, min_periods = 1, closed = 'left').mean()
        base_df_one_id['temperature_mean'] = base_df_one_id['temperature'].rolling(window=4, min_periods = 1, closed = 'left').mean()
        vin_df = pd.concat([vin_df, base_df_one_id], axis = 0)
    vin_df = vin_df.sort_index()
    
    #맨 앞은 자기 자신으로 채우기
    vin_df.loc[vin_df['uv_mean'].isnull(),'uv_mean']=vin_df.loc[vin_df['uv_mean'].isnull(),'uv_idx']
    vin_df.loc[vin_df['humidity_mean'].isnull(),'humidity_mean']=vin_df.loc[vin_df['humidity_mean'].isnull(),'humidity']
    vin_df.loc[vin_df['temperature_mean'].isnull(),'temperature_mean']=vin_df.loc[vin_df['temperature_mean'].isnull(),'temperature']

    return vin_df

# -------------------------------데이터 수집-----------------------------------------------
# pv_gens
start_date = datetime.strptime(start, "%Y-%m-%d")
last_date = datetime.strptime(last, "%Y-%m-%d")

df=pd.DataFrame(columns=['pv_id', 'time', 'amount'])

# 종료일 까지 반복
while start_date <= last_date:
    date = start_date.strftime("%Y-%m-%d")

    pv_gens = requests.get(f'https://research-api.dershare.xyz/open-proc/cmpt-2022/pv-gens/{date}', headers={
                            'Authorization': f'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTSkRYc3lqVWdEaUt0aHN1Y241dERaIiwiaWF0IjoxNjY3ODA5NjQwLCJleHAiOjE2Njg3ODM2MDAsInR5cGUiOiJhcGlfa2V5In0.W3mUTBYStCT5pj5LX4RI67oIxdF-skc8xEfZ5nJdFGg'
                        }).json()
    #globals()['df_pv_gens_{}'.format(date)]
    df_pv_gens = pd.DataFrame(columns = ['pv_id', 'time', 'amount'])
    for idx, i in enumerate(pv_gens):
        df_pv_gens.loc[idx, 'pv_id'] = i['pv_id']
        df_pv_gens.loc[idx, 'time'] = i['time']
        df_pv_gens.loc[idx, 'amount'] = i['amount']
    
    df=pd.concat([df,df_pv_gens])
    # 하루 더하기
    start_date += timedelta(days=1)
    
df.to_csv(f'{path}api_pv_gens.csv',index=False)
    
# # 광명 센서
# # 시작일, 종료일 datetime 으로 변환
# start_date = datetime.strptime(start, "%Y-%m-%d")
# last_date = datetime.strptime(last, "%Y-%m-%d")

# col=['time', 'nins', 'mtemp']

# df=pd.DataFrame(columns=col)

# while start_date <= last_date:
#     date = start_date.strftime("%Y-%m-%d")

#     success = requests.get(f'https://research-api.dershare.xyz/open-proc/cmpt-2022/evironments/{date}', headers={
#                             'Authorization': f'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTSkRYc3lqVWdEaUt0aHN1Y241dERaIiwiaWF0IjoxNjY3ODA5NjQwLCJleHAiOjE2Njg3ODM2MDAsInR5cGUiOiJhcGlfa2V5In0.W3mUTBYStCT5pj5LX4RI67oIxdF-skc8xEfZ5nJdFGg'
#                         }).json()
#     #globals()['df_pv_gens_{}'.format(date)]
#     df_KwangMyeongsensor = pd.DataFrame(columns = col)
#     for idx, i in enumerate(success):
#         for j in range(len(col)):
#             df_KwangMyeongsensor.loc[idx, col[j]] = i[col[j]]
    
#     df=pd.concat([df,df_KwangMyeongsensor])
#     # 하루 더하기
#     start_date += timedelta(days=1)
    
# df.to_csv(f'{path}api_KwangMyeongsensor.csv',index=False)

# weathers1
start_date = datetime.strptime(start, "%Y-%m-%d")
last_date = datetime.strptime(last, "%Y-%m-%d")

col=["id","time","temperature","humidity","dew_point","wind_dir","wind_spd","uv_idx","visibility","cloudiness","ceiling","pressure","precip_1h"]

df=pd.DataFrame(columns=col)
while start_date <= last_date:
    date = start_date.strftime("%Y-%m-%d")

    weathers_1 = requests.get(f'https://research-api.dershare.xyz/open-proc/cmpt-2022/weathers/1/observeds/{date}', headers={
                            'Authorization': f'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTSkRYc3lqVWdEaUt0aHN1Y241dERaIiwiaWF0IjoxNjY3ODA5NjQwLCJleHAiOjE2Njg3ODM2MDAsInR5cGUiOiJhcGlfa2V5In0.W3mUTBYStCT5pj5LX4RI67oIxdF-skc8xEfZ5nJdFGg'
                        }).json()
    
    df_weathers_1 = pd.DataFrame(columns = col)
    for idx, i in enumerate(weathers_1):
        for j in range(len(col)):
            df_weathers_1.loc[idx, col[j]] = i[col[j]]
    
    df=pd.concat([df,df_weathers_1])
    start_date += timedelta(days=1)

df.to_csv(f'{path}api_weathers_1.csv',index=False)

# weathers2
start_date = datetime.strptime(start, "%Y-%m-%d")
last_date = datetime.strptime(last, "%Y-%m-%d")

col=["id","time","temperature","humidity","wind_dir","wind_spd","cloudiness","pressure","precip_1h"]

df=pd.DataFrame(columns=col)
while start_date <= last_date:
    date = start_date.strftime("%Y-%m-%d")

    weathers_2 = requests.get(f'https://research-api.dershare.xyz/open-proc/cmpt-2022/weathers/2/observeds/{date}', headers={
                            'Authorization': f'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTSkRYc3lqVWdEaUt0aHN1Y241dERaIiwiaWF0IjoxNjY3ODA5NjQwLCJleHAiOjE2Njg3ODM2MDAsInR5cGUiOiJhcGlfa2V5In0.W3mUTBYStCT5pj5LX4RI67oIxdF-skc8xEfZ5nJdFGg'
                        }).json()
    
    df_weathers_2 = pd.DataFrame(columns = col)
    for idx, i in enumerate(weathers_2):
        for j in range(len(col)):
            df_weathers_2.loc[idx, col[j]] = i[col[j]]
    
    df=pd.concat([df,df_weathers_2])
    start_date += timedelta(days=1)
    
df.to_csv(f'{path}api_weathers_2.csv',index=False)

# weathers3
start_date = datetime.strptime(start, "%Y-%m-%d")
last_date = datetime.strptime(last, "%Y-%m-%d")

col=["id","time","temperature","humidity","wind_dir","wind_spd","precip_1h"]

df=pd.DataFrame(columns=col)
while start_date <= last_date:
    date = start_date.strftime("%Y-%m-%d")

    weathers_3 = requests.get(f'https://research-api.dershare.xyz/open-proc/cmpt-2022/weathers/3/observeds/{date}', headers={
                            'Authorization': f'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTSkRYc3lqVWdEaUt0aHN1Y241dERaIiwiaWF0IjoxNjY3ODA5NjQwLCJleHAiOjE2Njg3ODM2MDAsInR5cGUiOiJhcGlfa2V5In0.W3mUTBYStCT5pj5LX4RI67oIxdF-skc8xEfZ5nJdFGg'
                        }).json()
    
    df_weathers_3 = pd.DataFrame(columns = col)
    for idx, i in enumerate(weathers_3):
        for j in range(len(col)):
            df_weathers_3.loc[idx, col[j]] = i[col[j]]
    
    df=pd.concat([df,df_weathers_3])
    start_date += timedelta(days=1)
    
df.to_csv(f'{path}api_weathers_3.csv',index=False)

# forecasts_1 
hour = 1
col=['id',"fcst_time","time","temperature","humidity","dew_point","wind_dir","wind_spd","uv_idx","visibility","cloudiness","ceiling","precip_prob","precip_1h"]

df=pd.DataFrame(columns=col)
for i in range(1,22):
    id=i

    # 시작일, 종료일 datetime 으로 변환
    start_date = datetime.strptime(start, "%Y-%m-%d")
    last_date = datetime.strptime(last, "%Y-%m-%d")
    
    while start_date <= last_date:
        date = start_date.strftime("%Y-%m-%d")

        forecasts_1 = requests.get(f'https://research-api.dershare.xyz/open-proc/cmpt-2022/weathers/1/{id}/forecasts/{date}/{hour}', headers={
                                'Authorization': f'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTSkRYc3lqVWdEaUt0aHN1Y241dERaIiwiaWF0IjoxNjY3ODA5NjQwLCJleHAiOjE2Njg3ODM2MDAsInR5cGUiOiJhcGlfa2V5In0.W3mUTBYStCT5pj5LX4RI67oIxdF-skc8xEfZ5nJdFGg'
                            }).json()
        
        df_forecasts_1 = pd.DataFrame(columns = col)
        for idx, i in enumerate(forecasts_1):
            df_forecasts_1.loc[idx, 'id'] = id
            for j in range(1,len(col)):
                df_forecasts_1.loc[idx, col[j]] = i[col[j]]
        
        df=pd.concat([df,df_forecasts_1])
        start_date += timedelta(days=1)
    
df.to_csv(f'{path}api_forecasts_1.csv',index=False)

# forecasts_2
hour = 1
col=['id',"fcst_time","time","temperature","humidity","wind_dir","wind_spd","cloudiness","pressure","precip_1h"]

df=pd.DataFrame(columns=col)
for i in range(1,18):
    id=i
    # 시작일, 종료일 datetime 으로 변환
    start_date = datetime.strptime(start, "%Y-%m-%d")
    last_date = datetime.strptime(last, "%Y-%m-%d")
    
    while start_date <= last_date:
        date = start_date.strftime("%Y-%m-%d")

        forecasts_2 = requests.get(f'https://research-api.dershare.xyz/open-proc/cmpt-2022/weathers/2/{id}/forecasts/{date}/{hour}', headers={
                            'Authorization': f'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJTSkRYc3lqVWdEaUt0aHN1Y241dERaIiwiaWF0IjoxNjY3ODA5NjQwLCJleHAiOjE2Njg3ODM2MDAsInR5cGUiOiJhcGlfa2V5In0.W3mUTBYStCT5pj5LX4RI67oIxdF-skc8xEfZ5nJdFGg'
                        }).json()
        
        df_forecasts_2 = pd.DataFrame(columns = col)
        for idx, i in enumerate(forecasts_2):
            df_forecasts_2.loc[idx, 'id'] = id
            for j in range(1,len(col)):
                df_forecasts_2.loc[idx, col[j]] = i[col[j]]
        
        df=pd.concat([df,df_forecasts_2])
        start_date += timedelta(days=1)

df.to_csv(f'{path}api_forecasts_2.csv',index=False)
# -------------------------------데이터 수집 끝-----------------------------------------

gens=pd.read_csv(f'{path}api_pv_gens.csv')
#KwangMyeongsensor=pd.read_csv(f'{path}api_KwangMyeongsensor.csv')

gens.rename(columns={'pv_id':'id'},inplace=True)
gens['time'] = gens['time'].apply(lambda x : str(x).split('+')[0])
gens_process = gens
gens_process['time'] = pd.to_datetime(gens_process['time'])
gens_process = gens_process.sort_values(by = ['id','time']).reset_index(drop = True)
gens_process.to_csv(f'{path}gens_process.csv', index = False, encoding = 'utf-8-sig')

#envs = hour_avg('C:/Users/82106/Documents/PythonProject/11.data_analysis/cmpt2022_data1/envs.csv')
today_forecasts_1=hour_avg(f'{path}api_forecasts_1.csv')
today_forecasts_2=hour_avg(f'{path}api_forecasts_2.csv')
#today_forecasts_3=hour_avg(f'{path}api_forecasts_3.csv')
today_weathers_1=hour_avg(f'{path}api_weathers_1.csv')
today_weathers_2=hour_avg(f'{path}api_weathers_2.csv')
today_weathers_3=hour_avg(f'{path}api_weathers_3.csv')
#오늘날짜 가공 끝

today_pv_gens=gens_process
#api_KwangMyeongsensor=pd.read_csv(f'{path}api_KwangMyeongsensor.csv')

#api 기존파일 불러오기
weathers_1=pd.read_csv(f'{path}weathers_1.csv')
weathers_2=pd.read_csv(f'{path}weathers_2.csv')
weathers_3=pd.read_csv(f'{path}weathers_3.csv')
forecasts_1=pd.read_csv(f'{path}forecasts_1.csv')
forecasts_2=pd.read_csv(f'{path}forecasts_2.csv')
#forecasts_3=pd.read_csv(f'{path}forecasts_3.csv')
pv_gens=pd.read_csv(f'{path}pv_gens.csv')
#KwangMyeongsensor=pd.read_csv(f'{path}api_KwangMyeongsensor.csv')

weathers_1=pd.concat([weathers_1,today_weathers_1], axis=0)
weathers_2=pd.concat([weathers_2,today_weathers_2], axis=0)
weathers_3=pd.concat([weathers_3,today_weathers_3], axis=0)
forecasts_1=pd.concat([forecasts_1,today_forecasts_1], axis=0)
forecasts_2=pd.concat([forecasts_2,today_forecasts_2], axis=0)
#api_forecasts_3=pd.concat([api_forecasts_3,today_forecasts_3], axis=0)
pv_gens=pd.concat([pv_gens,today_pv_gens], axis=0)
#api_KwangMyeongsensor=pd.concat([api_KwangMyeongsensor,KwangMyeongsensor], axis=0)

#중복제거 하기
weathers_1 = weathers_1.drop_duplicates(['id','time'],keep='last')
weathers_2 = weathers_2.drop_duplicates(['id','time'],keep='last')
weathers_3 = weathers_3.drop_duplicates(['id','time'],keep='last')
forecasts_1 = forecasts_1.drop_duplicates(['id','time'],keep='last')
forecasts_2 = forecasts_2.drop_duplicates(['id','time'],keep='last')
pv_gens = pv_gens.drop_duplicates()

weathers_1.to_csv(f'{path}weathers_1.csv',index=False)
weathers_2.to_csv(f'{path}weathers_2.csv',index=False)
weathers_3.to_csv(f'{path}weathers_3.csv',index=False)
forecasts_1.to_csv(f'{path}forecasts_1.csv',index=False)
forecasts_2.to_csv(f'{path}forecasts_2.csv',index=False)
#forecasts_3.to_csv(f'{path}forecasts_3.csv')
pv_gens.to_csv(f'{path}pv_gens.csv',index=False)
#api_KwangMyeongsensor.to_csv(f'{today_path}api_KwangMyeongsensor.csv')

pv_site = pd.read_csv(f'{path}pv_sites.csv')

vin_df = pd.DataFrame(columns = ['id', 'time', 'amount', 'temperature', 'humidity', 'dew_point', 'wind_dir', 'wind_spd', 'uv_idx', 'visibility',
                                            'cloudiness', 'ceiling', 'pressure', 'precip_1h',
                                            'temperature_forcast', 'humidity_forcast', 'dew_point_forcast', 'wind_dir_forcast',
                                            'wind_spd_forcast', 'uv_idx_forcast', 'visibility_forcast', 'ceiling_forcast', 'cloudiness_forcast',
                                            'precip_1h_forcast'])
for id_ in list(range(0,21)):
    extractor = pv_site[pv_site['id'] == id_][['wth1_id','wth2_id','wth3_id','wth1_dist','wth2_dist','wth3_dist']]
    dist1 = extractor.loc[id_,'wth1_dist']
    dist2 = extractor.loc[id_,'wth2_dist']
    dist3 = extractor.loc[id_,'wth3_dist']
    dist_list = [dist1,dist2,dist3]
    # if dist_list.index(min(dist_list)) == 0:
    gens_weather1_forecast1_merge = make_weather1_case(id_, extractor, pv_gens, weathers_1, forecasts_1)
    vin_df = pd.concat([vin_df, gens_weather1_forecast1_merge], axis = 0)


# forecast2의 pressure_forecast 구하기
id_dic = {0:1, 1:2, 2:3, 3:4, 4:4, 5:5, 6:6, 7:7, 8:1, 9:8, 10:9, 11: 10, 12 : 9, 13 : 11, 14: 12, 15:13, 16:14, 17:15, 18:14, 19:16, 20:17}
vin_df['mapping_id'] = vin_df['id'].apply(lambda x : id_dic[x])
forecast2_ = forecasts_2[['id','time','pressure']]
forecast2_.columns = ['mapping_id', 'time', 'pressure_forecast']
df = pd.merge(vin_df, forecast2_, on = ['mapping_id','time'], how = 'inner')
df = df.sort_values(['id','time'])
df.drop(columns = 'mapping_id', inplace = True)

# capacity 갖다 붙이기
df = pd.merge(df, pv_site[['id','capacity']], on = 'id', how = 'inner')

#데이터 시간 보정
df['time'] = pd.to_datetime(df['time'], infer_datetime_format=True)
df['time'] = pd.DatetimeIndex(df['time']) + timedelta(hours=9)
df['time'] = df['time'].apply(str)
# 데이터 가공
df['year'] = df['time'].str.split('-').str[0].astype('int64')
df['month'] = df['time'].str.split('-').str[1].astype('int64')
df['day'] = df['time'].str.split(' ').str[0].str.split('-').str[-1].astype('int64')
df['hour'] = df['time'].str.split(' ').str[1].str.split(':').str[0].astype('int64')
df['season'] = df['month'].apply(lambda x : season_maker(x))
df.drop(columns = 'time', inplace = True)

df = df.drop_duplicates(['id','month','day','hour'],keep='last')

data_col=['id','year', 'month', 'day', 'hour', 'season', 'temperature', 'humidity', 'dew_point', 'wind_dir', 'wind_spd', 'uv_idx', 'visibility',
       'cloudiness', 'ceiling', 'pressure', 'precip_1h', 'temperature_forcast', 'humidity_forcast', 'dew_point_forcast', 'wind_dir_forcast',
       'wind_spd_forcast', 'uv_idx_forcast', 'visibility_forcast', 'ceiling_forcast', 'cloudiness_forcast', 'precip_1h_forcast',
       'pressure_forecast', 'capacity','amount']
df=df[data_col]

#가공한 api
df.to_csv(f'{path}perfect_api.csv',index=False)


df=pd.read_csv(f'{path}base_data.csv')
api_df=pd.read_csv(f'{path}perfect_api.csv')

df['year'] = df['time'].str.split('-').str[0].astype('int64')

#이상치 제거
df=df.drop(df[(df['dew_point']<-14) & (df['month']==10)].index)
df=df.drop(df[(df['time'].str.contains('2021-11-28'))].index)

df=df.drop(columns=['time'],axis=1)

df=df[data_col]

df=pd.concat([df,api_df],axis=0)

df.reset_index(inplace=True)
#이동평균 컬럼 생성
df=moving_avg_amount(df)
df=column_maker(df)

#태양 고도 매칭
sun=pd.read_csv('C:\\Users\\user\\Downloads\\태양고도.csv')
df = pd.merge(df, sun, on = ['month','day','hour'], how = 'inner')

# continueous time feature
df['cos_time'] = df['hour'].apply(lambda x : np.cos(2*np.pi*(x/24)))
df['sin_time'] = df['hour'].apply(lambda x : np.sin(2*np.pi*(x/24)))

df=df.sort_values(['id','year','month','day','hour'])

# df=df.drop(df[(df['year']==2022)&(df['month']==11)&(df['day']==16)].index)
# df=df.drop(df[(df['year']==2022)&(df['month']==11)&(df['day']==17)].index)

title=last.split('-')
df.to_csv(f'{path}train_{title[0]}{title[1]}{title[2]}.csv', index = False, encoding = 'utf-8-sig')