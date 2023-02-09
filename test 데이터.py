from datetime import datetime, timedelta
import time
import requests
import pandas as pd
import numpy as np

'''
사용법
1. 다운받을 시작날짜와 마지막 날짜 설정
2. 다운받이 원하는 경로 입력(파일명만 제외한 경로)
path: 파일을 다운받을 경로
'''

#----------------------------------------설정--------------------------------------------
#2022년 10월 24일 ~ 2022년 11월 18일
start = "2022-11-8"
last = "2022-11-15"

path='C:\\Users\\user\\Desktop\\VS code\\test\\태양광 대회\\api_data\\'
#path='C:\\Users\\82104\\OneDrive\\바탕 화면\\태양광 대회\\api_data\\'

#---------------------------------------------------------------------------------------

#함수
def hour_avg(path_):
    df = pd.read_csv(path_)
    df=df.drop_duplicates(['id','time'],keep='last')
    df['time'] = df['time'].apply(lambda x : str(x).split(':')[0] + ':00:00')
    df_process = df.groupby(['id','time']).mean().reset_index()
    df_process['time'] = pd.to_datetime(df_process['time'])
    df_process = df_process.sort_values(by = ['id','time']).reset_index(drop = True)
    path_ = path_.split('/')[-1].split('.')[0] + '_afterProcess.csv'
    print(path_)
    #df_process.to_csv(path_, index = False, encoding = 'utf-8-sig')
    return df_process

def make_weather1_case(id_, extractor, gens,  weather1, forecast1):
    gens_weather1_merge = pd.merge(gens[gens['id'] == id_], weather1[weather1['id'] == extractor.loc[extractor.index[0], 'wth1_id']], on = 'time')
    gens_weather1_forecast1_merge = pd.merge(gens_weather1_merge, forecast1[forecast1['id'] == extractor.loc[extractor.index[0], 'wth1_id']], on = 'time')
    gens_weather1_forecast1_merge.drop(columns = ['id_y','id','precip_prob'], inplace = True)
    gens_weather1_forecast1_merge.columns = ['id', 'time', 'amount', 'temperature', 'humidity',
                                            'dew_point', 'wind_dir', 'wind_spd', 'uv_idx', 'visibility',
                                            'cloudiness', 'ceiling', 'pressure', 'precip_1h',
                                            'temperature_forcast', 'humidity_forcast', 'dew_point_forcast', 'wind_dir_forcast',
                                            'wind_spd_forcast', 'uv_idx_forcast', 'visibility_forcast', 'ceiling_forcast', 'cloudiness_forcast',
                                            'precip_1h_forcast']

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

def plustime(df):
    df['time'] = pd.to_datetime(df['time'], infer_datetime_format=True)
    df['time'] = pd.DatetimeIndex(df['time']) + timedelta(hours=9)
    df['time'] = df['time'].apply(str)
    return df

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

def make_col(df):
    df['month'] = df['time'].str.split('-').str[1].astype('int64')
    df['day'] = df['time'].str.split(' ').str[0].str.split('-').str[-1].astype('int64')
    df['hour'] = df['time'].str.split(' ').str[1].str.split(':').str[0].astype('int64')
    df['season'] = df['month'].apply(lambda x : season_maker(x))
    df.drop(columns = 'time', inplace = True)
    
    df['cos_time'] = df['hour'].apply(lambda x : np.cos(2*np.pi*(x/24)))
    df['sin_time'] = df['hour'].apply(lambda x : np.sin(2*np.pi*(x/24)))
    
    return df

# forecasts_1 
hour = 1

col=['id',"fcst_time","time","temperature","humidity","dew_point","wind_dir","wind_spd","uv_idx","visibility","cloudiness","ceiling","precip_prob","precip_1h"]

df=pd.DataFrame(columns=col)
for i in range(1,2):
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
    
#df.to_csv(f'C:\\Users\\whdgu\\Downloads\\api_forecasts_1.csv',index=False)
df.to_csv(f'{path}api_forecasts_1.csv',index=False)

# forecasts_2
hour = 1

col=['id',"fcst_time","time","temperature","humidity","wind_dir","wind_spd","cloudiness","pressure","precip_1h"]

df=pd.DataFrame(columns=col)
for i in range(1,2):
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


gens=pd.read_csv(f'{path}api_pv_gens.csv')
gens.rename(columns={'pv_id':'id'},inplace=True)
gens['time'] = gens['time'].apply(lambda x : str(x).split('+')[0])
gens_process = gens
gens_process['time'] = pd.to_datetime(gens_process['time'])
gens_process = gens_process.sort_values(by = ['id','time']).reset_index(drop = True)
gens_process.to_csv(f'{path}gens_process.csv', index = False, encoding = 'utf-8-sig')

forecast1=hour_avg(f'{path}api_forecasts_1.csv')
forecast2=hour_avg(f'{path}api_forecasts_2.csv')

forecast1 = forecast1.drop_duplicates(['id','time'],keep='last')
forecast2 = forecast2.drop_duplicates(['id','time'],keep='last')

gens= pd.read_csv(f'{path}gens_process.csv')
gens=gens[gens['id']==0]

#시간 맞추기 +9
forecast1=plustime(forecast1)
forecast2=plustime(forecast2)
gens=plustime(gens)

id_dic = {1:1}
forecast1['mapping_id'] = forecast1['id'].apply(lambda x : id_dic[x])
forecast2_ = forecast2[['id','time','pressure']]
forecast2_.columns = ['mapping_id', 'time', 'pressure_forecast']
forecast1 = pd.merge(forecast1, forecast2_, on = ['mapping_id','time'], how = 'inner')
forecast1 = forecast1.sort_values(['id','time'])
forecast1.drop(columns = 'mapping_id', inplace = True)

forecast1=pd.merge(forecast1,gens, on = ['time'], how = 'left')

forecast1=make_col(forecast1)

forecast1=forecast1.rename(columns={'id_x':'id'})

#이동평균 컬럼
forecast1.reset_index(drop=True,inplace=True)
forecast1=moving_avg_amount(forecast1)
forecast1.reset_index(drop=True,inplace=True)
forecast1=column_maker(forecast1)

sun=pd.read_csv('C:\\Users\\user\\Downloads\\태양고도.csv')
forecast1 = pd.merge(forecast1, sun, on = ['month','day','hour'], how = 'inner')

forecast1=forecast1.drop(columns=['id_y','amount'],axis=1)
forecast1['capacity']=472.39
forecast1['id']=0

data=forecast1[forecast1['day']==int(last.split('-')[2])]
title=last.split('-')
data.to_csv(f'{path}test_{title[0]}{title[1]}{title[2]}.csv',index=False)

