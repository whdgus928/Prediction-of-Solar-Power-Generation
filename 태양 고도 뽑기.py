import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By

driver_path ='C:\\Users\\user\\Desktop\\VS code\\106\\chromedriver.exe'

driver = webdriver.Chrome(driver_path)
driver.get('https://astro.kasi.re.kr/life/pageView/10')

time.sleep(0.5)
SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/div[2]/section/div/div[2]/form/fieldset/div/div[1]/div/label[3]')
SearchInput.click() 

#주소찾기 버튼
SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/div[2]/section/div/div[2]/form/fieldset/div/div[4]/div/button')
SearchInput.click() 
## enter Error')
    
time.sleep(1)

#새로운 창으로 전환
driver.switch_to.window(driver.window_handles[-1])

#위치 입력
SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/form[2]/div/div[1]/div/div[1]/fieldset/span/input[1]')
SearchInput.send_keys("충북 옥천군 청성면 장연길 172-1") # 남한 정중앙
SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/form[2]/div/div[1]/div/div[1]/fieldset/span/input[2]')
SearchInput.click() 
SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/form[2]/div/div[1]/div/div[2]/table/tbody/tr/td[2]/a/div/div/b')
SearchInput.click() 
SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/form[2]/div/div[1]/div/div[3]/div/a')
SearchInput.click()
SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/div[2]/section/div/div[2]/form/div/button[1]')
SearchInput.click() 

#원래 창으로 돌아오기
driver.switch_to.window(driver.window_handles[0])
# 로딩 기다리기
time.sleep(1)

#월, 시간 입력해서 고도 뽑기
idx=0
df=pd.DataFrame(columns=['월','일','시','고도'])
for 월 in range(1,13):
    month=[31,28,31,30,31,30,31,31,30,31,30,31]
    #달력버튼
    SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/div[2]/section/div/div[2]/form/fieldset/div/div[2]/div/div[1]/a')
    SearchInput.click()
    #월년버튼
    SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/div[3]/div[1]/table/thead/tr[1]/th[2]')
    SearchInput.click()
    #월 1~12
    SearchInput = driver.find_element(by=By.XPATH, value= f'/html/body/div[3]/div[2]/table/tbody/tr/td/span[{월}]')
    SearchInput.click()
    #일 아무곳이나 선택
    SearchInput = driver.find_element(by=By.XPATH, value= f'/html/body/div[3]/div[1]/table/tbody/tr[3]/td[4]')
    SearchInput.click()

    #시간
    for i in range(1,25):
        #시간 버튼
        SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/div[2]/section/div/div[2]/form/fieldset/div/div[2]/div/div[2]/div/button')
        SearchInput.click()
        #시간 고르기
        SearchInput = driver.find_element(by=By.XPATH, value= f'/html/body/div[2]/section/div/div[2]/form/fieldset/div/div[2]/div/div[2]/div/ul/li[{i}]/a')
        SearchInput.click()
        #검색하기 버튼
        SearchInput = driver.find_element(by=By.XPATH, value= '/html/body/div[2]/section/div/div[2]/form/div/button[1]')
        SearchInput.click()

        time.sleep(1)
        #고도표
        for j in range(1,month[월-1]+1):
            #표에서 날짜 시간 가져오기 idx 1~31
            SearchInput = driver.find_element(by=By.XPATH, value= f'/html/body/div[2]/section/div/div[2]/div[3]/table/tbody/tr[{j}]/td[1]')
            tmp=SearchInput.get_attribute('innerHTML')
            tmp = tmp.replace(" " , "")
            s=tmp.split('/')
            df.loc[idx,'월']=월
            df.loc[idx,'일']=int(s[0])
            df.loc[idx,'시']=int(s[1])
            
            #표에서 고도 가져오기 idx 1~31
            SearchInput = driver.find_element(by=By.XPATH, value= f'/html/body/div[2]/section/div/div[2]/div[3]/table/tbody/tr[{j}]/td[3]')
            tmp=SearchInput.get_attribute('innerHTML')
            s=tmp.split(' ')
            df.loc[idx,'고도']=s[0]
            #df 인덱스
            idx+=1

df=df.sort_values(['월','일','시'])
df.to_csv('C:\\Users\\user\\Downloads\\태양고도.csv',encoding='utf-8-sig',index=False)

df.head(40)