import os
import pandas as pd
import json
import requests
import datetime

# Get the current datetime
current_datetime = datetime.datetime.now()

# Format the datetime as "date_min"
formatted_time = current_datetime.strftime("%Y-%m-%d-%H-%M")
# fetch data from bybit


baseurl = "https://api2.bybit.com/fapi/beehive/public/v1/common/dynamic-leader-list?timeStamp=1687979058658&pageNo={}&pageSize={}&sortType=SORT_TYPE_DESC&dataDuration=DATA_DURATION_SEVEN_DAY&leaderTag=&code=&leaderLevel=&userTag=&sortField=LEADER_SORT_FIELD_SORT_ROI"

#in total 7370 users
total_page_cnt=1
pagesize=100
cur_page_cnt=1

df = pd.DataFrame()

while cur_page_cnt <= total_page_cnt:
    cur_url=baseurl.format(cur_page_cnt,pagesize)
    # print(cur_url)
    resp = requests.get(cur_url)
    
    # Check the HTTP status code
    if resp.status_code == requests.codes.ok:
        # Request was successful (status code 200)
        
        # Access the content
        content = resp.content.decode('utf-8')
        # print(content)
        content=json.loads(content)
        
        before_rows=df.shape[0]
        
        if cur_page_cnt==1:
            df=pd.json_normalize(content['result']['leaderDetails'])
            # df=df[["leaderUserId", "nickName", "countryCode", "metricValues", "userTag", "currentFollowerCount"]]
            
        else:
            cur_df=pd.json_normalize(content['result']['leaderDetails'])
            # cur_df=cur_df[["leaderUserId", "nickName", "countryCode", "metricValues", "userTag", "currentFollowerCount"]]
            
            df =pd.concat([df, cur_df], ignore_index=True)
            
            df.drop_duplicates(subset="nickName", keep="first", inplace=True)
        
        after_rows=df.shape[0]
        print("Request {}/{} Success, Added ".format(cur_page_cnt, total_page_cnt)+str(after_rows-before_rows)+"/"+str(pagesize)+" new records")
        if (after_rows-before_rows)==0:
            break
    else:
        # Request encountered an error
        print("Request {} was failed.".format(cur_page_cnt))
        print("Status Code:", resp.status_code)
        # Print the error message
        print("Error Message:", resp.text)
        break
        
    cur_page_cnt+=1

# encoding='utf-8-sig' is important as some nicknames are non-utf8 and may lead to missing values when dumping to csv
df.to_csv('{}_pgsize_{}_pagecnt_{}_total_{}.csv'.format(formatted_time, pagesize, total_page_cnt,df.shape[0]), index=False, na_rep='NaN',encoding='utf-8-sig')

# # if reading from local
# df=pd.read_csv('2023-06-28-20-10_pgsize_100_pagecnt_200_total_7370.csv')
# import ast
# df['metricValues']=df['metricValues'].apply(ast.literal_eval)

df[["MasterROI","MasterPNL","FollowerPNL","MasterWinningRate","StabilityScore","Followers"]] = pd.DataFrame(df.metricValues.tolist(), index= df.index)

df = df.drop('metricValues', axis=1)


df.to_csv('new.csv', index=False, na_rep='NaN',encoding='utf-8-sig')

follower_return = df['FollowerPNL'].apply(lambda x: float(x.replace(",", ""))).tolist()

master_winning_rate = df['MasterWinningRate'].apply(lambda x: round(float(x.rstrip('%')))).tolist()
print(master_winning_rate)


#aggregate the values
aggregate_returns={}
for wrate, ret in zip(master_winning_rate, follower_return):
    if wrate in aggregate_returns:
        aggregate_returns[wrate]+=ret
    else:
        aggregate_returns[wrate]=ret
        
import matplotlib.pyplot as plt

plt.bar(aggregate_returns.keys(), aggregate_returns.values())

# Set labels and title
plt.xlabel('Mater Winning Rate (%)')
plt.ylabel('Follower Return')
plt.title('Bybit Copytrade Follower Return vs Mater Winning Rate')
plt.ticklabel_format(useOffset=False, style='plain')
# Show the plot
plt.show()