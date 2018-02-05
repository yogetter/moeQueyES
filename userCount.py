import requests
import pandas as pd
import json 
from collections import Counter

def get_es_data(index, start_date, end_date, url):
    param_date = {"query":{ "bool":{ "must_not":{ "term":{ "action": "logout"}}, "filter":{"range":{"timestamp":{"gte":start_date, "lte":end_date} }}}}}
    #param_date = {"query":{"range":{"timestamp":{"gte":start_date, "lte":end_date}}}}
    r = requests.get(url + index+"/_search", json=param_date)
    length = r.json()["hits"]["total"]
    #param = {"size" : length, "query":{"range":{"timestamp":{"gte":start_date, "lte":end_date}}}}
    param = {"size": length, "query":{ "bool":{ "must_not":{ "term":{ "action": "logout"}}, "filter":{"range":{"timestamp":{"gte":start_date, "lte":end_date} }}}}}
    r = requests.get(url + index + "/_search?scroll=1m", json=param)
    #data = pd.DataFrame(pd.read_json(r.text)['hits']['hits'])
    data = r.json()['hits']['hits']
    #print "debug:", data
    return data

def extract_user(x):
    try:
        return x['user'].decode('unicode-escape') 
    except:
        return 'undefined'

def check_type(data):
    try:
        return int(data)
    except:
        return data
    
def match_data(source, count, name):
    Data = {'name': name, 'count': count, 'region': 'undefined', 'school': 'undefined', 'role': 'undefined'}
    for i in source.values:
        try:
            if i['user'] == name.encode('unicode-escape'):
                Data['region'] = check_type(i['region'])
                Data['role'] = check_type(i['role'])
                Data['school'] = check_type(i['school'])
        except:
            continue 
    return Data

def create_newdata(countData, data):
    newData = []
    for name in countData.index:
        if countData[name] >= 1:
            newData.append(match_data(data['_source'], countData[name], name))
    return newData

def split_group(totalDf):
    output = {'Day03':[], 'Day05':[], 'Day10':[], 'Day15':[], 'Day20':[], 'Day25':[], 'Day30':[]}
    tmp = totalDf.name.value_counts() 
    for i in tmp.index:
        if tmp[i] >= 1 and tmp[i] <= 3:
            output['Day03'].append(i)
        if tmp[i] >= 4 and tmp[i] <= 5:
            output['Day05'].append(i)
        elif tmp[i] >= 6 and tmp[i]  <= 10:
            output['Day10'].append(i)
        elif tmp[i] >= 11 and tmp[i] <= 15:
            output['Day15'].append(i)
        elif tmp[i] >= 16 and tmp[i] <= 20:
            output['Day20'].append(i)
        elif tmp[i] >= 21 and tmp[i] <= 25:
            output['Day25'].append(i)
        elif tmp[i] >= 26 and tmp[i] <= 30:
            output['Day30'].append(i)
    return output

def check_user_detail(data):
    for i in data['_source'].values:
        try:
            if i['user'].decode('unicode-escape') == u'黃文穗':
                print i['user'], i['action'], i['timestamp']
        except:
            continue
   

def gen_pd_data(data):
    return pd.read_json(json.dumps(data))

def check_only_login_user(action_data, login_data):
    action_data = gen_pd_data(action_data)                                                                                                                        
    action_data['user'] = action_data['_source'].apply(extract_user)                                                                                              
    login_data = gen_pd_data(login_data)                                                                                                                          
    login_data['user'] = login_data['_source'].apply(extract_user)            
    totalDf = pd.DataFrame(columns=login_data.columns)
    action_data = action_data.drop_duplicates(subset=['user'])
    for i in action_data['user']:
        login_data = login_data.drop(login_data[login_data['user'] == i].index)
    return login_data 

def count_only_login(dateFormat1, dateFormat2, timeStart, timeEnd):
    totalDf = pd.DataFrame(columns=['_id', '_index', '_score', '_source', '_type'])
    data1 = get_es_data("owncloud-action", dateFormat1 + '1' + "T" + timeStart, dateFormat2 + '31' + "T" + timeEnd, "http://10.1.64.82:9200/")
    data2 = get_es_data("owncloud-login", dateFormat1 + '1' + "T" + timeStart, dateFormat2 + '31' + "T" + timeEnd, "http://10.1.64.82:9200/")
    
    totalDf = check_only_login_user(data1, data2)
    totalDf['user'] = totalDf['_source'].apply(extract_user)
    newData = create_newdata(totalDf.user.value_counts(), totalDf)
    return pd.read_json(json.dumps(newData, ensure_ascii=False))

def count_everyday(dateFormat1, dateFormat2, timeStart, timeEnd):
    totalDf = pd.DataFrame(columns=['count', 'name', 'region', 'role', 'school'])
    for day in range(1,2):
        if day <= 9:
            data1 = gen_pd_data(get_es_data("owncloud-action", dateFormat1 + str(day)+ "T" + timeStart, dateFormat1 + str(day)+ "T" + timeEnd, "http://10.1.64.82:9200/"))
            #data2 = gen_pd_data(get_es_data("owncloud-login", dateFormat1 + str(day) + "T" + timeStart, dateFormat1 + str(day)+ "T" + timeEnd, "http://10.1.64.82:9200/"))
        else:
            data1 = gen_pd_data(get_es_data("owncloud-action", dateFormat2 + str(day)+ "T" + timeStart, dateFormat2 + str(day)+ "T" + timeEnd, "http://10.1.64.82:9200/"))
            #data2 = gen_pd_data(get_es_data("owncloud-login", dateFormat2 + str(day) + "T" + timeStart, dateFormat2 + str(day)+ "T" + timeEnd, "http://10.1.64.82:9200/"))
        #data = pd.concat([data1, data2])
        data = data1
        if data.empty :
            continue
        data['user'] = data['_source'].apply(extract_user)
        #check_user_detail(data)
        newData = create_newdata(data.user.value_counts(), data)
        tmpDf = pd.DataFrame(pd.read_json(json.dumps(newData, ensure_ascii=False)))
        totalDf = pd.concat([totalDf, tmpDf], ignore_index=True)
    return totalDf, data


Y = "2018"
M = "01"
dateFormat1 = Y+"-"+M+"-0"
dateFormat2 = Y+"-"+M+"-"
timeStart = "00:00:00+08:00"
timeEnd = "23:59:59+08:00"
totalDf = count_only_login(dateFormat1, dateFormat2, timeStart, timeEnd)
#totalDf, data = count_everyday(dateFormat1, dateFormat2, timeStart, timeEnd)
#output = split_group(totalDf)
