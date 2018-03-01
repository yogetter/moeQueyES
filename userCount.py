import requests
import pandas as pd
import json 
from collections import Counter

def get_es_data(index, start_date, end_date, url):
    param_date = {'query':{ 'bool':{ 'must_not':{ 'term':{ 'action': 'logout'}}, 'filter':{'range':{'timestamp':{'gte':start_date, 'lte':end_date} }}}}}
    #param_date = {'query':{'range':{'timestamp':{'gte':start_date, 'lte':end_date}}}}
    r = requests.get(url + index+'/_search', json=param_date)
    length = r.json()['hits']['total']
    #param = {'size' : length, 'query':{'range':{'timestamp':{'gte':start_date, 'lte':end_date}}}}
    param = {'size': length, 'query':{ 'bool':{ 'must_not':{ 'term':{ 'action': 'logout'}}, 'filter':{'range':{'timestamp':{'gte':start_date, 'lte':end_date} }}}}}
    r = requests.get(url + index + '/_search?scroll=1m', json=param)
    data = r.json()['hits']['hits']
    #print 'debug:', data
    return data

def extract_data(data, param):
    if param not in data:
        return 'undefined'
       
    if param == 'user':
            return data['user'].decode('unicode-escape') 
    else:
        return data[param]

def check_type(data):
    try:
        return int(data)
    except:
        return data
    
def match_data(source, count, name):
    data = {'name': name, 'count': count, 'region': 'undefined', 'school': 'undefined', 'role': 'undefined'}
    tmp = source[source.loc[:,'user'] == name]['_source'].values[0]

    for i in {'region', 'role', 'school'}:
        if i not in tmp:
            continue
        data[i] = check_type(tmp[i])

    return data

def create_newdata(countData, data):
    newData = []

    for name in countData.index:
        if countData[name] >= 1:
            newData.append(match_data(data, countData[name], name))

    return gen_pd_data(newData)

def split_group(totalDf):
    output = {'Day03':[], 'Day05':[], 'Day10':[], 'Day15':[], 'Day20':[], 'Day25':[], 'Day30':[]}

    for i in totalDf.index:
        tmp = totalDf.loc[i]
        if tmp['count'] >= 1 and tmp['count'] <= 3:
            output['Day03'].append(tmp['name'])
        elif tmp['count'] >= 4 and tmp['count'] <= 5:
            output['Day05'].append(tmp['name'])
        elif tmp['count'] >= 6 and tmp['count']  <= 10:
            output['Day10'].append(tmp['name'])
        elif tmp['count'] >= 11 and tmp['count'] <= 15:
            output['Day15'].append(tmp['name'])
        elif tmp['count'] >= 16 and tmp['count'] <= 20:
            output['Day20'].append(tmp['name'])
        elif tmp['count'] >= 21 and tmp['count'] <= 25:
            output['Day25'].append(tmp['name'])
        elif tmp['count'] >= 26 and tmp['count'] <= 30:
            output['Day30'].append(tmp['name'])

    return output

def check_user_detail(data, name):
    if 'user' in data and data['user'].decode('unicode-escape') == name:
        print data['user'].decode('unicode-escape'), data['action'], data['timestamp']

def gen_pd_data(data):
    return pd.read_json(json.dumps(data, ensure_ascii=False))

def check_only_login_user(action_data, login_data):
    action_data = gen_pd_data(action_data)                                                                                                                        
    action_data['user'] = action_data['_source'].apply(extract_data, args=('user',))
    login_data = gen_pd_data(login_data)
    login_data['user'] = login_data['_source'].apply(extract_data, args=('user',))
    action_data = action_data.drop_duplicates(subset=['user'])

    for i in action_data['user']:
        login_data = login_data.drop(login_data[login_data['user'] == i].index)

    return login_data 

def count_only_login(dateFormat1, dateFormat2, timeStart, timeEnd):
    data1 = get_es_data('owncloud-action', dateFormat1 + '1T' + timeStart, dateFormat2 + '31T' + timeEnd, 'http://10.1.64.82:9200/')
    data2 = get_es_data('owncloud-login', dateFormat1 + '1T' + timeStart, dateFormat2 + '31T' + timeEnd, 'http://10.1.64.82:9200/')
    totalDf = check_only_login_user(data1, data2)
    newData = create_newdata(totalDf.user.value_counts(), totalDf)
    return newData

def count_everyday(dateFormat1, dateFormat2, timeStart, timeEnd):
    totalDf = pd.DataFrame(columns=['name'])
    esTmp = get_es_data('owncloud-action', dateFormat1 + '1T' + timeStart, dateFormat2 + '31T' + timeEnd, 'http://10.1.64.82:9200/')
    data = gen_pd_data(esTmp)
    data['time'] = data['_source'].apply(extract_data, args=('timestamp',))
    data['user'] = data['_source'].apply(extract_data, args=('user',))
   
    for day in range(1,32):
        if day <= 9:
            totalDf = pd.concat([pd.DataFrame({'name':data[data['time'].str.contains('0'+str(day)+'T')]['user'].value_counts().index}), totalDf])
        else:
            totalDf = pd.concat([pd.DataFrame({'name':data[data['time'].str.contains(str(day)+'T')]['user'].value_counts().index}), totalDf])
    
    newDf = create_newdata(totalDf['name'].value_counts(), data)
    return newDf, data

Y = '2018'
M = '02'
dateFormat1 = Y+'-'+M+'-0'
dateFormat2 = Y+'-'+M+'-'
timeStart = '00:00:00+08:00'
timeEnd = '23:59:59+08:00'
totalDf = count_only_login(dateFormat1, dateFormat2, timeStart, timeEnd)
#totalDf, data = count_everyday(dateFormat1, dateFormat2, timeStart, timeEnd)
#output = split_group(totalDf)
