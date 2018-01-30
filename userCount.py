import requests
import pandas as pd
import json 
from collections import Counter

def getEsData(index):
    start_date = "2017-12-01"
    end_date = "2017-12-31"
    param_date = {"query":{"range":{"timestamp":{"gte":start_date, "lte":end_date}}}}
    r = requests.get("http://10.1.64.82:9200/"+index+"/_search", json=param_date)
    length = r.json()["hits"]["total"]
    param = {"size" : length, "query":{"range":{"timestamp":{"gte":start_date, "lte":end_date}}}}
    #print length
    r = requests.get("http://10.1.64.82:9200/"+index+"/_search?scroll=1m", json=param)
    #print json.dumps(r.json()["hits"]["hits"])
    data = pd.DataFrame(pd.read_json(r.text)['hits']['hits'])
    return data

def extract_user(x):
    try:
        return x['user'].decode('unicode-escape') 
    except:
        return 'undefined'

def extract_region(x):
    try:
        return x['region']
    except:
        return 'undefined'


def check_type(data):
	if type(data) == str:
		return str
	else:
		return int(data)

def match_data(source, count, name):
	Data = {'name': name, 'count': count, 'region': 'undefined', 'school': 'undefined', 'role': 'undefined'}
	#print name
	for i in source.values:
		try:
			if i['user'] == name.encode('unicode-escape'):
				Data['region'] = check_type(i['region'])
				Data['school'] = check_type(i['school'])
				Data['role'] = check_type(i['role'])
				break
		except:
			continue

	return Data

def create_newdata(countData, allData):
	newData = []
	for name in countData.index:
		if countData[name] > 300
			newData.append(match_data(allData['_source'], countData[name], name))
	return newData

data1 = getEsData("owncloud-action")
#print "============================"
data2 = getEsData("owncloud-login")

f = [data1,data2]
allData = pd.concat(f)

allData['user']=allData['_source'].apply(extract_user)
allData['region']=allData['_source'].apply(extract_region)
#print(allData.user.value_counts().reset_index().rename(columns={'index':'user', 'user':'count'}).head(n=15))
newData = create_newdata(allData.user.value_counts(), allData)

print newData
