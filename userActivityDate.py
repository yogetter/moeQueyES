import pandas as pd
import requests
import time
import json 

def getEsData(index):
    start_date = "2017-12-01T00:00:00+08:00"
    end_date = "2017-12-31T23:59:59+08:00"
    param_date = {"query":{"range":{"timestamp":{"gte":start_date, "lte":end_date}}}}
    r = requests.get("http://10.1.64.82:9200/"+index+"/_search", json=param_date)
    length = r.json()["hits"]["total"]
    param = {"size" : length, "query":{"range":{"timestamp":{"gte":start_date, "lte":end_date}}}}
    #print length
    r = requests.get("http://10.1.64.82:9200/"+index+"/_search?scroll=1m", json=param)
    #print json.dumps(r.json()["hits"]["hits"])
    data = r.json()['hits']['hits']
    return data

def formatdate(data):
	return time.strftime('%Y-%m-%d', time.strptime(data, '%Y-%m-%dT%H:%M:%S+08:00'))

def collect(data):
	tmp = {'remote': 'undefined', 'timestamp': 'undefined', 'user': 'undefined', 'role': 'undefined', 'region': 'undefined', 'action': 'undefined', 
	'failed': 'undefined', 'success': 'undefined', 'agent': 'undefined', 'os': 'undefined'}
	for key in tmp:
		if key in data:
			if key == 'timestamp':
				tmp[key] = formatdate(data[key])
				#print tmp[key]
			else:
				tmp[key] = data[key]	
	return tmp
	
def splitData(data, newData):
	for source in data:
		newData.append(collect(source['_source']))

newData = []
splitData(getEsData("owncloud-action"), newData)
#print "============================"
splitData(getEsData("owncloud-login"), newData)

tmp = pd.DataFrame(pd.read_json(json.dumps(newData)))
names = tmp['user'].unique()
allUser = []
for name in names:
	date = []
	for n in newData:
		if n['user'] == name and n['timestamp'] not in date: 
			date.append(n['timestamp'])
	tmp = {'name': name, 'date': date}
	allUser.append(tmp)

print allUser 
