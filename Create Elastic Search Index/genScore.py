# This script will load user's query data
# Clean query to get tokens
# Search related documents from elastic search index
# Score them using groovy scripts placed in config/scripts
#   directory of elastic search
# Generate custom ranking using OKAPI, TFIDF, BM25, Laplace, Jelmer models
import glob
import operator
import os
import math
from elasticsearch import Elasticsearch
from elasticsearch import client


dLen=0
avDLen=0
lDict={}
sDict={}
slDict={}
V=0

es=Elasticsearch(timeout=200)
cl=client.IndicesClient(es)



# Search for tokens in elastic search index
# pass the script name to choose model for output top 100 results
def searchTerm(qry,wlist,scrpt,dlen,avglen):
	
	res=es.search(
		index='ap_dataset', 
		doc_type='document',
		#explain= True,
		body= { "query": {
    				"function_score": {
      					"boost_mode": "replace", 
      					"query": {
        					"match": {
          						"text": qry
        						}
      						},
      					"script_score": {
        					"params": {
          						"field": "text",
          						"terms": wlist,
          						"dlen" : dlen,
        						"avglen" : avglen
        					},
        				"script": scrpt,
        				"lang": "groovy"
      					}
    				}
  				}
  			},	
		size=100,
		analyzer= 'my-english'
		#timeout=180
		)
	return res


'''
body= {
			'query' : {'match' : {'text' : "alleg"}}},
		analyzer = 'my_english',
'''		


# Load query data from input files
def loadQuery():
	qDict={}
	fp=open('AP_DATA/query_desc.51-100.short.txt','r')
	
	data=fp.read()
	data=data.split('\n')
	for item in data:
		if item != '':
			item=cleanQuery(item)
			item=item.split()
			qDict[item[0]]=' '.join(i for i in item[2:])
	return qDict	


# Clean query data to get tokens
def cleanQuery(q):
	q=q.replace("Document will"," ")
	q=q.replace("\'"," ")
	q=q.replace("."," ")
	q=q.replace(","," ")
	sfile=open('AP_DATA/stoplist.txt','r')
	sList=sfile.read().split('\n')
	sfile.close()
	qList=q.split()
	query=' '.join([i for i in qList if i not in sList])
	#print query
	return query


# Clean old files from disk
def cleanup():
	for fName in glob.glob("*"):
		if '_out.log' in fName:
			try:
				os.remove(fName) 
			except:	
				print 'Unable to clean files...!'


# Get document lenth from the disk file
def loadDLength():
	global lDict
	fp=open('DocLength.txt','r')
	for line in fp.readlines():
		l=line.split('|')
		lDict[l[0]]=int(l[1])
	avDLen=304
	return avDLen


# Parse response from elastic search to get docId and score
def parseResponse(res,function,qID,):
	#mDoc=list()
	rList=list()
	global lDict
	for hit in res['hits']['hits']:
		docId=hit['_id']
		score=hit['_score']
		tpl=(docId,score)
		rList.append(tpl)
	generateFile(rList,function,qID)


# Generate output file with score and relevance sequence
def generateFile(rList,function,qID):
	i=0
	fName=function+'_out.log'
	fp=open(fName,'a')
	#print sorted_tpl
	for tpl in rList:
		i=i+1
		text=qID+' Q0 '+str(tpl[0])+' '+str(i)+' '+str(tpl[1])+' Exp\n'
		#text=str(tpl)+'\n'
		fp.write(text)
	fp.close()


def getTotalCount():
	len=0
	res=es.search(
		index='ap_dataset', 
		doc_type='document',
		body= {
			'query' : {'match_all' :{}}},
		analyzer = 'my_english',
		size=100000)
	for t in res['hits']['hits']:
		len=len+t['_source']['dCount']

	return len	


# Execute for all user queries and all models 
def mainFunc():
	cleanup()
	#avDLen=getLen()
	dlen=getTotalCount()
	avglen = dlen/84678 
	print avglen
	qDict=loadQuery()
	for key in qDict:
		wlist=list()
		ana=cl.analyze(index='ap_dataset',text=qDict[key] ,analyzer='my_english')
		for t in ana['tokens']:
			wlist.append(t['token'])
		res=searchTerm(qDict[key],wlist,'Okapi',dlen,avglen)
		parseResponse(res,'Okapi',key)
		res=searchTerm(qDict[key],wlist,'TFIDF',dlen,avglen)
		parseResponse(res,'TFIDF',key)
		res=searchTerm(qDict[key],wlist,'BM25',dlen,avglen)
		parseResponse(res,'BM25',key)
		res=searchTerm(qDict[key],wlist,'laplace',dlen,avglen)
		parseResponse(res,'laplace',key)
		res=searchTerm(qDict[key],wlist,'jelmer',dlen,avglen)
		parseResponse(res,'jelmer',key)
		print key

mainFunc()