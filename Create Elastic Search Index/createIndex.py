# This script will load files from input dataset
# read, clean(remove stop words and extra content) and parse required content
# Index content using elastic search
import glob
from elasticsearch import Elasticsearch

count=0
sdict={}
ldict={}
es=Elasticsearch()

sfile=open('AP_DATA/stoplist.txt','r')
sList=sfile.read().split('\n')


for fName in glob.glob("AP_DATA/ap89_collection/*"):
	fp = open (fName,'r')
	i=0
	for line in fp.readlines():
		if '</TEXT>' in line:
			i=0
			cList=fContent.lower().split()
			content=' '.join([i for i in cList if i not in sList])
			sdict[docNo[1]]=content
			ldict[docNo[1]]=len(content.split())
		if '<DOCNO>' in line:
			docNo=line.split()
			fContent=''
		if i==1:
			fContent=fContent+line
		if '<TEXT>' in line:
			i=1
		

# Index all data to elastic search
for key in sdict:
	es.index(index="ap_dataset", doc_type="document", id=key, body={"text": sdict[key],"dCount" : ldict[key]})
