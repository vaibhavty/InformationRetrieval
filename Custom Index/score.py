# This script calculates different scores using OKAPI, TFIDF, BM25, Jelmer, Laplace models
import pickle
from stemming.porter2 import stem
import re
import json
import math
import operator
import itertools


index_num= '3'
catTerm=dict()
docLen=dict()
docMap=dict()
unTerm=0
dCount=0
avg=0

'''
dCount:84679
avg:454
unTerm:165376
'''


# Load catalog data, document length data, into python dictionaries
def loadData():
	global catTerm
	global docLen
	global dCount
	global docMap
	global avg
	global unTerm
	fp= open(index_num+'_log/MainCatalog.log')
	lines=fp.read()
	lines=lines.split('\n')
	for line in lines:
		index=line.split(':')
		#print index[0]
		#print index[1]
		try:
			catTerm[index[0]] = int(index[1])
		except:
			pass	

	fp.close()
	
	fd = open(index_num+'_log/docLen.log','r')
	docLen = pickle.load(fd)
	fd.close()
	#print docLen
	fm = open(index_num+'_log/docMap.log','r')
	docMap = pickle.load(fm)
	fm.close()
	#print docMap[47354]
	fs = open(index_num+'_log/stat.log','r')
	dCount=fs.readline()
	dCount=int(dCount.split(':')[1])
	avg=fs.readline()
	avg=int(avg.split(':')[1])
	unTerm=fs.readline()
	unTerm=int(unTerm.split(':')[1])
	fs.close()


loadData()


# Calculate OKAPI score over all user queries
# Write relevent results to output file to evaluate result
def calculateOkapi(qList,qID):
	global catTerm
	global docLen
	global avg
	global dCount
	global unTerm
	
	Okapi=dict()
	fp = open(index_num+'_log/MainIndex.log','r')
	for term in qList:
		try:
			offset = catTerm[term]
			#print 'found'
		except:
			#print 'not found'
			continue

		fp.seek(offset,0)
		data=fp.readline()
		#print data		
		#data=json.loads(data)
		data=data.split(';')
		nDoc = len(data)
		for doc in data:
			current = doc.split(':')
			docNo = int(current[0])
			pos=current[1].split('|')
			tf = len (pos)
			'''
			print tf
			part = (tf+1)/float(docLen[docNo]) 
			#print part
			calc = math.log(part) 	
			print calc
			'''
			part1 = tf/(tf+0.5+(1.5*(docLen[docNo]/avg)))
			calc = part1 
			try:
				Okapi[docNo]=Okapi[docNo] + calc
			except:
				# when there is no previous calculation 
				Okapi[docNo]= calc

	
	fp.close()				
	writeRptFile(Okapi,'Okapi',qID)




# Calculate TF_IDF score over all user queries
# Write relevent results to output file to evaluate result
def calculateTF_IDF(qList,qID):
	global catTerm
	global docLen
	global avg
	global dCount
	TF_IDF=dict()
	fp = open(index_num+'_log/MainIndex.log','r')
	for term in qList:
		try:
			offset = catTerm[term]
			#print 'found'
		except:
			#print 'not found'
			continue

		fp.seek(offset,0)
		data=fp.readline()
		#print data		
		#data=json.loads(data)
		data=data.split(';')
		nDoc = len(data)
		for doc in data:
			#print 1
			current = doc.split(':')
			docNo = int(current[0])
			pos=current[1].split('|')
			tf = len (pos)
			part1 = tf/(tf+0.5+(1.5*(docLen[docNo]/avg)))
			part2 = math.log(dCount/nDoc)
			calc = part1 * part2
			try:
				TF_IDF[docNo]=TF_IDF[docNo] + calc
			except:
				# when there is no previous calculation 
				TF_IDF[docNo]= calc

	fp.close()
	writeRptFile(TF_IDF,'TF_IDF',qID)



# Calculate BM25 score over all user queries
# Write relevent results to output file to evaluate result
def calculateBM25(qList,qID):
	global catTerm
	global docLen
	global avg
	global dCount
	k1=1.2
	k2=100
	b=0.75
	BM25=dict()
	fp = open(index_num+'_log/MainIndex.log','r')
	for term in qList:
		try:
			offset = catTerm[term]
			#print 'found'
		except:
			#print 'not found'
			continue

		fp.seek(offset,0)
		data=fp.readline()
		#print data		
		#data=json.loads(data)
		data=data.split(';')
		nDoc = len(data)
		for doc in data:
			current = doc.split(':')
			docNo = int(current[0])
			pos=current[1].split('|')
			tf = len (pos)
			part1 = math.log((dCount+0.5)/(nDoc+0.5))
			part2 = (tf+(k1*tf))/(tf+(k1*((1-b) + (b * (docLen[docNo]/avg)))))
			tCount= qList.count(term)
			part3 = (tCount + (100 * tCount))/(tCount + k2)
			calc = part1 * part2 * part3 	
			try:
				BM25[docNo]=BM25[docNo] + calc
			except:
				# when there is no previous calculation 
				BM25[docNo]= calc

	fp.close()
	writeRptFile(BM25,'BM25',qID)


# Calculate Laplace score over all user queries
# Write relevent results to output file to evaluate result
def calculateLaplace(qList,qID):
	global catTerm
	global docLen
	global avg
	global dCount
	global unTerm
	
	Laplace=dict()
	docCount=dict()
	fp = open(index_num+'_log/MainIndex.log','r')
	total = len(qList)
	#print total
	for term in qList:
		try:
			offset = catTerm[term]
			#print 'found'
		except:
			#print 'not found'
			continue

		fp.seek(offset,0)
		data=fp.readline()
		#print data		
		#data=json.loads(data)
		data=data.split(';')
		nDoc = len(data)
		for doc in data:
			current = doc.split(':')
			docNo = int(current[0])
			pos=current[1].split('|')
			tf = len (pos)

			part = (tf+1)/float(docLen[docNo] + unTerm) 
			calc = math.log(part) 	
	
			
			try:
				Laplace[docNo]=Laplace[docNo] + calc
				docCount[docNo]= docCount[docNo] + 1
			except:
				# when there is no previous calculation 
				Laplace[docNo]= calc
				docCount[docNo]= 1
	
	
	for doc in docCount:
		diff = total - docCount[doc]
		part =  1/float(docLen[doc] + unTerm)	
		part = math.log(part)	
		Laplace[doc] = Laplace[doc] + (diff * part)


	fp.close()				
	writeRptFile(Laplace,'Laplace',qID)




# Calculate Jelinek score over all user queries
# Write relevent results to output file to evaluate result
def calculateJelinek(qList,qID):
	global catTerm
	global docLen
	global avg
	global dCount
	global unTerm
	L=0.4
	docCount=dict()
	total = len(qList)
	Jelinek=dict()
	fp = open(index_num+'_log/MainIndex.log','r')
	for term in qList:
		try:
			offset = catTerm[term]
			#print 'found'
		except:
			#print 'not found'
			continue

		fp.seek(offset,0)
		data=fp.readline()
		#print data		
		#data=json.loads(data)
		data=data.split(';')
		nDoc = len(data)
		ttf = 0
		for doc in data:
			current = doc.split(':')
			pos=current[1].split('|')
			tf = len (pos)
			ttf = ttf + tf

		for doc in data:
			current = doc.split(':')
			docNo = int(current[0])
			pos=current[1].split('|')
			tf = len (pos)
			part1 = L*(tf/float(docLen[docNo]))
			#print 'part1',part1
			part2 = (1-L)*(ttf/float(avg*dCount))
			#print 'part2',part2
			part = part1 + part2
			calc = math.log(part) 	
			#print calc
			try:
				Jelinek[docNo]=Jelinek[docNo] + calc
				docCount[docNo]= docCount[docNo] + 1
			except:
				# when there is no previous calculation 
				Jelinek[docNo]= calc
				docCount[docNo]=  1

	for doc in docCount:
		diff = total - docCount[doc]
		part = (1-L)*(ttf/float(avg*dCount))	
		part = math.log(part)	
		Jelinek[doc] = Jelinek[doc] + (diff * part)			
	
	fp.close()				
	writeRptFile(Jelinek,'Jelinek',qID)


# Calculate Proximity(RSVN) score over all user queries
# Write relevent results to output file to evaluate result
def calculateProximity(qList,qID):
	global catTerm
	global docLen
	global avg
	global dCount
	global unTerm
	b=0.9
	k=2
	k1=1.2
	k3=1000
	RSV=dict()
	wd=dict()
	qw=dict()
	RSVN = dict()
	fp = open(index_num+'_log/MainIndex.log','r')
	for term in qList:
		try:
			offset = catTerm[term]
			#print 'found'
		except:
			#print 'not found'
			continue

		fp.seek(offset,0)
		data=fp.readline()
		#print data		
		#data=json.loads(data)
		#nDoc = len(data)
		data=data.split(';')
		nDoc = len(data)
		for doc in data:
			#tf = len (doc[1])
			current = doc.split(':')
			docNo = int(current[0])
			pos=current[1].split('|')
			tf = len (pos)
			K = k * ((1-b) + b * (docLen[docNo]/float(avg)))
			w = (k1 + 1)* (tf/float(K+tf))
			tCount=qList.count(term)
			part1 = (tCount/float(k3+tCount))
			part2 = (math.log((dCount - nDoc)/float(nDoc)))
			qw[term] = part1 * part2
			#print 'part1',part1
			#print 'part2',part2
			
			calc = w * qw[term]

			try:
				RSV[docNo]=RSV[docNo] + calc
			except:
				# when there is no previous calculation 
				RSV[docNo]= calc


	combList = list(itertools.combinations(qList, 2))			
	print combList 
	docList = sorted(RSV.items(), key=operator.itemgetter(1),reverse = True)
	i=0
	'''
	for tpl in docList:
		TPRSV = 0
		if i==10:
				exit()
		print tpl
		i=i+1
	'''			
	for tpl in docList:
		TPRSV = 0
		if i==100:
				break
		i=i+1
		K = k * ((1-b) + b * (docLen[docNo]/avg))	
		for comb in combList:
			#print comb
			try:
				offset=catTerm[comb[0]]
			except:
				continue	
		
			fp.seek(offset,0)
			#data1 =json.loads(fp.readline())
			data1=fp.readline().split(';')
			try:
				offset = catTerm[comb[1]]
			except:
				continue
			fp.seek(offset,0)
			data2=fp.readline().split(';')

			pos1=[]
			pos2=[]
			for l1 in data1:
				l1=l1.split(':')
				if l1[0] == tpl[0]:
					#print l1[0]
					pos1 = l1[1].split('|')

			for l2 in data2:
				#print 'l2 is',l2[0]
				#print tpl[0]
				l2=l2.split(':')
				if l2[0] == tpl[0]:
					#print l2[1]
					pos2 = l2[1].split('|')

							
						
			tp=0		
			for p1 in pos1:
				for p2 in pos2:
					d=abs(int(p1) - int(p2))
					if d < 5 & d>0 :
						calc = 1/float(d*d)
						try:
							tp = tp + calc
						except:
							tp=calc	

							
			wd = (k1+1)*(tp/float(K+tp))
			wdq = wd * min(qw[comb[1]],	qw[comb[0]])			
							
			TPRSV = TPRSV + wdq

		RSVN[tpl[0]] = RSV[tpl[0]] + TPRSV	

	fp.close()


	writeRptFile(RSVN,'proximity1',qID)








def writeRptFile(docDict,indicator,qID):
	global docMap
	fName = index_num+'_log/'+indicator+'_'+index_num+'.rpt'
	docList = sorted(docDict.items(), key=operator.itemgetter(1),reverse = True)
	i=0
	fp = open(fName,'a')
	for tpl in docList:
		if i==100:
			break
		i=i+1
		#print tpl
		docNo = docMap[int(tpl[0])]
		#print docNo
		text=qID+' Q0 '+docNo+' '+str(i)+' '+str(tpl[1])+' Exp\n'
		#text=str(tpl)+'\n'
		fp.write(text)
	fp.close()

				
def calculateScore(query,qID):
	

	sfile=open('../AP_DATA/stoplist.txt','r')
	sList=sfile.read().split('\n')
	query=query.lower()
	qList=re.findall("\w+[\.?\w+]*",query)
	temp=list()
	for term in qList:
		if term.endswith('.') & term.count('.')==1 & (len(term)>1):
			term=term.replace('.','')
		if term.startswith('_') & term.count('_') ==1 & (len(term)>1):
			term = term.replace('_','')
		temp.append(term)
	
	qList = temp
	#print index_num
	if index_num=='4':
		#print 123
		qList=[i for i in temp if i not in sList]
		temp=list() 
		for term in qList:
			term=stem(term)
			temp.append(term)
		qList=temp

	if index_num=='3':
		temp=list()
		for term in qList:
			term=stem(term)
			temp.append(term)
		qList=temp

	if index_num=='2':
		qList=[i for i in temp if i not in sList]		
	
    # Call all functions to get output files
    calculateOkapi(qList,qID)
	calculateTF_IDF(qList,qID)
	calculateBM25(qList,qID)
	calculateLaplace(qList,qID)	
	calculateJelinek(qList,qID)
	calculateProximity(qList,qID)



# Load user query data
# Clean queries
# Calculate scores on all the queries using all models
def getQuery():
	qDict={}
	fp=open('../AP_DATA/query_desc.51-100.short.txt','r')
	
	data=fp.read()
	data=data.split('\n')
	for query in data:
		if query != '':
			query=query.split('.')
			qID = query[0]
			text = query[1].split()
			query = ' '.join(i for i in text [3:])
			calculateScore(query,qID)

			
getQuery()
	#calculateScore('allegations, or measures being taken against, corrupt public officials of any governmental jurisdiction worldwide.',index_num)
