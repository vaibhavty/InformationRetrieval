# This script creates a custom index to store all extracted tokens
# A catalog file to keep track for token positions in mainIndex file
# Load catalog file as dictionary in memory during begining of execution
# For any token get position from catalog dictionary
# Retrive all documents with count of occurance for each token from mainindex
#   using fetched position
# Calculate different scores on the documents. Sort them as per rank and output to a disk file
import os
import glob
import re
import collections
import json
import pickle



catTerm = list()
catIndex = dict()
dLen = dict()
docMap = dict()


# Clean old files from previous executions
def cleanFile(extension):
    gName='*.'+extension
    #print gName
    for fName in glob.glob(gName):
        try:
            os.remove(fName)
        except:
            print 'File not deleted'+fName
            pass


cleanFile('log')

# Match position for a token in catalog file and add details to main index file
# It appends to MainIndex file new token list details i.e document name, Term frequency etc
# Cover Base case, i.e a term is less than current 1st term in the index. Add at the begining of main index
# A term is more than all catalog file add at the end
# A term already exist in catalog file, Update its details in mainIndex file
# Run until all terms have been saved to index
def writeFile(termIndex,termList):
    global catIndex
    global catTerm
    tempTerm=list()
    tempIndex=dict()
    i=0
    j=0
    offset=0
    termList=sorted(termList)
    iLength = len(termList)
    jLength = len(catTerm)
    print 'jLength is',jLength
    iFile = '2_log/MainIndex.log'
    tFile = '2_log/TempIndex.log'
    fi = open(iFile,'a+')
    ft = open(tFile,'a')

    while (j<jLength) & (i<iLength):
        #print 'here'
        
        if catTerm[j] < termList[i]:
            t=catTerm[j]
            fi.seek(catIndex[t],0)
            data=fi.readline()
            tempIndex[t]=offset
            #print data
            #data=json.loads(data)
            #text=json.dumps(data)+'\n'
            text=data
            offset = offset + len(text)
            tempTerm.append(t)
            ft.write(text)
            j+=1
        

        elif catTerm[j] > termList[i]:
            t = termList[i]
            data=termIndex[t]
            #data=json.dumps(data)
            tempIndex[t]=offset
            text=data+'\n'
            offset = offset + len(text)
            tempTerm.append(t)
            ft.write(text)
            i+=1
            
        else :
            #print 'equal'
            t = termList[i]
            data1 = termIndex[t]
            #data1=json.dumps(data1)
            #data1=json.loads(data1)

            fi.seek(catIndex[t],0)
            data2=fi.readline()
            #data2=json.loads(data2)
            
            data=data1+';'+data2
            #print data
            #data=json.dumps(data)
            tempIndex[t]=offset
            text=data
            offset = offset + len(text) 
            tempTerm.append(t)
            ft.write(text)
            i+=1
            j+=1
            


    if i==iLength:
        while j<jLength:
            t=catTerm[j]
            fi.seek(catIndex[t],0)
            data=fi.readline()
            #data=json.loads(data)
            #text=json.dumps(data)+'\n'
            text=data
            tempIndex[t]=offset
            offset = offset + len(text) 
            tempTerm.append(t)
            ft.write(text)
            j+=1
               
                
    if j == jLength:  
        #print 1              
        while i<iLength:
            #print i
            t = termList[i]
            data=termIndex[t]
            #data=json.dumps(data)
            #print offset
            tempIndex[t]=offset
            text=data+'\n'
            #print termIndex[t]
            #print text
            #print 'text len',len(text)
            offset = offset + len(text)
            tempTerm.append(t)
            ft.write(text)
            i+=1
            
                        
    os.rename('2_log/TempIndex.log','2_log/MainIndex.log')  
    catIndex = tempIndex
    catTerm=tempTerm
    #print tempTerm
    #print catTerm
    ft.close()
    fi.close()



# Append a term position to catalog file
def writeCatalog():
    global catIndex
    global catTerm
    fc=open('2_log/MainCatalog.log','a')
    for term in catTerm:
        fc.write(term+':'+str(catIndex[term])+'\n') 



# Store basic stats like doclen in seperate disk file
def writeStats(dLen,dCount):
    global catTerm
    import pickle
    with open('2_log/docLen.log', 'w') as handle:
        pickle.dump(dLen, handle)


    with open('2_log/docMap.log', 'w') as handle:
        pickle.dump(docMap, handle)    

    sum=0
    for term in dLen:
        sum+=dLen[term]
    
    avg=sum/dCount

    fs = open('2_log/stat.log','a')
    fs.write('dCount:'+str(dCount)+'\n')
    fs.write('avg:'+str(avg)+'\n')
    fs.write('unTerm:'+str(len(catTerm)))



# Generate index over all the documents
# Clean input data and filter tokens 
def genIndex():
    k=0
    j=0
    dCount=0
    termIndex=dict()
    termList=list()
    dLen=dict()

    sfile=open('../AP_DATA/stoplist.txt','r')
    sList=sfile.read().split('\n')
    for fName in glob.glob("../AP_DATA/ap89_collection/*"):
        k+=1
        print fName
        fp = open(fName,"r")
        text = fp.readlines()
        i=0
        fContent = ""
        for line in text:
            if "<DOCNO>" in line:
                j+=1
                docNo = line.split()[1]
            if "</TEXT>" in line:
                i=0    
            if i == 1:
                fContent += line
            if "<TEXT>" in line:
                i=1     
            
            if "</DOC>" in line:
                dCount+=1
                docMap[dCount]=docNo
                fContent = fContent.lower()
                terms = re.findall("\w+[\.?\w+]*",fContent)
                dLen[dCount] = len(terms)
                temp=list()
                for term in terms:
                    if term.endswith('.') & term.count('.')==1 & (len(term)>1):
                        term=term.replace('.','')
                    
                    if term.startswith('_') & term.count('_') ==1 & (len(term)>1):
                        term = term.replace('_','')
                    #term=stem(term)
                    temp.append(term)    

                tfDict = collections.Counter(temp)         
                uTerms = [i for i in temp if i not in sList]    
                uTerms=set(uTerms)

                for term in uTerms:
                    tf = tfDict[term]
                    #pos=[]
                    offset = 0
                    '''
                    for i in range(tf):
                        index = temp.index(term,offset)
                        pos.append(index)
                        offset = index + 1
                    '''
                    pos = ''
                    for i in range(tf):
                        index = temp.index(term,offset)
                        if pos=='':
                            pos=str(index)
                        else:
                            pos=pos+'|'+str(index)
                        offset = index + 1
                    #print pos        
                    entry =str(dCount)+':'+pos
                    #print entry
                    if termIndex.get(term) == None:
                        termIndex[term] = entry
                        termList.append(term)
                    else:
                        termIndex[term]=termIndex[term]+';'+entry
                            
                i = 0
                fContent = ""
                #print termIndex
                #if j==5:
                #   j=0 
                #   break
                if dCount%10==0 :
                    writeFile(termIndex,termList)
                    termIndex=dict()
                    termList=list()

        #if k==1:                    
        #    break            
        fp.close()
    #print sorted(termList) 
    #print termIndex['000']       
    writeFile(termIndex,termList)    
    #writeFile(termIndex,j)        
    writeCatalog()
    writeStats(dLen,dCount)

genIndex()  


    