# This script applies page rank algorithm to rank web pages
__author__ = 'vaibhavtyagi'

import sys,math

d=0.85


# Load inlinks and outlink mapping to python objects
def readInputFile():
    inLinks = dict()
    countOutLinks = dict()
    count=0
    i=0
    try:
        fp = open('wt2g_inlinks.txt','r')
    except (OSError, IOError) as e:
        print 'Unable to read input file..!!'
        exit()
    content = fp.readlines()

    for line in content:
        #print i
        line = line.strip()
        links = line.split()
        main = links[0]
        links.remove(main)
        inLinks[main] = links

        for link in links:
            try:
                countOutLinks[link] +=1
            except KeyError as e:
                countOutLinks[link] =1

        count+=1
        #if i==20000:
        #    break

        #i+=1
    fp.close()
    return inLinks,countOutLinks,count





# This function uses entropy and perplexity to calculate rank of a page

def generatePageRank(inLinks,countOutLinks,count):
    global d
    i=0
    noOutList = list()
    pRank = dict()
    for link in inLinks.keys():
        if link not in countOutLinks.keys():
            noOutList.append(link)
    #print 'len',len(noOutList)
    for link in inLinks.keys():
        pRank[link] = 1/float(count);

    pPerplexity = 0.0
    cPerplexity = round(getPerplex(pRank),2)
    #print pPerplexity,' ', cPerplexity
    while (round(pPerplexity,4) != round(cPerplexity,4)):
        #print i
        i+=1
        rNew = dict()
        sRank = 0
        for link in noOutList:
            sRank += pRank[link]
        #print 'sRank',sRank
        for link in pRank.keys():
            #print link
            rNew[link] = (1-d)/float(count)
            #print rNew[link]
            rNew[link] += d * sRank/float(count)
            #print rNew[link]
            for inLink in inLinks[link]:
                try:
                    rNew[link] += d * pRank[inLink]/float(countOutLinks[inLink])
                except:
                    continue
            #print rNew[link]
        pRank = rNew
        #print pPerplexity,' ', cPerplexity
        pPerplexity = cPerplexity
        cPerplexity = getPerplex(pRank)
        #print pPerplexity,' ', cPerplexity
    return pRank



# Calculate perplexity
def getPerplex(pRank):
    total = 0
    for link in pRank:
        temp = pRank[link]
        #print temp
        total += temp * math.log(temp,2)
    return pow(2,(-1 * total))


# Generate output with all pages ranked in sequence
def printPageRank(pRank):
    fp = open('PageRank.out','w')
    for link,rank in sorted(pRank.iteritems(), key=lambda (i,j): (j,i), reverse=True):
        fp.write(link+'='+str(rank)+'\n')

inLinks,countOutLinks,count=readInputFile()
#print inLinks
#print countOutLinks

print count
pRank=generatePageRank(inLinks,countOutLinks,count)
printPageRank(pRank)
#print inLinks
#print countOutLinks

