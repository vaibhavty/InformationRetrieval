// custom script to calculate BM25 score
def score = 0;
def doc_freq =0;
def term_freq=0;
def D=84678;
def k1=1.2
def k2=100
def b=0.75
for (t in terms){
	term_freq =_index[field][t].tf(); 
	doc_freq =_index[field][t].df(); 
        count=doc['dCount'].value
	tCount=terms.count(t)
	part1=log(D+0.5/doc_freq+0.5)
	part2=(term_freq + k1*term_freq)/(term_freq + k1*((1-b) + b*(count/avglen)))
	part3=(tCount+k2*tCount)/(tCount+k2)
	calc=part1*part2*part3
	score=score+calc;
};
score;
