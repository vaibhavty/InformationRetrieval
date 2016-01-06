// Custom groovy script to calculate laplace score
def score = 0;
def doc_freq =0;
def term_freq=0;
def V=178050;
for (t in terms){
	term_freq =_index[field][t].tf(); 
	doc_freq =_index[field][t].df(); 
        count=doc['dCount'].value;
	calc=(term_freq+1)/(count+V);
	score=score+ log(calc);
};
score;
