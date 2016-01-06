//Custom okapi script to calculate OKAPI score
def score = 0;
def doc_freq =0
def term_freq=0
for (t in terms){
	term_freq =_index[field][t].tf(); 
	doc_freq =_index[field][t].df(); 
        count=doc['dCount'].value
	calc=term_freq/(term_freq+0.5+(1.5*(count/avglen)))
	score=score+calc;
};
score;
