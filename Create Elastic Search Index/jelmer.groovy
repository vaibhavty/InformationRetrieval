// Custom groovy script to calculate jelmer score
def score = 0;
def doc_freq =0;
def term_freq=0;
def L=0.9
for (t in terms){
	term_freq =_index[field][t].tf(); 
	doc_freq =_index[field][t].df(); 
	ttf = _index[field][t].ttf();
	count=doc['dCount'].value
	calc=L*(term_freq/count) + (1-L)*(ttf/dlen); 
	score=score+log(calc);
};
score;
