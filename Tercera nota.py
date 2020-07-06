import re

not_useful_words = ['','the','this','but','s','very','much','even','game','ads','m','t','get','can','would','nan','i','it','days','app','10 ','best','foods','for','you']

def reviews (line):
    aux = []
    for i in range(len(line)):
        if i == 1:
            aux.append(line[i])
    return aux

src = "/susejzepoltemp/Kaggle_Repos/1) Google Play datasets/data/googleplaystore_user_reviews.csv"

RDD_ORIGEN  = sc.textFile(src)

RDD_MEDITATIONAPP = RDD_ORIGEN.filter(lambda str: "10 Best Foods for You" in str)

RDD_REVIEWS = RDD_MEDITATIONAPP.map(lambda aux: aux.split(",")).flatMap(lambda aux: reviews(aux))

#SOLO PALABRAS
RDD_PALABRAS = RDD_REVIEWS.flatMap(lambda aux: re.compile("\W").split(aux)).map(lambda palabra: palabra.lower()).filter(lambda aux: aux not in not_useful_words)
               
#HISTOGRAMA
RDD_HISGRAMA = (RDD_PALABRAS.map(lambda palabra : (palabra,1)).reduceByKey(lambda x,y: x+y))

RDD_MASFRECUENTES = (RDD_HISGRAMA.takeOrdered(10,lambda tupla: -1*tupla[1]))

for p in RDD_MASFRECUENTES:
	print (p[0] + ": %d" % p[1])
