"""     
    Realizando joins sobre un RDD de forma clave valor (k,v) en Spark
"""

A = [ ('a',1), ('b',2), ('b',3), ('c',3), ('d',5), ('e',3) ]
B = [ ('a',3), ('b',1), ('c',4), ('c',1), ('c',2), ('d',3) ]

#JOIN A & B
RDDA = sc.parallelize( A )
RDDB = sc.parallelize( B )
RDDS = RDDA.join(RDDB)
RDDS.collect()

#LEFTOUTERJOIN A & B
RDDS = RDDA.leftOuterJoin(RDDB)
RDDS.collect()

#RIGHTOUTERJOIN A & B
RDDS = RDDA.rightOuterJoin(RDDB)
RDDS.collect()

#FULLOUTERJOIN A & B
A = [ ('a',1), ('b',2), ('b',3), ('y',3), ('d',5), ('e',3) ]
B = [ ('a',3), ('b',1), ('c',4), ('c',1), ('x',2), ('d',3) ]

RDDA = sc.parallelize( A )
RDDB = sc.parallelize( B )

RDDS = RDDA.fullOuterJoin(RDDB)
RDDS.collect()

