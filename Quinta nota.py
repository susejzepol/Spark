"""
 1) ARCHVIOS PLANOS
"""

# CARGAR ARCHIVOS PLANOS
# CARGAR UN SOLO ARCHIVO
RutaArchivo = "/susejzepoltemp/casos-ejemplos/spark/1.-Cargar-Guardar-archivos/caso 1 - Archivo plano/Star-Wars.txt"
RDD = sc.textFile(RutaArchivo)
RDD.collect() #Obtenemos aquí un <class 'pyspark.rdd.RDD'>

# GUARDAR ARCHIVOS PLANOS
RutaParaGuardarArchivos = "/susejzepoltemp/casos-ejemplos/spark/1.-Cargar-Guardar-archivos/caso 1 - Archivo plano/"
RDDResultado = (RDD.flatMap(lambda linea: linea.split(" ")).map(lambda palabra: (palabra,1)).reduceByKey(lambda a,b: a + b))
RDDResultado.saveAsTextFile("file://" + RutaParaGuardarArchivos + "Resultados")

# GUARDAR ARCHIVOS USANDO UN SOLO NODO
RDDResultado.coalesce(1).saveAsTextFile("file://" + RutaParaGuardarArchivos + "Resultados2")

# GUARDAR ARCHIVO COMPRIMIDO
RDDResultado.saveAsTextFile("file://" + RutaParaGuardarArchivos + "Resultados3","org.apache.hadoop.io.compress.GzipCodec")

RDDResultado.coalesce(1).saveAsTextFile("file://" + RutaParaGuardarArchivos + "Resultados4","org.apache.hadoop.io.compress.GzipCodec")

# CARGANDO VARIOS ARCHIVOS USANDO COMODINES EN TEXTFILE
RutaArchivo = "/susejzepoltemp/casos-ejemplos/spark/1.-Cargar-Guardar-archivos/caso 1 - Archivo plano/*.txt"
RDD = sc.textFile("file://" + RutaArchivo)
RDD.collect()

# CARGANDO VARIOS ARCHIVOS USANDO WHOLATEXTFILE
RutaArchivo = "/susejzepoltemp/casos-ejemplos/spark/1.-Cargar-Guardar-archivos/caso 1 - Archivo plano/"
RDD = sc.wholeTextFiles("file://" +RutaArchivo)
RDD.collect()