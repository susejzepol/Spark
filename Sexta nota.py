"""
---------------------------------------------------------------------------------------------------------------------------------
                                        Cargar archivos CSV usando IO y CSV
---------------------------------------------------------------------------------------------------------------------------------
"""
RutaArchivo = "/susejzepol/CasosEjemplo/Spark/caso2-Archivocsv/Star_Wars_Episodio_IX.csv" #Cambiar la ruta en donde se tenga el archivo CSV

import csv
import io

def convertirlineas(linea):
    input = io.StringIO(linea)
    dictR = csv.DictReader(input,
        fieldnames = [
            "Pelicula",
            "Personaje",
            "Actor",
            "Anio",
            "Director",
            "Duracion"
        ])
    return next(dictR)

RDD = sc.textFile(RutaArchivo)
RDD.collect()
RDD_Resultado = RDD.zipWithIndex().filter(lambda x: x[1]>0).map(lambda x: convertirlineas(x[0]))

# Filtrar actores por J o L

RDD_Resultado.foreach(lambda fila: print(fila['Actor']))

# Filtrar actores por letra J
RDD_FiltroJ = RDD_Resultado.filter(lambda fila: (fila['Actor'].upper().find("J",0,1)) !=  -1)

# Filtrar actores por letra L
RDD_FiltroL = RDD_Resultado.filter(lambda fila: (fila['Actor'].upper().find("L",0,1)) !=  -1)

#Unir ambos RDDs
RDD_Final = RDD_FiltroJ.union(RDD_FiltroL)


"""
---------------------------------------------------------------------------------------------------------------------------------
                                                Guardar archivos CSV usando IO y CSV
---------------------------------------------------------------------------------------------------------------------------------
"""
RutaParaGuardarArchivos = "/susejzepol/CasosEjemplo/Spark/caso2-Archivocsv/datos_csv" 
# Probando error de CSV
RDD_Final.saveAsTextFile("csv://" + RutaParaGuardarArchivos) 

# Probando guardar archivo usando solo file
RDD_Final.saveAsTextFile("file://" + RutaParaGuardarArchivos)

# dictW.writerow(linea)
def escribirlineas(lineas):
    output =  io.StringIO()
    dictW  =  csv.DictWriter(output,
        fieldnames = [
            "Pelicula",
            "Personaje",
            "Actor",
            "Anio",
            "Director",
            "Duracion"
        ])
    print(lineas)
    for linea in lineas:
        dictW.writerow(linea)
    return [output.getvalue()]



RDD_Final.mapPartitions(lambda lineas: escribirlineas(lineas)).saveAsTextFile(RutaParaGuardarArchivos)
RDD_Final.foreach(lambda lineas: print(escribirlineas(lineas)))

"""
---------------------------------------------------------------------------------------------------------------------------------
                                        Usando Data Frames (PySpark - SQL)
---------------------------------------------------------------------------------------------------------------------------------
"""
#       CARGAR CSV 

#       USAR EL MÉTODO OPTION
RDD = spark.read.option("header", True).csv(RutaArchivo)
RDD.printSchema() #Obtenemos aquí un <class 'pyspark.sql.dataframe.DataFrame'>

#       USAR EL MÉTODO OPTIONS
RDD = spark.read.options(header = True).csv(RutaArchivo)
RDD.printSchema() #Obtenemos aquí un <class 'pyspark.sql.dataframe.DataFrame'>


#       ALGUNA TRANFORMACIÓN AQUÍ
RDD_Personajes = RDD.where("Personaje LIKE 'K%'")

#       GUARDAR CSV 
RutaParaGuardarArchivos = "/susejzepol/CasosEjemplo/Spark/caso2-Archivocsv/datos_csv2" 
RDD_Personajes.write.format("csv").mode('overwrite').save(RutaParaGuardarArchivos)


"""
    Ahora probar definir el schema
    inferSchema     –   infers the input schema automatically from data. 
                        It requires one extra pass over the data. If None is set, it uses the default value, false.
    enforceSchema   -   If None is set, true is used by default. 
                        Though the default value is true, it is recommended to disable the enforceSchema option to avoid incorrect results.

    NOTAS PARA ARTÍCULO: 
    
    Si nos fijamos detenidamente antes cuando usabamos el método printSchema() todas las columnas se mostraban como String()

    root
    |-- Pelicula: string (nullable = true)
    |-- Personaje: string (nullable = true)
    |-- Actor: string (nullable = true)
    |-- Anio: string (nullable = true)
    |-- Director: string (nullable = true)
    |-- Duracion: string (nullable = true)

    Sin embargo, algunas de las columnas que manejamos no son del tipo String. Esto sucede debido a que, por defecto la opción "enforceSchema"
    se encuentra activada al momento de cargar un archivo usando el método csv(). Lo recomendable sería, apagarlo si es que se conoce
    la estructura del archivo. Una vez que, se deshabilita esta opción es necesario definir un esquema para nuestro archivo. Para esto 
    vamos a definir un diccionario en python que contenga la estructara del archivo definiendo así, correctamente sus tipos de datos.
"""