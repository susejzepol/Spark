
"""
---------------------------------------------------------------------------------------------------------------------------------
                                        Invoke 'ptodos' from placeholder repository
---------------------------------------------------------------------------------------------------------------------------------
"""
import json
import requests

#Calling to users json from jsonplaceholder repository
response = requests.get("https://jsonplaceholder.typicode.com/todos")
ptodos = json.loads(response.text)

#type of 'ptodos' variable
type(ptodos)

#To view the elemens of the 'users' variables we can iterate
for todo in ptodos:
    print(todo)

"""
---------------------------------------------------------------------------------------------------------------------------------
                                            Load 'ptodos' response into a RDD
---------------------------------------------------------------------------------------------------------------------------------
"""
#To load 'ptodos' response we can do the following
RDD = sc.parallelize(ptodos)

#We can access to an element of the list to filter a specific result
ptodosTrue = RDD.filter(lambda todo: todo["completed"]).map(lambda js: json.dumps(js)) #encoding 'ptodosTrue' 


# Save 'ptodos' in to a file
ptodosTrue.saveAsTextFile("/susejzepol/CasosEjemplo/Spark/caso3-Archivojson/ptodostrueA")

# To save json with python can use the following code
with open("/susejzepol/CasosEjemplo/Spark/caso3-Archivojson/ptodos_file.json","w") as write_file:
...     json.dump(ptodos, write_file)

"""
---------------------------------------------------------------------------------------------------------------------------------
                                            Using pysql to handle json file
---------------------------------------------------------------------------------------------------------------------------------
"""

SqlCtx = SQLContext(sc) #instance of the module python sql

#Loads JSON files and returns the results as a DataFrame.
ptodosSql = SqlCtx.read.json("/susejzepol/CasosEjemplo/Spark/caso3-Archivojson/ptodostrueA/")

#The variable 'ptodosSql' is a dataframe
type(ptodosSql)

#to show rows in the dataframe, we can call collect() method or show() method
ptodosSql.show() #show() method by default, display the first twenty rows in a table format.

#For filter data in the dataframe, we can call filter() or where method. 
ptodosSql.filter(ptodosSql.userId == 9).show()

"""
---------------------------------------------------------------------------------------------------------------------------------
                                                Saving a json file
---------------------------------------------------------------------------------------------------------------------------------
"""

ptodos9 = ptodosSql.filter(ptodosSql.userId == 9)
#to save a dataframe in to disk, we can use saveAsTextFile() method.
ptodos9.toJSON().saveAsTextFile("/susejzepol/CasosEjemplo/Spark/caso3-Archivojson/ptodosnueve")