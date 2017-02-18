
# coding: utf-8

# ### Assignment 2
# Welcome to Assignment 2. This will be fun. It is the first time you actually access external data from ApacheSpark. 
# 
# Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook.
# 
# ##### Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.

# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.

# In[24]:

#Please don't modify this function
def readDataFrameFromCloudant(host,user,pw,database):
    sparkSession = SQLContext.getOrCreate(sc).sparkSession

    cloudantdata=sparkSession.read.format("com.cloudant.spark").     option("cloudant.host",host).     option("cloudant.username", user).     option("cloudant.password", pw).     load(database)

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# This is the first function you have to implement. You are passed a dataframe object. We've also registered the dataframe in the ApacheSparkSQL catalog - so you can also issue queries against the "washing" table using "spark.sql()". Hint: To get an idea about the contents of the catalog you can use: spark.catalog.listTables().
# So now it's time to implement your first function. You are free to use the dataframe API, SQL or RDD API. In case you want to use the RDD API just obtain the encapsulated RDD using "df.rdd". You can test the function by running one of the three last cells of this notebook, but please make sure you run the cells from top to down since some are dependant of each other...

# In[30]:

def count(df,spark):
    #TODO Please enter your code here
    return df.count()


# No it's time to implement the second function. Please return an integer containing the number of fields. The most easy way to get this is using the dataframe API. Hint: You might find the dataframe API documentation useful: http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame

# In[35]:

def getNumberOfFields(df,spark):
    #TODO Please enter your code here
    return len(df.columns)


# Finally, please implement a function which returns a (python) list of string values of the field names in this data frame. Hint: Just copy&past doesn't work because the auto-grader will create a random data frame for testing, so please use the data frame API as well. Again, this is the link to the documentation: http://spark.apache.org/docs/latest/api/python/pyspark.sql.html#pyspark.sql.DataFrame

# In[40]:

def getFieldNames(df,spark):
    #TODO Please enter your code here
    return df.columns


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# So after this block you can basically do what you like since the auto-grader is not considering this part of the notebook

# Now it is time to connect to the cloudant database. Please have a look at the Video "Overview of end-to-end scenario" of Week 2 starting from 6:40 in order to learn how to obtain the credentials for the database. Please paste this credentials as strings into the below code

# In[41]:

#TODO Please provide your Cloudant credentials here
hostname = "9274dedf-1881-427d-8445-de0970d541e3-bluemix.cloudant.com"
user = "9274dedf-1881-427d-8445-de0970d541e3-bluemix"
pw = "94a4e87e5aef7bf6959c2666f303acee7f36da0ebc6701b6dba648ce89b2dabc"
database = "washing" #as long as you didn't change this in the NodeRED flow the database name stays the same
cloudantdata=readDataFrameFromCloudant(hostname, user, pw, database)


# The following cell can be used to test your count function

# In[42]:

count(cloudantdata,spark)


# The following cell can be used to test your getNumberOfFields function

# In[43]:

getNumberOfFields(cloudantdata,spark)


# The following cell can be used to test your getFieldNames function

# In[44]:

getFieldNames(cloudantdata,spark)


# Congratulations, you are done. So please export this notebook as python script using the "File->Download as->Python (.py)" option in the menue of this notebook. This file should be named "assignment2.1.py" and uploaded to the autograder of week2.
