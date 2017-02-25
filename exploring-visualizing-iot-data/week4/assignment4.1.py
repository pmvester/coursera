
# coding: utf-8

# # Assignment 4
# 
# Welcome to Assignment 4. This will be the most fun. Now we will prepare data for plotting.
# 
# Just make sure you hit the play button on each cell from top to down. There are three functions you have to implement. Please also make sure than on each change on a function you hit the play button again on the corresponding cell to make it available to the rest of this notebook. Please also make sure to only implement the function bodies and DON'T add any additional code outside functions since this might confuse the autograder.
# 
# So the function below is used to make it easy for you to create a data frame from a cloudant data frame using the so called "DataSource" which is some sort of a plugin which allows ApacheSpark to use different data sources.
# 

# In[1]:

#Please don't modify this function
def readDataFrameFromCloudant(host,user,pw,database):
    cloudantdata=spark.read.format("com.cloudant.spark").     option("cloudant.host",host).     option("cloudant.username", user).     option("cloudant.password", pw).     load(database)

    cloudantdata.createOrReplaceTempView("washing")
    spark.sql("SELECT * from washing").show()
    return cloudantdata


# Sampling is one of the most important things when it comes to visualization because often the data set get so huge that you simply
# 
# - can't copy all data to a local Spark driver (Data Science Experience is using a "local" Spark driver)
# - can't throw all data at the plotting library
# 
# Please implement a function which returns a 10% sample of a given data frame:

# In[2]:

def getSample(df,spark):
    return df.rdd.sample(False, 0.1)


# Now we want to create a histogram and boxplot. Please ignore the sampling for now and retur a python list containing all temperature values from the data set

# In[3]:

def getListForHistogramAndBoxPlot(df,spark):
    data = spark.sql("SELECT temperature FROM washing WHERE temperature IS NOT NULL")
    data_array = data.rdd.map(lambda row : row.temperature).collect()
    return data_array


# Finally we want to create a run chart. Please return two lists (encapusalted in a python tuple object) containing temperature and timestamp (ts) ordered by timestamp. Please refere to the following link to learn more about tuples in python: https://www.tutorialspoint.com/python/python_tuples.htm

# In[4]:

#should return a tuple containing the two lists for timestamp and temperature
#please make sure you take only 10% of the data by sampling
#please also ensure that you sample in a way that the timestamp samples and temperature samples correspond (=> call sample on an object still containing both dimensions)
def getListsForRunChart(df,spark):
    result_df = spark.sql("SELECT ts, temperature FROM washing WHERE temperature IS NOT NULL ORDER BY ts ASC")
    result_rdd = result_df.rdd.sample(False, 0.1)
    result_ts_array = result_rdd.map(lambda row : row.ts).collect()
    result_temperature_array = result_rdd.map(lambda row : row.temperature).collect()
    return result_ts_array, result_temperature_array


# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED
# #axx
# ### PLEASE DON'T REMOVE THIS BLOCK - THE FOLLOWING CODE IS NOT GRADED

# In[5]:

#TODO Please provide your Cloudant credentials here
hostname = "9274dedf-1881-427d-8445-de0970d541e3-bluemix.cloudant.com"
user = "9274dedf-1881-427d-8445-de0970d541e3-bluemix"
pw = "94a4e87e5aef7bf6959c2666f303acee7f36da0ebc6701b6dba648ce89b2dabc"
database = "washing"
cloudantdata=readDataFrameFromCloudant(hostname, user, pw, database)


# In[6]:

get_ipython().magic(u'matplotlib inline')
import matplotlib.pyplot as plt


# In[7]:

plt.hist(getListForHistogramAndBoxPlot(cloudantdata,spark))
plt.show()


# In[8]:

plt.boxplot(getListForHistogramAndBoxPlot(cloudantdata,spark))
plt.show()


# In[9]:

lists = getListsForRunChart(cloudantdata,spark)


# In[10]:

plt.plot(lists[0],lists[1])
plt.xlabel("time")
plt.ylabel("temperature")
plt.show()


# Congratulations, you are done! Please download the notebook as python file, name it assignment4.1.py and sumbit it to the grader.
