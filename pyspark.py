#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.getOrCreate()


# In[3]:


test_df = [("James","Sales","NY",90000,34,10000),
          ("Michael","Sales","NY",86000,56,20000),
          ("Robert","Sales","CA",81000,30,23000),
          ("Maria","Finance","CA",90000,24,23000),
          ("Raman","Finance","CA",99000,40,24000),
          ("Scott","Finance","NY",83000,36,19000),
          ("Jen","Finance","NY",79000,53,15000),
          ("Jeff","Marketing","CA",80000,25,18000),
          ("Kumar","Marketing","NY",91000,50,21000),]


# In[4]:


udf_schema=["employee_name","department","state","salary","age","bonus"]


# In[5]:


df=spark.createDataFrame(data=test_df,schema=udf_schema)


# In[6]:


df.show()


# In[7]:


from pyspark import StorageLevel


# In[8]:


test=StorageLevel(useDisk=False,useMemory=True,useOffHeap=False,deserialized=False)


# In[9]:


import sys
print(sys.version)


# In[10]:


df.persist(storageLevel=test)


# In[11]:


df.unpersist()


# In[16]:


df.groupBy("department").


# In[14]:


df.write.saveAsTable("emp_tbl")


# In[17]:


test=spark.sql("DESCRIBE emp_tbl")


# In[18]:


test.show()


# In[19]:


test01=spark.sql("DESCRIBE EXTENDED emp_tbl")
test01.show()


# In[20]:


spark.sql('CREATE DATABASE idashell')


# In[21]:


spark.sql("use idashell")


# In[22]:


df.write.saveAsTable('test_tbl')


# In[23]:


test01=spark.sql('DESCRIBE EXTENDED test_tbl')
test01.show()


# In[ ]:




