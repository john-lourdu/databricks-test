-- Databricks notebook source
-- MAGIC %scala
-- MAGIC val publicKey = ""
-- MAGIC 
-- MAGIC 
-- MAGIC def addAuthorizedPublicKey(key: String): Unit = {
-- MAGIC   val fw = new java.io.FileWriter("/home/ubuntu/.ssh/authorized_keys", /* append */ true)
-- MAGIC   fw.write("\n" + key)
-- MAGIC   fw.close()
-- MAGIC }
-- MAGIC 
-- MAGIC val numExecutors = sc.getExecutorMemoryStatus.keys.size
-- MAGIC sc.parallelize(0 until numExecutors, numExecutors).foreach { i =>
-- MAGIC   addAuthorizedPublicKey(publicKey)
-- MAGIC }
-- MAGIC addAuthorizedPublicKey(publicKey)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls("/databricks-datasets"))

-- COMMAND ----------

-- MAGIC %python
-- MAGIC with open("/dbfs/databricks-datasets/README.md") as f:
-- MAGIC     x = ''.join(f.readlines())
-- MAGIC 
-- MAGIC print(x)

-- COMMAND ----------

-- MAGIC %fs mounts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.unmount("/mnt/john")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC ACCESS_KEY = ""
-- MAGIC SECRET_KEY = ""
-- MAGIC ENCODED_SECRET_KEY = SECRET_KEY.replace("/", "%2F")
-- MAGIC AWS_BUCKET_NAME = "john-s3"
-- MAGIC MOUNT_NAME = "john"
-- MAGIC 
-- MAGIC dbutils.fs.mount("s3a://%s:%s@%s" % (ACCESS_KEY, ENCODED_SECRET_KEY, AWS_BUCKET_NAME), "/mnt/%s" % MOUNT_NAME)
-- MAGIC display(dbutils.fs.ls("/mnt/%s" % MOUNT_NAME))

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC ls -l /dbfs/mnt/john

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC cd  /dbfs/mnt/john/; gzip -c yellow_tripdata_2009-03.csv yellow_tripdata_2009-04.csv yellow_tripdata_2009-10.csv yellow_tripdata_2010-04.csv yellow_tripdata_2010-09.csv yellow_tripdata_2011-03.csv yellow_tripdata_2014-03.csv > john-test.csv.gz

-- COMMAND ----------

-- MAGIC %sh cat  /dbfs/databricks/john/scripts/install_openssl.sh

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC gunzip /dbfs/mnt/john/john-test.csv.gz

-- COMMAND ----------

/dbfs/databricks/cluster_init/OpenSSL/openssl.sh 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mkdirs("dbfs:/databricks/john/scripts")
-- MAGIC dbutils.fs.put("dbfs:/databricks/john/scripts/install_openssl.sh","""
-- MAGIC #!/bin/bash
-- MAGIC /databricks/python/bin/pip install pyOpenSSL==19.0.0
-- MAGIC """,True)

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC #/bin/bash
-- MAGIC 
-- MAGIC while true; do
-- MAGIC echo 'test'
-- MAGIC done

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.put("dbfs:/databricks/john/scripts/additional_tag.sh" ,""" 
-- MAGIC #!/bin/bash 
-- MAGIC sudo apt-get -y update 
-- MAGIC sudo apt-get -y install --fix-missing cloud-utils 
-- MAGIC EC2_INSTANCE_ID=$(ec2metadata --instance-id) 
-- MAGIC /databricks/python3/bin/pip3 install --upgrade awscli 
-- MAGIC export PATH=/databricks/python3/bin:$PATH
-- MAGIC export AWS_ACCESS_KEY_ID=''
-- MAGIC export AWS_SECRET_ACCESS_KEY=''
-- MAGIC aws configure set aws_access_key_id ''
-- MAGIC aws configure set aws_secret_access_key ''
-- MAGIC aws configure set default.region us-west-2
-- MAGIC aws ec2 create-tags --resources $EC2_INSTANCE_ID --tags Key=Name,Value=Databricks-John Key=VA-BusinessUnit,Value=claimanaly Key=VA-EnvironmentType,Value=dev Key=VA-DeploymentZone,Value=private Key=VA-MgmtModel,Value=cattle Key=VA-ConfigManagementTool,Value=Databricks Key=WorkloadType,Value=app Key=VA-OSType,Value=linux Key=VA-ConfigPersonality,Value=default Key=VA-ConfigVersion,Value=default Key=VA-DeploymentGuid,Value=None
-- MAGIC """, True)

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC #!/bin/bash 
-- MAGIC sudo apt-get -y update 
-- MAGIC sudo apt-get -y install --fix-missing cloud-utils 
-- MAGIC EC2_INSTANCE_ID=$(ec2metadata --instance-id) 
-- MAGIC /databricks/python3/bin/pip3 install --upgrade awscli 
-- MAGIC export PATH=/databricks/python3/bin:$PATH
-- MAGIC export AWS_ACCESS_KEY_ID=''
-- MAGIC export AWS_SECRET_ACCESS_KEY=''
-- MAGIC aws configure set aws_access_key_id 
-- MAGIC aws configure set aws_secret_access_key 
-- MAGIC aws configure set default.region us-west-2
-- MAGIC aws ec2 create-tags --resources $EC2_INSTANCE_ID --tags Key=Name,Value=Databricks-John Key=VA-BusinessUnit,Value=claimanaly Key=VA-EnvironmentType,Value=dev Key=VA-DeploymentZone,Value=private Key=VA-MgmtModel,Value=cattle Key=VA-ConfigManagementTool,Value=Databricks Key=WorkloadType,Value=app Key=VA-OSType,Value=linux Key=VA-ConfigPersonality,Value=default Key=VA-ConfigVersion,Value=default Key=VA-DeploymentGuid,Value=None

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/tables/databricks_usage

-- COMMAND ----------

-- MAGIC %fs mkdirs /FileStore/tables/databricks_usage

-- COMMAND ----------

-- MAGIC %sh pip install atlassian_jwt_auth==3.6.0

-- COMMAND ----------

-- MAGIC %sh
-- MAGIC pip list | egrep -i 'atlassian-jwt-auth|PyJWT|CacheControl|cryptography|requests'

-- COMMAND ----------

-- MAGIC %sh pip show atlassian-jwt-auth

-- COMMAND ----------

-- MAGIC %sh pip install pipdeptree

-- COMMAND ----------

-- MAGIC %sh pipdeptree  -p atlassian-jwt-auth

-- COMMAND ----------

-- MAGIC %sh pipdeptree

-- COMMAND ----------

-- MAGIC %sh ls -l /

-- COMMAND ----------

-- MAGIC %sh df -h

-- COMMAND ----------

-- MAGIC %sh df -h

-- COMMAND ----------

-- MAGIC %sh lsblk

-- COMMAND ----------

-- MAGIC %sh lsblk

-- COMMAND ----------

-- MAGIC %fs

-- COMMAND ----------

-- MAGIC %sh df -h

-- COMMAND ----------

-- MAGIC %sh df -h

-- COMMAND ----------

-- MAGIC %sh lsblk

-- COMMAND ----------

-- MAGIC %sh ls -l /var/log

-- COMMAND ----------

-- MAGIC %sh df -T

-- COMMAND ----------

-- MAGIC %sh pvscan

-- COMMAND ----------

-- MAGIC %sh j

-- COMMAND ----------

-- MAGIC %fs ls  /18431

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('dbfs:/18431/userdata1.parquet','file:/dbfs/ml/userdata1.parquet', recurse=True)

-- COMMAND ----------

-- MAGIC %fs ls /ml

-- COMMAND ----------

-- MAGIC %sh ls -l /dbfs/mnt

-- COMMAND ----------

-- MAGIC %fs mounts

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("s3a://chris-s3-us-west2")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC sc._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", "")
-- MAGIC sc._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", "")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.ls("s3a://john-s3")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.cp('s3://john-s3/hello.txt/','file:/dbfs/ml/hello.txt', recurse=True)

-- COMMAND ----------

-- MAGIC %sh rm -f   /dbfs/ml/hello.txt

-- COMMAND ----------

