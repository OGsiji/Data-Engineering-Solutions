# -*- coding: utf-8 -*-
"""
Created on Thu Oct 14 23:27:29 2021

@author: ogunniran siji
"""



from __future__ import print_function

import json
import boto3
from kafka import KafkaProducer
import urllib
import ssl
import logging

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)
logging.basicConfig(format='%(asctime)s %(message)s',level=logging.DEBUG)

print('Loading function')

s3 = boto3.client('s3')

context = ssl.create_default_context()
context.options &= ssl.OP_NO_TLSv1
context.options &= ssl.OP_NO_TLSv1_1



#The Producer API allows applications to send streams of data to topics in the Kafka cluster
producer = KafkaProducer(
   bootstrap_servers=['koboLogistics:23.345.546.001'],
   value_serializer=lambda m: json.dumps(m).encode('ascii'),
   retry_backoff_ms=500,
   request_timeout_ms=20000,
   security_protocol='SASL_SSL',
   sasl_mechanism='PLAIN',
   ssl_context=context,
   sasl_plain_username='KAQ6FBDAGJHXTNUD',
   sasl_plain_password='+Vz/bZr89unWz8f2ufuDUeJgKSB2/BBFtAsxgCM6cstG2WrO6cK4lMTfoTyewSUv')


#connect-mongodb-source.properties
#This contains the MongoDB’s connection URL, port, database name, collection name.

{
  "name": "mongo-sink-debezium-cdc",
  "config": {
    "key.converter=org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable=true",
    "value.converter=org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable=true",
    "value.converter.schema.registry.url":"koboLogistics:23.345.546.001",
  	"connector.class": "com.mongodb.kafka.connect.MongoSinkConnector",
    "topics": "myreplset.kafkaconnect.mongosrc",
    "connection.uri": "mongodb://mongodb:27017/kafkaconnect?w=1&journal=true",
    "change.data.capture.handler": "com.mongodb.kafka.connect.cdc.debezium.mongodb.MongoDbHandler",
    "collection": "mongosink"
  }
}
  
  


#it is expected each stream of the json file is loaded into the event. 
  def preprocess(json):
  Parsed_data = json_normalize( json)
  Parsed_data['_id']=Parsed_data['documentKey._id'].apply(lambda x: x.split('"')[3])
  Parsed_data['fullDocument.name'] = Parsed_data['fullDocument.name'].str[:-7] 
  Parsed_data=Parsed_data[['_id','fullDocument.name','fullDocument.surname','fullDocument.position','operationType']]
  return(Parsed_data)


def lambda_handler(event, context):
        
    print("Received event: " +  preprocess(event['file']))

  
    except Exception as e:
        print(e)
        print('Error getting Topic {}. Make sure they exist and your bucket is in the same region as this function.'.format(koboLogistics”))
        raise e