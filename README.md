# Data-Engineering-Solutions
Kafka Cluster setup whose source is a mongo DB database.

This is a solution to the kobo360 question.

Assumptions and Thought Process:

Question 1:
1) The text file loaded for the serialization needs to be converted to a json format before preprocessing
2) The JSON file given is with nested lists/dictionaries which means this would require you to write a script for flattening.
3) The fullDocument.name has an error ('JSON R') at the end which needs to be removed along each row. 
4) This solution needs to be automated hence the function must be modularized

Question 2
1) I used kafka-python rather than the more logical confluent-kafka-python because confluent-kafka-python has a dependency on librdkafka, which is a C library. 
2) The extra SSL configs depends on the version of SSL dependency. 
3) MongoDB is feeds the connector api for the experiment while the producer is the kafka cluster setup
4) All other info including metadata are ignored and data is in arrays

Question3
1) Assume the Table name is Event
2) We want to return the id of the inserted row
3) We are inserting update into first row and delete into second row
