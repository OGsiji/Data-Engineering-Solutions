Question 3



#Lets Assume the Table name is Event
#the following Postgress RDBMS solution  returns the id of the inserted row

INSERT INTO Event(operationType)
VALUES (value1, value2)
RETURNING _id;
