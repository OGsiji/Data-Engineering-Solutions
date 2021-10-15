Question 3



''' -Lets Assume the Table name is Event
    -the following Postgress RDBMS solution returns the id of the inserted row
    -we are inserting update into first row and delete into second row '''

INSERT INTO Event(operationType)
VALUES ('update', 'delete')
RETURNING _id;
