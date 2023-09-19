# kafka-assignment-streaming-app
topic-1 partition:3 contains user info - key : ssn body: JSON with details about the user. 
topic-2 partition:5 contains fines - key: fineId  body: JSON with details of fee, amount to pay and ssn of the user. 
store in topic-3 with partition:4 - key: ssn  body: JSON with the details of user, list of the fines assigned to user and total amount to pay.
