# kafka-concurrent-processing

To run test, perform the below:
- Run docker sql server db, you cannot use postgres because row version only works on sql server
- Run the program using "dotnet run" command
- After around 4 seconds, run second instance of the program after kafka message get pick up. If the second instance, you should see concurrency exception occured.
