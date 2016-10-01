
 Assume:
    - session time window is fixed, 15 minutes
    - this session time window starts at 2015-07-22T09:00:00.000Z
    - this session time windows ends at 2015-07-22T09:15:00.000Z
    - distinct user is ip:port
    
To find the average session time and the most engaged user, I chose to run it as two parts:
  1) a mapreduce job to get the max time for each user. 
  2) then use the output of the mapreduce job, and run in a local file to get the average session time and the most engaged user. 
Reason: I think it will not benefit from a mapreduce job since it only can run in one reducer.


Problem study:

  IP addresses do not guarantee distinct users, but this is the limitation of the data. 
  As a bonus, consider what additional data would help make better analytical conclusions

For the above problem, maybe take Cookies which contains user id into consideration.

