Problem - 1 

---------------Find out districts who achieved 100% objective in BPL cards-----------

Step 1:

This is our data : 

xml_converted_result = FOREACH regex_output generate FLATTEN(($0));

(West Bengal,NORTH 24 PARAGANAS,361462,225080,586542,51,11158,4466,30,0,357960,226104,584064,66,10931,3150,101,0)
(West Bengal,PURULIA,210168,306933,517101,50,7542,4047,10,0,97160,79169,176329,10,4692,1128,20,0)
(West Bengal,SILIGURI,59536,25377,84913,30,935,1393,0,10,37794,18060,55854,30,929,906,5,7)
(West Bengal,SOUTH 24 PARAGANAS,628712,521192,1149904,50,8940,5448,30,0,593712,162487,756199,31,7257,1631,29,29)
(West Bengal,UTTAR DINAJPUR,257662,301645,559307,50,4806,1556,30,0,148802,180619,329421,30,2562,2041,17,0)



============================== >


Step 2 :

Our requirement is to find out states who achieved 100% in BPL cards. So to match this, performance should be equal to objective.

Hence Filter out the dataa with Objective == Performance.


b = FILTER xml_converted_result by ($2==$10);


============================== >

Step 3 : Now generate only the states name with performance number.


States_With_100_Achievement = foreach b generate $0, $1, $10;


================================ >


Step 4 : Exporting the result to HDFS.


STORE States_With_100_Achievement INTO 'hdfs://localhost:9000/Project_2.1' USING PigStorage(',');
;


================================= >

Step 5 : Exporting result data to RDBMS.


use ACADGILD; ---------- > Mention which DB to use.

Creating a table to store result:

CREATE TABLE State_Wise_Development(State_Name varchar(20), District_Name varchar(20), Performance int(10));

Query OK, 0 rows affected (0.01 sec)


Execute below command to export result data into mysql table ----- > State_Wise_Development


sqoop export --connect jdbc:mysql://localhost/ACADGILD --username 'root' -P --table State_Wise_Development --export-dir /Project_2.1 -m 1;


====================================== > 


Result :


mysql> select * from State_Wise_Development;
+-------------------+----------------------+-------------+
| State_Name        | District_Name        | Performance |
+-------------------+----------------------+-------------+
| Andhra Pradesh    | NIZAMABAD            |      225519 |
| Arunachal Pradesh | TIRAP                |        5780 |
| Assam             | HAILAKANDI           |       49837 |
| Bihar             | MADHUBANI            |       67482 |
| Goa               | NORTH GOA            |       15000 |
| Gujarat           | AHMEDABAD            |       80192 |
| Gujarat           | DANGS                |       27900 |
| Gujarat           | NAVSARI              |       75015 |
| Gujarat           | PORBANDAR            |       17024 |
| Gujarat           | SURAT                |      158797 |

