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

Step 2: Assign schema to the data:

data_with_schema = FOREACH xml_converted_result GENERATE (CHARARRAY)$0 as State, (CHARARRAY)$1 as District, (double)$2 as Objective_BPL, (double)$10 as Performance_BPL;


============================== >


Step 3 :

Our requirement is to find out states who achieved 80% in BPL cards.

i.e Performance should be 80% of objective


Hence Filter out the dataa with Performance == 80% of Objective


b = FILTER data_with_schema by (Performance_BPL==(0.8*Objective_BPL));


Result : None of the States have achieved exacly 80% of Objective


============================== >


Below are the list of districts who achieved more than 80% of objective in BPL cards


States_With_MORETHAN_80_Achievement = FILTER data_with_schema by (Performance_BPL>=(0.8*Objective_BPL));


(West Bengal,BARDHAMAN,700047.0,601906.0)
(West Bengal,DAKSHIN DINAJPUR,182621.0,184153.0)
(West Bengal,HOOGHLY,271737.0,269779.0)
(West Bengal,HOWRAH,231860.0,230190.0)
(West Bengal,JALPAIGURI,372999.0,337740.0)
(West Bengal,MIDNAPUR EAST,392371.0,527389.0)
(West Bengal,MIDNAPUR WEST,509496.0,596291.0)
(West Bengal,NADIA,346696.0,321462.0)
(West Bengal,NORTH 24 PARAGANAS,361462.0,357960.0)
(West Bengal,SOUTH 24 PARAGANAS,628712.0,593712.0)


================================ > 

Step 4 : Exporting the result to HDFS.


STORE States_With_MORETHAN_80_Achievement INTO 'hdfs://localhost:9000/Project_2.2' using PigStorage(',');


================================= >

Step 5 : Storing this result in MySql

use ACADGILD; ---------- > Mention which DB to use.

Creating a table to store result:

create table State_Wise_Development_2(State_Name varchar(50), District_Name varchar(50),Target int(10)) Target_Achieved int(10));
Query OK, 0 rows affected (0.01 sec)


Execute below command to export result data into mysql table ----- > State_Wise_Development


sqoop export --connect jdbc:mysql://localhost/ACADGILD --username 'root' -P --table State_Wise_Development_2 --export-dir hdfs://localhost:9000/Project_2.2 -m 1;



=============================== > 


Result :


mysql> select * from State_Wise_Development_2;
+---------------------+------------------------------------------+--------+-----------------+
| State_Name          | District_Name                            | Target | Target_Achieved |
+---------------------+------------------------------------------+--------+-----------------+
| Andhra Pradesh      | ANANTAPUR                                | 363314 |          366557 |
| Andhra Pradesh      | CHITTOOR                                 | 296465 |          269750 |
| Andhra Pradesh      | CUDDAPAH                                 | 251653 |          239780 |
| Andhra Pradesh      | EAST GODAVARI                            | 370255 |          347305 |
| Andhra Pradesh      | KARIMNAGAR                               | 365267 |          369433 |
| Andhra Pradesh      | KHAMMAM                                  | 189225 |          195763 |
| Andhra Pradesh      | KRISHNA                                  | 351572 |          318730 |
| Andhra Pradesh      | KURNOOL                                  | 383478 |          323616 |
| Andhra Pradesh      | MEDAK                                    | 311743 |          310591 |
| Andhra Pradesh      | NALGONDA                                 | 215058 |          224813 |
| Andhra Pradesh      | NIZAMABAD                                | 225519 |          225519 |
| Andhra Pradesh      | RANGAREDDI                               | 212629 |          174460 |
| Andhra Pradesh      | WARANGAL                                 | 330260 |          359732 |
| Andhra Pradesh      | WEST GODAVARI                            | 344272 |          319477 |
| Arunachal Pradesh   | DIBANG VALLEY                            |   1085 |            1088 |
| Arunachal Pradesh   | LOHIT                                    |   8800 |            8410 |
| Arunachal Pradesh   | TIRAP                                    |   5780 |            5780 |













