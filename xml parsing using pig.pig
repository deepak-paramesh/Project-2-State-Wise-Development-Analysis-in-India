File is in xml format. To process the file first we need to parse it using pig.


Since we have xml file in HDFS, launch pig in Map reduce mode.


First register piggybank jar.

	1. load the raw xml file into a relation using XMLLoader function. In this xml root element is ---- > 'row'

xml_raw_data = LOAD 'hdfs://localhost.localdomain:9000/flume_sink/FlumeData.1502377822768' using org.apache.pig.piggybank.storage.XMLLoader('row')as (a:chararray);

----------------------------- >

2. This step includes bringing the tags between root eleemet 'row' into a single line.


xml_single_line = FOREACH xml_raw_data generate  REPLACE(a,'[\\n]','') as a;


----------------------------- >

3.  In this step using regeular expressions remove all the angular braces and format the data in csv file type.



regex_output = foreach xml_single_line generate REGEX_EXTRACT_ALL(a,'.*(?:<State_Name>)([^<]*).*(?:<District_Name>)([^<]*).*(?:<Project_Objectives_IHHL_BPL>)([^<]*).*(?:<Project_Objectives_IHHL_APL>)([^<]*).*(?:<Project_Objectives_IHHL_TOTAL>)([^<]*).*(?:<Project_Objectives_SCW>)([^<]*).*(?:<Project_Objectives_School_Toilets>)([^<]*).*(?:<Project_Objectives_Anganwadi_Toilets>)([^<]*).*(?:<Project_Objectives_RSM>)([^<]*).*(?:<Project_Objectives_PC>)([^<]*).*(?:<Project_Performance-IHHL_BPL>)([^<]*).*(?:<Project_Performance-IHHL_APL>)([^<]*).*(?:<Project_Performance-IHHL_TOTAL>)([^<]*).*(?:<Project_Performance-SCW>)([^<]*).*(?:<Project_Performance-School_Toilets>)([^<]*).*(?:<Project_Performance-Anganwadi_Toilets>)([^<]*).*(?:<Project_Performance-RSM>)([^<]*).*(?:<Project_Performance-PC>)([^<]*).*');

Sample output :

((Andhra Pradesh,ADILABAD,247475,148181,395656,0,4462,427,10,0,176300,52431,228731,0,4462,427,0,0))
((Andhra Pradesh,ANANTAPUR,363314,181335,544649,0,3421,284,10,0,366557,42000,408557,0,4258,302,0,0))
((Andhra Pradesh,CHITTOOR,296465,236986,533451,0,8171,375,10,0,269750,190905,460655,0,8171,375,11,0))
((Andhra Pradesh,CUDDAPAH,251653,251610,503263,0,6802,277,10,0,239780,125493,365273,0,5431,277,0,0))
((Andhra Pradesh,EAST GODAVARI,370255,191400,561655,50,7004,1164,10,0,347305,191400,538705,49,7004,781,20,0))


----------------------------- >


4. Using FLATTEN remove all remaining braces to get final output.

xml_converted_result = FOREACH regex_output generate FLATTEN(($0));

(West Bengal,NADIA,346696,278335,625031,50,6974,6620,50,0,321462,198890,520352,28,6635,3961,17,41)
(West Bengal,NORTH 24 PARAGANAS,361462,225080,586542,51,11158,4466,30,0,357960,226104,584064,66,10931,3150,101,0)
(West Bengal,PURULIA,210168,306933,517101,50,7542,4047,10,0,97160,79169,176329,10,4692,1128,20,0)
(West Bengal,SILIGURI,59536,25377,84913,30,935,1393,0,10,37794,18060,55854,30,929,906,5,7)

-------------------------------- > 


