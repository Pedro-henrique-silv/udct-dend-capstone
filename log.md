# Log details

## S3 Conection and transform process

![Transform_process](./images/transform_process.png)

## Create table (first time)

![part1_create1](./images/create_table1.PNG)
![part1_create2](./images/create_table2.PNG)
![part1_create3](./images/create_table3.PNG)
![part1_create4](./images/create_table4.PNG)

## Data Quality

obs: The total quantity of lines in S3 is summed 10.000 on dimensions tables and favorites staging tables. On staging anime list table it is summed 10.000.000. It had ocurred during some tests in data quality check. Correct total ammount is:

* animes - 16.938
* anime_relations - 23.428
* producers - 18.640
* licensors - 4.307
* studios - 10.328
* genres - 47.662
* staging_anime_list - 1.195.606 
* staging_favorite - 18.207
* users - 3.781

![data_quality1](./images/data_quality1.PNG)
![data_quality2](./images/data_quality2.PNG)

## Drop old redshift tables and create new ones

![drop](./images/drop.PNG)
![part2_create1](./images/part2_create_table1.PNG)
![part2_create2](./images/part2_create_table2.PNG)
![part2_create3](./images/part2_create_table3.PNG)
![part2_create4](./images/part2_create_table4.PNG)

## Copy from S3 to Redshift

![Copy_to_redshift](./images/Copy_to_redshift.PNG)

## Insert data into Fact table

![inset_query](./images/inset_query.PNG)

## Redshift data quality

![part2_data_quality1](./images/part2_data_quality1.PNG)
![part2_data_quality2](./images/part2_data_quality2.PNG)

## Final

![final](./images/final.PNG)
