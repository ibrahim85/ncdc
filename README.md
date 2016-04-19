
```
mvn clean package; spark-submit --conf spark.hadoop.validateOutputSpecs=false target/*.jar file:///path/ncdc_data/19?? file:///path/output/
```
