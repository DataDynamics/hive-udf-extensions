# Hive UDF Extensions

## Requirement

* Hive 3.1.3
* JDK 11 이상

## Use

### Temporary Use

임시적으로 사용하고자 하는 경우 다음과 같이 적용합니다.

```sql
add jar 'hive-udf-extensions-1.0.0.jar';

create temporary function array_contains as 'io.datadynamics.hive.udf.array.UDFArrayContains';
create temporary function array_equals as 'io.datadynamics.hive.udf.array.UDFArrayEquals';
create temporary function array_intersect as 'io.datadynamics.hive.udf.array.UDFArrayIntersect';
create temporary function array_max as 'io.datadynamics.hive.udf.array.UDFArrayMax';
create temporary function array_min as 'io.datadynamics.hive.udf.array.UDFArrayMin';
create temporary function array_join as 'io.datadynamics.hive.udf.array.UDFArrayJoin';
create temporary function array_distinct as 'io.datadynamics.hive.udf.array.UDFArrayDistinct';
create temporary function array_position as 'io.datadynamics.hive.udf.array.UDFArrayPosition';
create temporary function array_remove as 'io.datadynamics.hive.udf.array.UDFArrayRemove';
create temporary function array_reverse as 'io.datadynamics.hive.udf.array.UDFArrayReverse';
create temporary function array_sort as 'io.datadynamics.hive.udf.array.UDFArraySort';
create temporary function array_concat as 'io.datadynamics.hive.udf.array.UDFArrayConcat';
create temporary function array_value_count as 'io.datadynamics.hive.udf.array.UDFArrayValueCount';
create temporary function array_slice as 'io.datadynamics.hive.udf.array.UDFArraySlice';
create temporary function array_element_at as 'io.datadynamics.hive.udf.array.UDFArrayElementAt';
create temporary function array_shuffle as 'io.datadynamics.hive.udf.array.UDFArrayShuffle';
```

```sql
select array_contains(array(16,12,18,9), 12) => true
select array_equals(array(16,12,18,9), array(16,12,18,9)) => true
select array_intersect(array(16,12,18,9,null), array(14,9,6,18,null)) => [null,9,18]
select array_max(array(16,13,12,13,18,16,9,18)) => 18
select array_min(array(16,12,18,9)) => 9
select array_join(array(16,12,18,9,null), '#','=') => 16#12#18#9#=
select array_distinct(array(16,13,12,13,18,16,9,18)) => [9,12,13,16,18]
select array_position(array(16,13,12,13,18,16,9,18), 13) => 2
select array_remove(array(16,13,12,13,18,16,9,18), 13) => [16,12,18,16,9,18]
select array_reverse(array(16,12,18,9)) => [9,18,12,16]
select array_sort(array(16,13,12,13,18,16,9,18)) => [9,12,13,13,16,16,18,18]
select array_concat(array(16,12,18,9,null), array(14,9,6,18,null)) => [16,12,18,9,null,14,9,6,18,null]
select array_value_count(array(16,13,12,13,18,16,9,18), 13) => 2
select array_slice(array(16,13,12,13,18,16,9,18), -2, 3) => [9,18]
select array_element_at(array(16,13,12,13,18,16,9,18), -1) => 18
select array_shuffle(array(16,12,18,9))
```

### Permanent Use

HDFS에 UDF JAR 파일을 다음과 같이 업로드합니다. 해당 JAR 파일은 `hive` 계정이 접근할 수 있는 권한이 있어야 합니다.

```
hdfs dfs -mkdir /udf_jars
hdfs dfs -chmod 755 /udf_jars
hdfs dfs -put hive-udf-extensions-1.0.0.jar /udf_jars
hdfs dfs -chmod 755 /udf_jars/hive-udf-extensions-1.0.0.jar
```

```sql
USE bdphive;
    
create function bdphive.array_contains as 'io.datadynamics.hive.udf.array.UDFArrayContains' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_equals as 'io.datadynamics.hive.udf.array.UDFArrayEquals' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_intersect as 'io.datadynamics.hive.udf.array.UDFArrayIntersect' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_max as 'io.datadynamics.hive.udf.array.UDFArrayMax' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_min as 'io.datadynamics.hive.udf.array.UDFArrayMin' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_join as 'io.datadynamics.hive.udf.array.UDFArrayJoin' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_distinct as 'io.datadynamics.hive.udf.array.UDFArrayDistinct' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_position as 'io.datadynamics.hive.udf.array.UDFArrayPosition' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_remove as 'io.datadynamics.hive.udf.array.UDFArrayRemove' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_reverse as 'io.datadynamics.hive.udf.array.UDFArrayReverse' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_sort as 'io.datadynamics.hive.udf.array.UDFArraySort' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_concat as 'io.datadynamics.hive.udf.array.UDFArrayConcat' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_value_count as 'io.datadynamics.hive.udf.array.UDFArrayValueCount' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_slice as 'io.datadynamics.hive.udf.array.UDFArraySlice' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_element_at as 'io.datadynamics.hive.udf.array.UDFArrayElementAt' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
create function bdphive.array_shuffle as 'io.datadynamics.hive.udf.array.UDFArrayShuffle' USING JAR '/udf_jars/hive-udf-extensions-1.0.0.jar';
```