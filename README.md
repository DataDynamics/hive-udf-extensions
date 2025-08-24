# Hive UDF Extensions

## Requirement

* Hive 3.1.3
* JDK 11 이상

## Use

```sql
add jar 'hive-udf-extensions-1.0.0.jar';

create temporary function array_contains as 'com.github.aaronshan.functions.array.UDFArrayContains';
create temporary function array_equals as 'com.github.aaronshan.functions.array.UDFArrayEquals';
create temporary function array_intersect as 'com.github.aaronshan.functions.array.UDFArrayIntersect';
create temporary function array_max as 'com.github.aaronshan.functions.array.UDFArrayMax';
create temporary function array_min as 'com.github.aaronshan.functions.array.UDFArrayMin';
create temporary function array_join as 'com.github.aaronshan.functions.array.UDFArrayJoin';
create temporary function array_distinct as 'com.github.aaronshan.functions.array.UDFArrayDistinct';
create temporary function array_position as 'com.github.aaronshan.functions.array.UDFArrayPosition';
create temporary function array_remove as 'com.github.aaronshan.functions.array.UDFArrayRemove';
create temporary function array_reverse as 'com.github.aaronshan.functions.array.UDFArrayReverse';
create temporary function array_sort as 'com.github.aaronshan.functions.array.UDFArraySort';
create temporary function array_concat as 'com.github.aaronshan.functions.array.UDFArrayConcat';
create temporary function array_value_count as 'com.github.aaronshan.functions.array.UDFArrayValueCount';
create temporary function array_slice as 'com.github.aaronshan.functions.array.UDFArraySlice';
create temporary function array_element_at as 'com.github.aaronshan.functions.array.UDFArrayElementAt';
create temporary function array_shuffle as 'com.github.aaronshan.functions.array.UDFArrayShuffle';
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