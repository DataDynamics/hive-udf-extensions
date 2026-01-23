# Hive UDF Extensions

## Requirement

* Hive 3.1.3
* JDK 11 이상

## Cloudera CDP for GeoSpatial

Cloudera CDP의 Impala에서 GeoSpatial 기능을 활성화 하기 위해서 Coordinator, Executor의 startup flag 설정에 `--geospatial_library=HIVE_ESRI`을 추가하도록 합니다.

Cloudera CDP의 GeoSpatial 지원은 https://impala.apache.org/docs/build/html/topics/impala_geospatial_functions.html 을 참고하십시오.

Apache Impala의 documentation에 다음과 같이 기술되어 있습니다.

```
Coordinates for geometries may be 2D (x, y), 3D (x, y, z) or 4D (x, y, z, m). Geospatial functionality can be controlled through the startup flag GEOSPATIAL_LIBRARY.
- By default, the library is set to HIVE_ESRI, enabling the Hive ESRI geospatial functions.
- If geospatial functionality is not needed, it can be turned off by setting GEOSPATIAL_LIBRARY to NONE.
```

Apache Impala의 GeoSpatial Function은 Hive ESRI(https://github.com/Esri/spatial-framework-for-hadoop/wiki/UDF-Documentation)을 기반으로 하고 있으며 native하고 C++로 구현되어 있습니다.

### Impala GeoSpatial의 포맷

* Impala는 https://github.com/Esri/spatial-framework-for-hadoop의 Binary 포맷을 사용
* 이 shapefile 내부의 바이너리 포멧 + 헤더로 구성
* 헤더에 대한 설명 - https://github.com/apache/impala/blob/master/be/src/exprs/geo/shape-format.h
* 임팔라 GeoSpatial Query시 보통 WKB를 사용하고, 변환/복원에 사용하는 함수는 다음과 같음
  * `ST_AsBinary` 
  * `ST_GeomFromWKB`
  * 바이너리 포맷을 ESRI로 변환하는 로직 : `com.esri.hadoop.hive.GeometryUtils`
    * https://github.com/Esri/spatial-framework-for-hadoop/blob/master/hive/src/main/java/com/esri/hadoop/hive/GeometryUtils.java 

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
hdfs dfs -mkdir hdfs://<NN>/lib-ext/
hdfs dfs -chmod 755 hdfs://<NN>/lib-ext/
hdfs dfs -put hive-udf-extensions-1.0.0.jar hdfs://<NN>/lib-ext/
hdfs dfs -chmod 755 hdfs://<NN>/lib-ext/hive-udf-extensions-1.0.0.jar
```

```sql
USE bdp;
    
create function bdp.array_contains as 'io.datadynamics.hive.udf.array.UDFArrayContains' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_equals as 'io.datadynamics.hive.udf.array.UDFArrayEquals' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_intersect as 'io.datadynamics.hive.udf.array.UDFArrayIntersect' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_max as 'io.datadynamics.hive.udf.array.UDFArrayMax' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_min as 'io.datadynamics.hive.udf.array.UDFArrayMin' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_join as 'io.datadynamics.hive.udf.array.UDFArrayJoin' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_distinct as 'io.datadynamics.hive.udf.array.UDFArrayDistinct' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_position as 'io.datadynamics.hive.udf.array.UDFArrayPosition' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_remove as 'io.datadynamics.hive.udf.array.UDFArrayRemove' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_reverse as 'io.datadynamics.hive.udf.array.UDFArrayReverse' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_sort as 'io.datadynamics.hive.udf.array.UDFArraySort' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_concat as 'io.datadynamics.hive.udf.array.UDFArrayConcat' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_value_count as 'io.datadynamics.hive.udf.array.UDFArrayValueCount' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_slice as 'io.datadynamics.hive.udf.array.UDFArraySlice' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_element_at as 'io.datadynamics.hive.udf.array.UDFArrayElementAt' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_shuffle as 'io.datadynamics.hive.udf.array.UDFArrayShuffle' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
create function bdp.array_sequence as 'io.datadynamics.hive.udf.array.UDFSequence' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';

create function bdp.safe_divide as 'io.datadynamics.hive.udf.GenericUDFSafeDivide' USING JAR 'hdfs://<NN>/data/raw/system/lib-ext/hive-udf-extensions-1.0.0.jar';
```

## Macro

### Presto Try

```sql
-- 세이프 정수 변환
CREATE TEMPORARY MACRO try_int(s STRING)
(CASE WHEN s RLIKE '^-?[0-9]+$' THEN CAST(s AS INT) ELSE NULL END);

-- 세이프 실수 변환
CREATE TEMPORARY MACRO try_double(s STRING)
(CASE WHEN s RLIKE '^-?[0-9]+(\\.[0-9]+)?$' THEN CAST(s AS DOUBLE) ELSE NULL END);

-- 세이프 날짜(YYYY-MM-DD)
CREATE TEMPORARY MACRO try_date(s STRING)
(CASE WHEN s RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN TO_DATE(s) ELSE NULL END);
``````

# Impala GeoSpatial Functions for Oracle GeoSpatial Functions

* Cloudera Impala 에서는 Cloudera Manager 의 Impala 에서 다음 설정을 추가합니다.
  * CM > Impala
    * 구성 > Impala Daemon 명령줄 인수 고급 구성 스니펫(안전 밸브)
    * Configuration > Impala Daemon Command Line Argument Advanced Configuration Snippet (Safety Valve)
      * `--geospatial_library=HIVE_ESRI`
      * [impala 공식문서](https://impala.apache.org/docs/build/html/topics/impala_geospatial_functions.html) 에는 `GEOSPATIAL_LIBRARY` 를 `HIVE_ESRI` 로 설정하라고 하지만, 소문자로 지정해야 합니다.
* Apache Impala 에서는 impala coordinator 와 impala executor 의 startup flag 에 다음 설정을 추가합니다.
  * `--geospatial_library=HIVE_ESRI`

더 이전 버전을 사용하는 경우 아래와 같이 수동으로 udf 를 추가합니다.

spatial function 을 hive 에 등록하기 위해 아래 esri repository 에서 hive udf 가 들어있는 jar file 을 download 한 후에 udf 를 추가합니다.

https://github.com/Esri/spatial-framework-for-hadoop/releases

```shell
wget https://github.com/Esri/spatial-framework-for-hadoop/releases/download/v2.2.0/spatial-sdk-hive-2.2.0.jar
hdfs dfs -put spatial-sdk-hive-2.2.0.jar </path/to/jars>

impala-shell -i <impala-daemon.host.local>
```

```sql
DROP FUNCTION IF EXISTS ST_AsText(BINARY);
DROP FUNCTION IF EXISTS ST_GeomFromText(STRING);
-- ... 그 외 필요한 함수들
CREATE FUNCTION ST_AsText(BINARY) RETURNS STRING LOCATION '/path/to/jars/spatial-sdk-hive-2.2.0.jar' SYMBOL='com.esri.hadoop.hive.ST_AsText';
CREATE FUNCTION ST_GeomFromText(STRING) RETURNS BINARY LOCATION '/path/to/jars/spatial-sdk-hive-2.2.0.jar' SYMBOL='com.esri.hadoop.hive.ST_GeomFromText';
-- ... 그 외 필요한 함수들
select ST_AsText(ST_GeomFromText('LINESTRING(0 0, 1 1)')) as line_string;
```

```text
[hdw1.dd.io:21050] default> select ST_AsText(ST_GeomFromText('LINESTRING(0 0, 1 1)')) as line_string;
Query: select ST_AsText(ST_GeomFromText('LINESTRING(0 0, 1 1)')) as line_string
Query submitted at: 2026-01-22 22:55:33 (Coordinator: http://hdw1.dd.io:25000)
Query state can be monitored at: http://hdw1.dd.io:25000/query_plan?query_id=1f4a90c47e7d5e15:442a9b9900000000
+-----------------------+
| line_string           |
+-----------------------+
| LINESTRING (0 0, 1 1) |
+-----------------------+
Fetched 1 row(s) in 0.12s
[hdw1.dd.io:21050] default> 
```

## Oracle to Impala Mapping

### 1:1 대응 또는 유사한 표준 함수

Impala에서 제공하는 ST_ 계열 함수로 대체 가능합니다.

|Oracle 함수|Impala 대응 함수 / 방법|비고|
|---|---|---|
|SDO_UTIL.FROM_WKTGEOMETRY|ST_GeomFromText(string)|WKT를 Geometry 객체로 변환|
|SDO_GEOM.SDO_LENGTH|ST_Length(geometry)|선의 길이 계산|
|SDO_GEOM.SDO_DISTANCE|ST_Distance(geom1, geom2)|두 객체 간 최단 거리|
|SDO_GEOM.SDO_AREA|ST_Area(poly),면적 계산|
|SDO_GEOM.SDO_BUFFER|ST_Buffer(geom, dist)|버퍼 생성|
|SDO_GEOM.SDO_CENTROID|ST_Centroid(geom)|무게 중심점 반환|
|SDO_RELATE|ST_Intersects, ST_Contains 등|상황에 맞는 개별 위상 함수 사용|

### MBR(최소 사각형) 관련 연산

Impala는 ST_Envelope를 통해 MBR을 구하며, 좌표 추출은 별도 함수를 조합해야 합니다.

|Oracle 함수|Impala 대응 방법|
|-|-|
|SDO_GEOM.SDO_MAX_MBR_ORDINATE|ST_MaxX(ST_Envelope(geom)) / ST_MaxY(...)|
|SDO_GEOM.SDO_MIN_MBR_ORDINATE|ST_MinX(ST_Envelope(geom)) / ST_MinY(...)|

### Impala에서 지원하지 않는 함수 및 우회 방법

이 섹션의 함수들은 Impala에 내장되어 있지 않거나, 별도의 라이브러리(Esri Geometry API 등)를 사용한 UDF 등록이 필요합니다.

* 공간 좌표계 및 변환 (Critical)
  * SDO_CS.TRANSFORM: Impala는 좌표계 변환(Projection)을 자체적으로 수행하지 못합니다.
    * 우회: 데이터를 Impala에 로드하기 전에 Python(PyProj)이나 Spark를 사용하여 단일 좌표계(예: EPSG:4326 또는 3857)로 **미리 변환(Pre-processing)**하여 저장해야 합니다.
* 기하학적 복잡 연산
  * SDO_GEOM.SDO_CONCAVEHULL: 지원 불가. (ConvexHull은 ST_ConvexHull로 가능하나 Concave는 불가)
  * SDO_GEOM.SDO_CLOSEST_POINTS: 지원 불가.
  * SDO_UTIL.SIMPLIFY: ST_Generalize(geom, distance)로 유사 기능 수행 가능 (버전에 따라 확인 필요).
  * SDO_UTIL.RECTIFY_GEOMETRY: 지원 불가. (보통 데이터 적재 단계에서 정리 필요)
* LRS (Linear Referencing System)
  * SDO_LRS.SPLIT_GEOM_SEGMENT / CONVERT_TO_LRS_GEOM 등: Impala는 LRS를 지원하지 않습니다.
    * 우회: 이 로직이 꼭 SQL View에 있어야 한다면, Java/C++로 전용 UDF를 작성하여 Impala에 등록하거나, 비즈니스 로직을 GeoServer가 아닌 데이터 처리 레이어로 옮겨야 합니다.
* 구조적 편집 및 유효성 검사
  * SDO_UTIL.APPEND: ST_Union(geom1, geom2)으로 우회 가능.
  * SDO_GEOM.VALIDATE_GEOMETRY_...: ST_IsValid(geom)로 유효 여부 확인 가능.
  * SDO_UTIL.GETVERTICES: 지원 불가.
    * 우회: ST_AsText로 변환 후 문자열 파싱(정규식 등)을 하거나 전용 UDF 필요.

### Oracle Spatial vs. Impala vs. Sedona 비교 및 대체 표

|Oracle Spatial 함수|Impala (ESRI LIB)|Apache Sedona (ST_ 함수)|비고|
|---|---|---|---|
|FROM_WKTGEOMETRY|ST_GeomFromText|ST_GeomFromWKT|표준 WKT 변환|
|SDO_CONCAVEHULL|❌ 지원 불가|ST_ConcaveHull|Sedona는 파라미터로 정교함 조절 가능|
|SDO_CLOSEST_POINTS|❌ 지원 불가|ST_ClosestPoint|두 객체 간 최단 거리 지점 계산|
|SDO_CS.TRANSFORM|❌ 지원 불가|ST_Transform|Sedona의 핵심 강점 (좌표계 변환)|
|SDO_LRS.SPLIT_GEOM_SEGMENT|❌ 지원 불가|ST_SubLine / ST_LineSubstring|LRS 선형 참조 기반 분할 가능|
|SDO_UTIL.EXTRACT|❌ 지원 불가|ST_GeometryN / ST_PointN|멀티 객체에서 특정 요소 추출|
|SDO_UTIL.APPEND|ST_Union|ST_Union / ST_Collect|여러 기하 구조 합치기|
|SDO_GEOM.SDO_LENGTH|ST_Length|ST_Length|선의 길이|
|SDO_GEOM.SDO_DISTANCE|ST_Distance|ST_Distance|두 지점 간 거리|
|SDO_GEOM.SDO_BUFFER|ST_Buffer|ST_Buffer|버퍼 생성|
|CONVERT_TO_STD_GEOM|❌ (이미 표준)|❌ (자동 처리)|Sedona는 기본적으로 표준 기하학 사용|
|SDO_UTIL.RECTIFY_GEOMETRY|❌ 지원 불가|ST_MakeValid|잘못된 Geometry 구조 교정|
|SDO_GEOM.SDO_CENTROID|ST_Centroid|ST_Centroid|무게 중심|
|SDO_UTIL.SIMPLIFY|ST_Generalize|ST_SimplifyPreserveTopology|형상 단순화 (위상 유지)|
|SDO_MAX_MBR_ORDINATE|ST_MaxX, ST_MaxY|ST_XMax, ST_YMax|MBR의 최대 좌표값|
|SDO_MIN_MBR_ORDINATE|ST_MinX, ST_MinY|ST_XMin, ST_YMin|MBR의 최소 좌표값|
|SDO_GEOM.SDO_SELF_UNION|⚠️ ST_Union 활용|ST_UnaryUnion|자기 자신과의 결합 (Clean up 용도)|
|SDO_UTIL.GETNUMELEM|❌ 지원 불가|ST_NumGeometries|객체 내 요소 개수|
|CONVERT_TO_LRS_GEOM|❌ 지원 불가|ST_Force3DM / ST_AddMeasure|M 좌표(Measure)를 가진 LRS 객체 생성|
|SDO_GEOM.SDO_AREA|ST_Area|ST_Area|면적 계산|
|VALIDATE_GEOMETRY_...|ST_IsValid|ST_IsValidReason|오류 사유까지 상세 확인 가능|
|SDO_UTIL.GETVERTICES|❌ 지원 불가|ST_DumpPoints|모든 꼭짓점을 개별 포인트로 전개|
|SDO_RELATE|ST_Intersects 등|ST_Intersects, ST_Within 등|표준 위상 관계 함수로 대체|

## 참고

### Apache Impala의 UDF

* UDF, UDAF만 지원
* Native Impala UDF 지원 (C++로 구현) - C++로 구현한 UDF는 일반적으로 Java 버전 대비 10배 이상 빠름
* Hive UDF with Impala 지원
  *  `org.apache.hadoop.hive.ql.exec.UDF` 반드시 상속해야 함
  * Timestamp의 리턴은 지원하지 않음
  * UDAF, UDTF는 지원하지 않음
  * 스칼라 데이터 타입을 사용하도록 함

다음은 Java UDF를 Impala에서 사용하는 방법입니다. Impala에서 Hive UDF를 사용하려면 HDFS에 Jar 파일을 업로드한 후에 사용해야 하며, 함수의 signature를 명시적으로 지정해야 합니다.

```text
[localhost:21000] > create database udfs;
[localhost:21000] > use udfs;
localhost:21000] > create function lower(string) returns string location '/user/hive/udfs/hive.jar' symbol='org.apache.hadoop.hive.ql.udf.UDFLower';
ERROR: AnalysisException: Function cannot have the same name as a builtin: lower
[localhost:21000] > create function my_lower(string) returns string location '/user/hive/udfs/hive.jar' symbol='org.apache.hadoop.hive.ql.udf.UDFLower';
[localhost:21000] > select my_lower('Some String NOT ALREADY LOWERCASE');
+----------------------------------------------------+
| udfs.my_lower('some string not already lowercase') |
+----------------------------------------------------+
| some string not already lowercase                  |
+----------------------------------------------------+
Returned 1 row(s) in 0.11s
[localhost:21000] > create table t2 (s string);
[localhost:21000] > insert into t2 values ('lower'),('UPPER'),('Init cap'),('CamelCase');
Inserted 4 rows in 2.28s
[localhost:21000] > select * from t2;
+-----------+
| s         |
+-----------+
| lower     |
| UPPER     |
| Init cap  |
| CamelCase |
+-----------+
Returned 4 row(s) in 0.47s
[localhost:21000] > select my_lower(s) from t2;
+------------------+
| udfs.my_lower(s) |
+------------------+
| lower            |
| upper            |
| init cap         |
| camelcase        |
+------------------+
Returned 4 row(s) in 0.54s
[localhost:21000] > select my_lower(concat('ABC ',s,' XYZ')) from t2;
+------------------------------------------+
| udfs.my_lower(concat('abc ', s, ' xyz')) |
+------------------------------------------+
| abc lower xyz                            |
| abc upper xyz                            |
| abc init cap xyz                         |
| abc camelcase xyz                        |
+------------------------------------------+
Returned 4 row(s) in 0.22s
```

Impala UDF에 대한 자세한 사항은 https://impala.apache.org/docs/build/html/topics/impala_udf.html 을 참고하십시오.
또한 Cloudera의 CDP 문서인 https://docs.cloudera.com/runtime/7.3.1/impala-sql-reference/topics/impala-udf.html 을 참고하십시오.

함수 변경시 다음의 절차에 따라서 진행합니다.

* JAR 파일 업로드

```text
# 기존 파일 교체 시
hadoop fs -put -f my-udf-v2.jar /user/lib/udf/my-udf.jar
```

* 기존 함수 삭제

```sql
DROP FUNCTION IF EXISTS my_database.calculate_discount(STRING, DOUBLE);
```

* 함수 재등록

```sql
CREATE FUNCTION my_database.calculate_discount(STRING, DOUBLE)
RETURNS DOUBLE
LOCATION '/user/lib/udf/my-udf.jar'
SYMBOL='com.example.hive.udf.CalculateDiscount'; 
```
* 메타데이터 갱신 및 동기화

```sql
INVALIDATE METADATA my_database.calculate_discount;
-- 또는 간단하게
REFRESH FUNCTIONS my_database;
```