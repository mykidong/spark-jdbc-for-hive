package mykidong.datasources.jdbc.hive;

import com.fasterxml.jackson.databind.ObjectMapper;
import mykidong.meta.HiveMetaResolver;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.collection.immutable.Map;

import java.io.Serializable;
import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.*;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;

public class JdbcHiveRelation extends BaseRelation implements Serializable, TableScan {

    private SQLContext sqlContext;
    private StructType schema;
    private java.util.Map<String, String> parametersAsJava;

    private java.util.Map<String, HiveMetaResolver.HiveMetadata> hiveMetadataMap;

    public JdbcHiveRelation(SQLContext sqlContext, Map<String, String> parameters)
    {
        this.sqlContext = sqlContext;
        this.parametersAsJava = mapAsJavaMapConverter(parameters).asJava();
    }

    @Override
    public SQLContext sqlContext() {
        return this.sqlContext;
    }

    @Override
    public StructType schema() {
        if(this.schema == null)
        {
            this.buildSchema();
        }

        return this.schema;
    }

    private void buildSchema()
    {
        String dbTable = parametersAsJava.get(JdbcHiveOptions.dbTable);
        String hiveJdbcUrl = parametersAsJava.get(JdbcHiveOptions.hiveJdbcUrl);
        String hiveJdbcUser = parametersAsJava.get(JdbcHiveOptions.hiveJdbcUser);
        String hiveJdbcPassword = parametersAsJava.get(JdbcHiveOptions.hiveJdbcPassword);
        String hiveMetastoreUrl = parametersAsJava.get(JdbcHiveOptions.hiveMetastoreUrl);
        String hiveMetastoreUser = parametersAsJava.get(JdbcHiveOptions.hiveMetastoreUser);
        String hiveMetastorePassword = parametersAsJava.get(JdbcHiveOptions.hiveMetastorePassword);

        HiveMetaResolver hiveMetaResolver = new HiveMetaResolver(dbTable,
                                                                hiveJdbcUrl,
                                                                hiveJdbcUser,
                                                                hiveJdbcPassword,
                                                                hiveMetastoreUrl,
                                                                hiveMetastoreUser,
                                                                hiveMetastorePassword);

        this.schema = hiveMetaResolver.getSparkSchema();
        this.hiveMetadataMap = hiveMetaResolver.getHiveMetadataMap();
    }


    @Override
    public RDD<Row> buildScan() {
        schema();

        String dbTable = parametersAsJava.get(JdbcHiveOptions.dbTable);
        String conditionClause = parametersAsJava.get(JdbcHiveOptions.conditionClause);
        String hiveJdbcUrl = parametersAsJava.get(JdbcHiveOptions.hiveJdbcUrl);
        String hiveJdbcUser = parametersAsJava.get(JdbcHiveOptions.hiveJdbcUser);
        String hiveJdbcPassword = parametersAsJava.get(JdbcHiveOptions.hiveJdbcPassword);
        int fetchSize = Integer.parseInt(parametersAsJava.get(JdbcHiveOptions.fetchsize));
        String tempPath = parametersAsJava.get(JdbcHiveOptions.tempPath);
        tempPath = (tempPath != null) ? tempPath : "/jdbc-hive-temp";

        // TODO: limit 을 걸지 않을 경우 hive server 가 죽는다!!! 왜 그렇지???
        String query = "select * from " + dbTable + " " + conditionClause;
        System.out.println("input query: [" + query + "]");

        ObjectMapper mapper = new ObjectMapper();

        Properties properties = new Properties();
        properties.setProperty("user", hiveJdbcUser);
        properties.setProperty("password", hiveJdbcPassword);


        // TODO: hive jdbc connection 을 통해 얻어진 결과들은 temp file path 의 parquet file 에 먼저 저장이 됨.
        //       그리고  이 parquet file 들은 다시 spark dataframe 으로 loading 이 됨.
        //       결국 이 temp file path 는 dataframe loading 후 지워져야 함!!!
        String filePath = tempPath + "/" + UUID.randomUUID().toString() + "-" + UUID.randomUUID().toString();
        System.out.println("temp file path: [" + filePath + "]");

        Connection connection = null;
        try {
            connection = DriverManager.getConnection(hiveJdbcUrl, properties);

            PreparedStatement statement = connection.prepareStatement(query);
            statement.setFetchSize(fetchSize);
            ResultSet rs = statement.executeQuery();

            long count = 0;

            JavaRDD<Row> emptyRdd = new JavaSparkContext(sqlContext.sparkContext()).emptyRDD();
            Dataset<Row> df = sqlContext.createDataFrame(emptyRdd, schema);
            String dfJson = df.schema().json();
            //System.out.println("df schema in json \n" + JsonWriter.formatJson(dfJson));

            StructField[] structFieldList = schema.fields();

            List<Row> rowList = new ArrayList<>();
            while (rs.next()) {

                List<Object> rowElements = new ArrayList<>();
                for(StructField structField : structFieldList)
                {
                    String columnName = structField.name();
                    DataType dataType = structField.dataType();

                    HiveMetaResolver.HiveMetadata hiveMetadata = hiveMetadataMap.get(columnName);
                    int sqlType = hiveMetadata.getDataType();
                    int precision = hiveMetadata.getFieldSize();
                    int scale = hiveMetadata.getFieldScale();
                    boolean signed = hiveMetadata.isSigned();

                    Dataset<Row> columnDf = df.select(functions.col(columnName));
                    StructType columnSchema = columnDf.schema();
                    String columnSchemaJson = columnSchema.json();
                    //System.out.println("column schema in json\n" + JsonWriter.formatJson(columnSchemaJson));

                    java.util.Map<String, Object> columnSchemaMap = JsonUtils.toMap(mapper, columnSchemaJson);

                    Object tempResult = null;

                    switch (sqlType) {
                        case Types.ARRAY:
                            String arrayJson = rs.getString(columnName);

                            List list = JsonUtils.toList(mapper, arrayJson);
                            if(list.size() == 0)
                            {
                                tempResult = null;
                                break;
                            }

                            List<java.util.Map<String, Object>> fieldsList = (List<java.util.Map<String, Object>>) columnSchemaMap.get("fields");
                            java.util.Map<String, Object> fieldsMap = fieldsList.get(0);
                            String fieldName = (String) fieldsMap.get("name");
                            java.util.Map<String, Object> typeMap = (java.util.Map<String, Object>) fieldsMap.get("type");
                            Object elementTypeObj = typeMap.get("elementType");
                            String elementType = (String) typeMap.get("type");
                            if(elementType.equals("array"))
                            {
                                // struct array type.
                                if(elementTypeObj instanceof java.util.Map)
                                {
                                    java.util.Map<String, Object> elementTypeMap = (java.util.Map<String, Object>) elementTypeObj;

                                    List<Row> rows = iterateFieldsRecursively(elementTypeMap, list, columnName, mapper);
                                    tempResult = rows;
                                }
                                // primitive array.
                                else if(elementTypeObj instanceof String)
                                {
                                    String primitiveType = (String) elementTypeObj;

                                    List<Object> primitiveTypeArrayValues = new ArrayList<>();
                                    for(Object elementObj : list)
                                    {
                                        primitiveTypeArrayValues.add(getValueWithSparkType(elementObj, primitiveType, mapper));
                                    }

                                    tempResult = primitiveTypeArrayValues;
                                }
                            }
                            else {
                                System.err.println("not supported elementType: " + elementType);
                            }

                            break;
                        case Types.BIGINT:
                        case Types.ROWID:
                            tempResult = rs.getLong(columnName);
                            break;
                        case Types.BINARY:
                        case Types.BLOB:
                        case Types.LONGVARBINARY:
                        case Types.VARBINARY:
                            tempResult = rs.getBinaryStream(columnName);
                            break;
                        case Types.BIT:
                        case Types.BOOLEAN:
                            tempResult = rs.getBoolean(columnName);
                            break;
                        case Types.STRUCT:
                            String structJson = rs.getString(columnName);
                            java.util.Map<String, Object> structValueMap = JsonUtils.toMap(mapper, structJson);

                            List structList = new ArrayList();
                            structList.add(structValueMap);

                            List<java.util.Map<String, Object>> structFieldsList = (List<java.util.Map<String, Object>>) columnSchemaMap.get("fields");
                            java.util.Map<String, Object> structFieldsMap = structFieldsList.get(0);
                            String structFieldName = (String) structFieldsMap.get("name");
                            java.util.Map<String, Object> structSchemaMap = (java.util.Map<String, Object>) structFieldsMap.get("type");


                            List<Row> rows = iterateFieldsRecursively(structSchemaMap, structList, columnName, mapper);

                            tempResult = rows.get(0);

                            break;
                        case Types.CHAR:
                        case Types.CLOB:
                        case Types.LONGNVARCHAR:
                        case Types.LONGVARCHAR:
                        case Types.NCHAR:
                        case Types.NCLOB:
                        case Types.NVARCHAR:
                        case Types.REF:
                        case Types.SQLXML:
                        case Types.VARCHAR:
                            tempResult = rs.getString(columnName);
                            break;
                        case Types.DATE:
                            tempResult = rs.getDate(columnName);
                            break;
                        case Types.DECIMAL:
                        case Types.NUMERIC:
                            tempResult = rs.getDouble(columnName);
                            break;
                        case Types.DOUBLE:
                        case Types.REAL:
                            tempResult = rs.getDouble(columnName);
                            break;
                        case Types.FLOAT:
                            tempResult = rs.getFloat(columnName);
                            break;
                        case Types.INTEGER:
                            if (signed) {
                                tempResult = rs.getInt(columnName);
                            } else {
                                tempResult = rs.getLong(columnName);
                            }
                            break;
                        case Types.SMALLINT:
                        case Types.TINYINT:
                            tempResult = rs.getInt(columnName);
                            break;
                        case Types.TIME:
                        case Types.TIMESTAMP:
                            tempResult = rs.getTimestamp(columnName);
                            break;
                        default:
                            tempResult = null;
                    }

                    //System.out.printf("temp result: %s\n", tempResult);

                    rowElements.add(tempResult);
                }

                Row finalRow = RowFactory.create(rowElements.toArray(new Object[0]));

                rowList.add(finalRow);

                if(count % fetchSize == 0)
                {
                    if(count > 0) {
                        System.out.println("fetch count: [" + count + "]");

                        // fetch size 만큼 모아진 Row List 를 file 에 저장.
                        appendFetchedData(sqlContext.sparkSession(), rowList, schema, filePath);

                        // rowList refresh.
                        rowList = new ArrayList<>();
                    }
                }

                count++;
            }

            // row list 에 남아 있는 fetch data 를 append.
            if(rowList.size() > 0)
            {
                System.out.println("row list remained: [" + rowList.size() + "]");

                appendFetchedData(sqlContext.sparkSession(), rowList, schema, filePath);
            }

            statement.close();
            if(connection != null)
            {
                connection.close();
            }
        } catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException(e.getCause());
        }

        Dataset<Row> df = sqlContext.read().format("parquet").load(filePath);

        return df.rdd();
    }

    private void appendFetchedData(SparkSession spark, List<Row> rowList, StructType schema, String filePath)
    {
        if(rowList.size() > 0) {
            spark.createDataFrame(rowList, schema).write()
                    .format("parquet")
                    .mode(SaveMode.Append)
                    .save(filePath);

            System.out.println("row list saved to the temp file: [" + filePath + "]");

        }
    }

    private List<Row> iterateFieldsRecursively(java.util.Map<String, Object> parentSchema,
                                               List arrayValues,
                                               String columnName,
                                               ObjectMapper mapper) throws Exception
    {
        // array 를 구성하는 row element list.
        List<Row> rowElements = new ArrayList<>();

        // array 값 loop.
        for(Object arrayValue : arrayValues) {
            // array 내의 struct 값.
            java.util.Map<String, Object> arrayValueMap = (java.util.Map<String, Object>) arrayValue;

            // array 내부의 struct row element list.
            List<Object> structRowElements = new ArrayList<>();

            List<java.util.Map<String, Object>> fieldsList = (List<java.util.Map<String, Object>>) parentSchema.get("fields");

            // schema fields loop.
            for (java.util.Map<String, Object> structMap : fieldsList) {
                String fieldName = (String) structMap.get("name");
                Object typeObj = structMap.get("type");

                // array type.
                if (typeObj instanceof java.util.Map) {
                    java.util.Map<String, Object> innerStructArrayMap = (java.util.Map<String, Object>) typeObj;
                    String innerType = (String) innerStructArrayMap.get("type");

                    boolean hasElementType = innerStructArrayMap.containsKey("elementType");

                    List innerArrays = (List) arrayValueMap.get(fieldName);

                    if(hasElementType) {
                        Object innerElementTypeObj = innerStructArrayMap.get("elementType");
                        if (innerType.equals("array")) {
                            // struct array type.
                            if (innerElementTypeObj instanceof java.util.Map) {
                                java.util.Map<String, Object> elementTypeMap = (java.util.Map<String, Object>) innerElementTypeObj;

                                List<Row> rows = iterateFieldsRecursively(elementTypeMap, innerArrays, fieldName, mapper);

                                //System.out.println("rows: " + rows.toString());

                                structRowElements.add(rows);
                            }
                            // primitive array type.
                            else if (innerElementTypeObj instanceof String) {
                                String primitiveType = (String) innerElementTypeObj;

                                List<Object> primitiveTypeArrayValues = new ArrayList<>();

                                for (Object elementObj : innerArrays) {
                                    primitiveTypeArrayValues.add(getValueWithSparkType(elementObj, primitiveType, mapper));
                                }

                                structRowElements.add(primitiveTypeArrayValues);
                            }
                        }
                    }
                    // struct type.
                    else if(innerType.equals("struct")) {
                        Object arrayElementValue = arrayValueMap.get(fieldName);

                        List structList = new ArrayList();
                        structList.add(arrayElementValue);

                        List<Row> rows = iterateFieldsRecursively(innerStructArrayMap, structList, fieldName, mapper);
                        structRowElements.add(rows.get(0));
                    }
                }
                // primitive type.
                else if (typeObj instanceof String) {
                    String primitiveType = (String) typeObj;

                    Object arrayElementValue = arrayValueMap.get(fieldName);

                    Object value = getValueWithSparkType(arrayElementValue, primitiveType, mapper);

                    //System.out.printf("paranetFieldName: %s, fieldName: %s, type: %s, valueType: %s, value: %s\n", columnName, fieldName, primitiveType, value.getClass(), value);

                    structRowElements.add(value);
                }
            }
            //System.out.println("fieldName: [" + columnName + "], struct: [" + structRowElements.toString() + "]");

            rowElements.add(RowFactory.create(structRowElements.toArray(new Object[0])));
        }

        return rowElements;
    }

    private Object getValueWithSparkType(Object obj, String sparkType, ObjectMapper mapper) throws Exception
    {
        Object value = null;
        switch (sparkType) {
            case "bigint":
            case "long":
                if(obj instanceof Integer)
                {
                    value = Long.valueOf((int)obj);
                    break;
                }
                else if(obj instanceof Long) {
                    value = Long.valueOf((long) obj);
                    break;
                }

                break;
            case "binary":
                byte[] data = (obj != null) ? mapper.convertValue((String) obj, byte[].class) : null;
                value = data;
                break;
            case "boolean":
                value = Boolean.valueOf((boolean) obj);
                break;
            case "string":
                value = String.valueOf((String) obj);
                break;
            case "date":
                String dateString = (String) obj;
                value = getDateFromString(dateString);
                break;
            case "decimal":
            case "double":
                value = Double.valueOf((double) obj);
                break;
            case "float":
                value = Float.valueOf((float) obj);
                break;
            case "int":
            case "integer":
                value = Integer.valueOf((int) obj);
                break;
            case "timestamp":
                value = new Timestamp(getDateFromString((String) obj).getTime());
                break;
            default:
                value = null;
                break;
        }

        return value;
    }

    private Date getDateFromString(String dateString) throws Exception
    {
        final DateFormat fmt;
        if (dateString.endsWith("Z")) {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        } else {
            fmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
        }
        return fmt.parse(dateString);
    }
}
