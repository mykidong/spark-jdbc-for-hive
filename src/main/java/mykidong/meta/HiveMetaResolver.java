/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package mykidong.meta;

import org.apache.commons.io.IOUtils;
import org.apache.spark.sql.types.*;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class HiveMetaResolver {

    private String dbTable;
    private String hiveJdbcUrl;
    private String hiveJdbcUser;
    private String hiveJdbcPassword;
    private String hiveMetastoreUrl;
    private String hiveMetastoreUser;
    private String hiveMetastorePassword;

    private StructType sparkSchema;
    private Map<String, HiveMetadata> hiveMetadataMap;


    public HiveMetaResolver(String dbTable,
                            String hiveJdbcUrl,
                            String hiveJdbcUser,
                            String hiveJdbcPassword,
                            String hiveMetastoreUrl,
                            String hiveMetastoreUser,
                            String hiveMetastorePassword)
    {
        this.dbTable = dbTable;

        this.hiveJdbcUrl = hiveJdbcUrl;
        this.hiveJdbcUser = hiveJdbcUser;
        this.hiveJdbcPassword = hiveJdbcPassword;

        this.hiveMetastoreUrl = hiveMetastoreUrl;
        this.hiveMetastoreUser = hiveMetastoreUser;
        this.hiveMetastorePassword = hiveMetastorePassword;

        convertHiveTypeToSparkType();
    }

    private void convertHiveTypeToSparkType()
    {
        // jdbc 를 통해 hive table 의 meta 정보를 얻음.
        hiveMetadataMap = getMeta();
        //System.out.printf("before setting hive type - hiveMetadataMap: [%s]\n", JsonWriter.formatJson(new ObjectMapper().writeValueAsString(hiveMetadataMap)));

        // hive metastore 에서 hive table 의 schema 정보를 얻음.
        Map<String, HiveTableSchema> hiveTableSchemaMap = getHiveTableSchema();
        //System.out.printf("hiveTableSchemaMap: [%s]\n", JsonWriter.formatJson(new ObjectMapper().writeValueAsString(hiveTableSchemaMap)));

        // hive column type 을 set.
        for(String columnName : hiveTableSchemaMap.keySet())
        {
            System.out.println("columnName: [" + columnName + "]");

            String hiveColumnType = hiveTableSchemaMap.get(columnName).getColumnType();
            hiveMetadataMap.get(columnName).setHiveColumnType(hiveColumnType);
        }

        //System.out.printf("after setting hive type - hiveMetadataMap: [%s]\n", JsonWriter.formatJson(new ObjectMapper().writeValueAsString(hiveMetadataMap)));

        List<StructField> structFields = new ArrayList<StructField>();

        // sort hive metadata list by index.
        List<HiveMetadata> hiveMetadataList = new ArrayList<>(hiveMetadataMap.values());
        Collections.sort(hiveMetadataList, (o1, o2) -> {
            return o1.getIndex() - o2.getIndex();
        });

        for(HiveMetadata hiveMetadata : hiveMetadataList)
        {
            String columnName = hiveMetadata.getColumnName();

            int sqlType = hiveMetadata.getDataType();
            int precision = hiveMetadata.getFieldSize();
            int scale = hiveMetadata.getFieldScale();
            boolean signed = hiveMetadata.isSigned;
            boolean nullable = hiveMetadata.nullable;
            String hiveColumnType = hiveMetadata.getHiveColumnType();

            // TODO: Array 와 Struct 를 제외한 Data Type 은 Primitive Type 으로 간주.
            //       하지만 Hive Complex Type 인 예를들어 Map Type 등도 확대해야 함.
            if(sqlType == Types.ARRAY) {
                // array struct type 일 경우: 예를들어, array<struct<contents:string,date:string,like_cnt:bigint>>
                if(hiveColumnType.startsWith("array<struct")) {
                    String rootParentKey = "0:root|0:array<struct>";

                    StructType arrayStructType = buildStructTypeWithHiveSchema(hiveColumnType, rootParentKey);
                    structFields.add(DataTypes.createStructField(columnName, ArrayType.apply(arrayStructType), nullable));
                }
                // array 내 primitive type 일 경우: 예를들어, array<string>
                else
                {
                    int startIndex = hiveColumnType.indexOf("<");
                    int endIndex = hiveColumnType.indexOf(">");

                    String primitiveType = hiveColumnType.substring(startIndex + 1, endIndex);

                    DataType dataType = hiveTypeToCatalystType(primitiveType);
                    dataType = (dataType == null) ? DataTypes.StringType : dataType;
                    structFields.add(DataTypes.createStructField(columnName, ArrayType.apply(dataType), nullable));
                }
            }
            else if(sqlType == Types.STRUCT) {
                String rootParentKey = "0:root|0:struct";
                StructType structType = buildStructTypeWithHiveSchema(hiveColumnType, rootParentKey);

                structFields.add(DataTypes.createStructField(columnName, structType, nullable));
            }
            else {
                // hive column type 값이 null 일 경우 partition column 으로 간주.
                // partition column 은 포함하지 않음.
                if(hiveColumnType != null) {
                    DataType dataType = sqlTypeToCatalystType(sqlType, precision, scale, signed);
                    structFields.add(DataTypes.createStructField(columnName, dataType, nullable));
                }
            }
        }

        StructType schema = DataTypes.createStructType(structFields);

        // print spark schema.
        schema.printTreeString();

        this.sparkSchema = schema;
    }


    public StructType getSparkSchema()
    {
        return sparkSchema;
    }

    public Map<String, HiveMetadata> getHiveMetadataMap() {
        return hiveMetadataMap;
    }

    public static class HiveMetadata
    {
        private String dbTable;
        private int index;
        private String columnName;
        private int dataType;
        private String typeName;
        private int fieldSize;
        private int fieldScale;
        private boolean isSigned;
        private boolean nullable;
        private String hiveColumnType;

        public String getDbTable() {
            return dbTable;
        }

        public void setDbTable(String dbTable) {
            this.dbTable = dbTable;
        }

        public String getHiveColumnType() {
            return hiveColumnType;
        }

        public void setHiveColumnType(String hiveColumnType) {
            this.hiveColumnType = hiveColumnType;
        }

        public int getIndex() {
            return index;
        }

        public void setIndex(int index) {
            this.index = index;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public int getDataType() {
            return dataType;
        }

        public void setDataType(int dataType) {
            this.dataType = dataType;
        }

        public String getTypeName() {
            return typeName;
        }

        public void setTypeName(String typeName) {
            this.typeName = typeName;
        }

        public int getFieldSize() {
            return fieldSize;
        }

        public void setFieldSize(int fieldSize) {
            this.fieldSize = fieldSize;
        }

        public int getFieldScale() {
            return fieldScale;
        }

        public void setFieldScale(int fieldScale) {
            this.fieldScale = fieldScale;
        }

        public boolean isSigned() {
            return isSigned;
        }

        public void setSigned(boolean signed) {
            isSigned = signed;
        }

        public boolean isNullable() {
            return nullable;
        }

        public void setNullable(boolean nullable) {
            this.nullable = nullable;
        }
    }

    private static class HiveTableSchema
    {
        private String dbTable;
        private String columnName;
        private String columnType;

        public String getDbTable() {
            return dbTable;
        }

        public void setDbTable(String dbTable) {
            this.dbTable = dbTable;
        }

        public String getColumnName() {
            return columnName;
        }

        public void setColumnName(String columnName) {
            this.columnName = columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        public void setColumnType(String columnType) {
            this.columnType = columnType;
        }
    }

    private static class ColumnSchema
    {
        // key(level:columnName)
        private String key;
        private String columnName;
        private String columnType;
        private String parentKey;
        private int level;
        private int position;

        public ColumnSchema(int level, String hiveColumnSchema)
        {
            this(level, hiveColumnSchema, 0, null);
        }

        public ColumnSchema(int level, String hiveColumnSchema, int position)
        {
            this(level, hiveColumnSchema, position, null);
        }

        public ColumnSchema(int level, String hiveColumnSchema, int position, String parentKey)
        {
            this.level = level;
            this.position = position;
            this.parentKey = parentKey;

            String[] tokens = hiveColumnSchema.split(":");
            this.columnName = tokens[0];
            if(tokens.length > 1) {
                this.columnType = tokens[1];
            }

            this.key = this.parentKey + "|" + this.level + ":" + columnName;
        }

        public String getKey() {
            return key;
        }

        public String getColumnName() {
            return columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        public String getParentKey() {
            return parentKey;
        }

        public int getLevel() {
            return level;
        }

        public int getPosition() {
            return position;
        }
    }

    /**
     * hive array / struct column type string 을 parsing 하여 hierarchical 한 map 구조를 return.
     *
     * map 의 key는 구조 depth(level) 별 parent key 들의 연속인  [parent-key]|[parent-key2]|... 로 이뤄졌으며,
     * 각각의 [parent-key] 는 [level]:[column-name] 으로 구성됨.
     *
     * @param hiveColumnType
     * @return parentKey 별 column schema list 형식의 map 을 return.
     *
     * @throws Exception
     */
    private Map<String, List<ColumnSchema>> buildColumnGroup(String hiveColumnType)
    {
        int currentIndex = hiveColumnType.indexOf(",");
        String currentString = hiveColumnType;
        int level = 0;

        // parentkey(level:parentColumn) 별 column list.
        Map<String, List<ColumnSchema>> parentKeyGroupMap = new HashMap<>();

        // level 별 current parent key.
        Map<Integer, String> levelParentKeyMap = new HashMap<>();

        String parentKey = "";

        while(currentIndex > -1)
        {
            String subString = currentString.substring(0, currentIndex);
            //System.out.println("subString: [" + subString + "], level: [" + level + "]");

            String rest = currentString.substring(currentIndex + 1);
            //System.out.println("rest: [" + rest + "]");

            // substring 안에 decimal type 이 포함되어 있을 경우.
            if(subString.contains("decimal(")){
                int decimalIndex = rest.indexOf(",");
                String subStringPrefix = rest.substring(0, decimalIndex);

                subString = subString + "," + subStringPrefix;

                rest = rest.substring(decimalIndex + 1);
            }

            if(!levelParentKeyMap.containsKey(level)) {
                levelParentKeyMap.put(level, makeParentKey(level, "root:"));
            }

            parentKey = levelParentKeyMap.get(level);
            //System.out.println("subString: [" + subString + "], level: [" + level + "], parentKey: [" + parentKey + "]");

            updateColumnGroup(level, parentKeyGroupMap, parentKey, subString);

            if(subString.contains("array<")) {
                String arrayStructString = "array<struct";
                // struct array type 일 경우.
                if (subString.contains(arrayStructString)) {
                    int arrayIndex = subString.indexOf(arrayStructString);
                    String arrayType = subString.substring(0, arrayIndex + arrayStructString.length()) + ">";
                    //System.out.println("arrayType: [" + arrayType + "], level: [" + level + "], parentKey: [" + parentKey + "]");

                    removeLatestColumnGroup(level, parentKeyGroupMap, parentKey);
                    updateColumnGroup(level, parentKeyGroupMap, parentKey, arrayType);

                    // array struct 안의 element 는 level 을 올려야 함.
                    level = level + 2;


                    parentKey = parentKey + "|" + makeParentKey(level - 2, arrayType);
                    levelParentKeyMap.put(level, parentKey);

                    subString = subString.substring(arrayIndex + arrayStructString.length() + 1);
                    //System.out.println("subString in array struct: [" + subString + "], level: [" + level + "], parentKey: [" + parentKey + "]");

                    updateColumnGroup(level, parentKeyGroupMap, parentKey, subString);
                }
                // primitive array type 일 경우.
                else
                {
                    int arrayPrimitiveIndex = subString.indexOf(">");
                    String arrayType = subString.substring(0, arrayPrimitiveIndex + 1);
                    //System.out.println("parentArray array primitive: [" + arrayType + "], level: [" + level + "], parentKey: [" + parentKey + "]");

                    removeLatestColumnGroup(level, parentKeyGroupMap, parentKey);
                    updateColumnGroup(level, parentKeyGroupMap, parentKey, arrayType);
                }
            }
            else if(subString.contains("struct"))
            {
                String structString = "struct";
                int structIndex = subString.indexOf(structString);
                String structType = subString.substring(0, structIndex + structString.length());
                //System.out.println("structType: [" + structType + "], level: [" + level + "], parentKey: [" + parentKey + "]");

                removeLatestColumnGroup(level, parentKeyGroupMap, parentKey);
                updateColumnGroup(level, parentKeyGroupMap, parentKey, structType);

                // array struct 안의 element 는 level 을 올려야 함.
                level = level + 1;

                parentKey = parentKey + "|" + makeParentKey(level - 1, structType);
                levelParentKeyMap.put(level, parentKey);

                subString = subString.substring(structIndex + structString.length() + 1);
                //System.out.println("subString in struct: [" + subString + "], level: [" + level + "], parentKey: [" + parentKey + "]");

                updateColumnGroup(level, parentKeyGroupMap, parentKey, subString);
            }
            // struct 종료 character 를 만날경우.
            else if(subString.contains(">"))
            {
                long count = subString.chars().filter(ch -> ch == '>').count();

                int structCloseIndex = subString.indexOf(">");
                subString = subString.substring(0, structCloseIndex);
                //System.out.println("subString in closed struct: [" + subString + "], level: [" + level + "], parentKey: [" + parentKey + "]");

                removeLatestColumnGroup(level, parentKeyGroupMap, parentKey);
                updateColumnGroup(level, parentKeyGroupMap, parentKey, subString);

                level = level - Long.valueOf(count).intValue();
            }

            long commaCount = rest.chars().filter(ch -> ch == ',').count();
            // rest string 안에 comma 가 없을 경우, 즉 마지막 token 일 경우.
            if(commaCount == 0)
            {
                int structCloseIndex = rest.indexOf(">");
                if(structCloseIndex > -1) {
                    rest = rest.substring(0, structCloseIndex);
                }
                //System.out.println("rest: [" + rest + "], level: [" + level + "], parentKey: [" + parentKey + "]");

                updateColumnGroup(level, parentKeyGroupMap, parentKey, rest);
            }

            currentString = rest;
            currentIndex = currentString.indexOf(",");
        }

        return parentKeyGroupMap;
    }

    private String makeParentKey(int level, String subString)
    {
        String[] tokens = subString.split(":");
        String columnName = tokens[0];

        return level + ":" + columnName;
    }

    private Map<String, HiveMetadata> getMeta()
    {
        String query = "select * from " + this.dbTable + " where 1 = 0";

        System.out.printf("query: [%s]\n", query);

        Properties properties = new Properties();
        properties.setProperty("user", this.hiveJdbcUser);
        properties.setProperty("password", this.hiveJdbcPassword);

        Map<String, HiveMetadata> hiveMetadataMap = new HashMap<>();
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(this.hiveJdbcUrl, properties);

            PreparedStatement statement = connection.prepareStatement(query);
            ResultSet rs = statement.executeQuery();

            ResultSetMetaData rsmd = rs.getMetaData();
            int columnCount = rsmd.getColumnCount();
            int i = 0;
            while (i < columnCount) {
                String columnName = rsmd.getColumnLabel(i + 1);
                columnName = columnName.toLowerCase();

                // table prefix 를 제거.
                int strIndex = columnName.indexOf(".");
                if (strIndex > -1) {
                    String[] tokens = columnName.split("\\.");

                    columnName = tokens[tokens.length - 1];
                }


                int dataType = rsmd.getColumnType(i + 1);
                String typeName = rsmd.getColumnTypeName(i + 1);
                int fieldSize = rsmd.getPrecision(i + 1);
                int fieldScale = rsmd.getScale(i + 1);
                boolean isSigned = true;
                try {
                    isSigned = rsmd.isSigned(i + 1);
                } catch (Exception e) {
                    //e.printStackTrace();
                }
                boolean nullable = true;
                try {
                    nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls;
                } catch (Exception e) {
                    //e.printStackTrace();
                }

//            System.out.printf("columnCount: [%d], columnName: [%s], dataType: [%d], typeName: [%s], fieldSize: [%d], fieldScale: [%d], isSigned: [%s], nullable: [%s]\n",
//                    (i + 1), columnName, dataType, typeName, fieldSize, fieldScale, String.valueOf(isSigned), String.valueOf(nullable));


                HiveMetadata hiveMetadata = new HiveMetadata();
                hiveMetadata.setDbTable(dbTable);
                hiveMetadata.setIndex(i + 1);
                hiveMetadata.setColumnName(columnName);
                hiveMetadata.setDataType(dataType);
                hiveMetadata.setTypeName(typeName);
                hiveMetadata.setFieldSize(fieldSize);
                hiveMetadata.setFieldScale(fieldScale);
                hiveMetadata.setSigned(isSigned);
                hiveMetadata.setNullable(nullable);

                hiveMetadataMap.put(hiveMetadata.getColumnName(), hiveMetadata);

                i++;
            }
        }catch (Exception e)
        {
            e.printStackTrace();
            throw new RuntimeException(e.getCause());
        } finally {
            try {
                if(connection != null) {
                    connection.close();
                }
            } catch (Exception e)
            {
            }
        }

        return hiveMetadataMap;
    }

    public static DataType sqlTypeToCatalystType(int sqlType, int precision, int scale, boolean signed) {
        switch (sqlType) {
            case Types.BIGINT:
                if (signed) {
                    return DataTypes.LongType;
                }
                else {
                    return new DecimalType(20,0);
                }
            case Types.BINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
            case Types.VARBINARY:
                return DataTypes.BinaryType;
            case Types.BIT:
            case Types.BOOLEAN:
                return DataTypes.BooleanType;
            case Types.CHAR:
            case Types.CLOB:
            case Types.LONGNVARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NCLOB:
            case Types.NVARCHAR:
            case Types.REF:
            case Types.SQLXML:
            case Types.STRUCT:
            case Types.VARCHAR:
                return DataTypes.StringType;
            case Types.DATE:
                return DataTypes.DateType;
            case Types.DECIMAL:
            case Types.NUMERIC:
                return DataTypes.DoubleType;
            case Types.DISTINCT:
                return null;
            case Types.DOUBLE:
            case Types.REAL:
                return DataTypes.DoubleType;
            case Types.FLOAT:
                return DataTypes.FloatType;
            case Types.INTEGER:
                if (signed) {
                    return DataTypes.IntegerType;
                } else {
                    return DataTypes.LongType;
                }
            case Types.ROWID:
                return DataTypes.LongType;
            case Types.SMALLINT:
            case Types.TINYINT:
                return DataTypes.IntegerType;
            case Types.TIME:
            case Types.TIMESTAMP:
                return DataTypes.TimestampType;
            default:  return null;
        }
    }

    public static String fileToString(String filePath) {
        try (InputStream inputStream = new ClassPathResource(filePath).getInputStream())  {
            return IOUtils.toString(inputStream);
        } catch (IOException ie) {
            throw new RuntimeException(ie);
        }
    }

    /**
     * get hive table schema from hive metastore.
     *
     */
    private Map<String, HiveTableSchema> getHiveTableSchema()
    {
        String metaStoreSql = fileToString("/data/hive-metastore.sql");
        metaStoreSql = metaStoreSql.replaceAll("#dbTable#", this.dbTable);
        System.out.println("metaStoreSql: [" + metaStoreSql + "]");

        Properties properties = new Properties();
        properties.setProperty("user", this.hiveMetastoreUser);
        properties.setProperty("password", this.hiveMetastorePassword);

        Map<String, HiveTableSchema> hiveTableSchemaMap = new HashMap<>();
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(this.hiveMetastoreUrl, properties);

            PreparedStatement statement = connection.prepareStatement(metaStoreSql);
            ResultSet rs = statement.executeQuery();
            while (rs.next()) {
                String columnName = rs.getString("column_name");
                String columnType = rs.getString("column_type");

                HiveTableSchema hiveTableSchema = new HiveTableSchema();
                hiveTableSchema.setDbTable(dbTable);
                hiveTableSchema.setColumnName(columnName);
                hiveTableSchema.setColumnType(columnType);

                hiveTableSchemaMap.put(hiveTableSchema.getColumnName(), hiveTableSchema);
            }
        } catch (Exception e)
        {
            throw new RuntimeException(e.getCause());
        } finally {
            if(connection != null)
            {
                try {
                    connection.close();
                } catch (Exception e) {}
            }
        }

        return hiveTableSchemaMap;
    }

    private void updateColumnGroup(int level, Map<String, List<ColumnSchema>> parentKeyGroupMap, String parentKey, String subString) {
        List<ColumnSchema> columnGroup = null;
        if(!parentKeyGroupMap.containsKey(parentKey))
        {
            columnGroup = new ArrayList<>();
        }
        else {
            columnGroup = parentKeyGroupMap.get(parentKey);
        }

        int position = columnGroup.size();

        ColumnSchema columnSchema = new ColumnSchema(level, subString, position, parentKey);
        columnGroup.add(columnSchema);
        parentKeyGroupMap.put(columnSchema.getParentKey(), columnGroup);
    }


    private void removeLatestColumnGroup(int level, Map<String, List<ColumnSchema>> parentKeyGroupMap, String parentKey) {
        List<ColumnSchema> columnGroup = parentKeyGroupMap.get(parentKey);
        int size = columnGroup.size();
        columnGroup.remove(size -1);
        parentKeyGroupMap.put(parentKey, columnGroup);
    }


    /**
     * array struct type 의 hive column schema 를 spark schema 로 변경.
     *
     * @param hiveColumnType
     * @param rootParentKey
     * @return
     * @throws Exception
     */
    private StructType buildStructTypeWithHiveSchema(String hiveColumnType, String rootParentKey)
    {
        Map<String, List<ColumnSchema>> parentKeyColumnGroupMap = buildColumnGroup(hiveColumnType);

        List<ColumnSchema> columnSchemas = parentKeyColumnGroupMap.get(rootParentKey);

        List<StructField> structFields = new ArrayList<StructField>();
        for(ColumnSchema columnSchema : columnSchemas)
        {
            String key = columnSchema.getKey();
            String parentKey = columnSchema.getParentKey();
            String columnName = columnSchema.getColumnName();
            String columnType = columnSchema.getColumnType();
            int position = columnSchema.getPosition();

            DataType dataType = null;
            if(columnType.equals("array<struct>"))
            {
                dataType = getRecursiveStructTypeWithHiveSchema(parentKeyColumnGroupMap, key);
                structFields.add(DataTypes.createStructField(columnName, ArrayType.apply(dataType), true));
            }
            else if(columnType.equals("struct"))
            {
                dataType = getRecursiveStructTypeWithHiveSchema(parentKeyColumnGroupMap, key);
                structFields.add(DataTypes.createStructField(columnName, dataType, true));
            }
            else {
                dataType = hiveTypeToCatalystType(columnType);
                dataType = (dataType == null) ? DataTypes.StringType : dataType;
                structFields.add(DataTypes.createStructField(columnName, dataType, true));
            }
        }

        return DataTypes.createStructType(structFields);
    }

    /**
     * array struct type 의 hive schema 를 spark schema 로 변경.
     * 이때 array struct 내에 또다른 array struct 가 존재할수 있기 때문에 recursive 하게 실행.
     *
     * @param parentKeyColumnGroupMap
     * @param parentKey
     * @return
     */
    private StructType getRecursiveStructTypeWithHiveSchema(Map<String, List<ColumnSchema>> parentKeyColumnGroupMap, String parentKey)
    {
        List<ColumnSchema> columnSchemas = parentKeyColumnGroupMap.get(parentKey);

        List<StructField> structFields = new ArrayList<StructField>();
        for(ColumnSchema columnSchema : columnSchemas)
        {
            String key = columnSchema.getKey();
            String parentKeyCurrent = columnSchema.getParentKey();
            String columnName = columnSchema.getColumnName();
            String columnType = columnSchema.getColumnType();
            int position = columnSchema.getPosition();

            DataType dataType = null;
            if(columnType.equals("array<struct>"))
            {
                dataType = getRecursiveStructTypeWithHiveSchema(parentKeyColumnGroupMap, key);
                structFields.add(DataTypes.createStructField(columnName, ArrayType.apply(dataType), true));
            }
            else if(columnType.equals("struct"))
            {
                dataType = getRecursiveStructTypeWithHiveSchema(parentKeyColumnGroupMap, key);
                structFields.add(DataTypes.createStructField(columnName, dataType, true));
            }
            else {
                dataType = hiveTypeToCatalystType(columnType);
                dataType = (dataType == null) ? DataTypes.StringType : dataType;
                structFields.add(DataTypes.createStructField(columnName, dataType, true));
            }
        }

        return DataTypes.createStructType(structFields);
    }


    public static DataType hiveTypeToCatalystType(String hiveType)
    {
        int index = hiveType.indexOf("(");
        String hiveTypePrefix = hiveType;
        if(index > -1)
        {
            hiveTypePrefix = hiveType.split("\\(")[0];
        }

        hiveTypePrefix = hiveTypePrefix.toLowerCase();

        switch (hiveTypePrefix) {
            case "tinyint":
                return DataTypes.ByteType;
            case "smallint":
                return DataTypes.ShortType;
            case "int":
            case "integer":
                return DataTypes.IntegerType;
            case "bigint":
                return DataTypes.LongType;
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            case "decimal":
                Pattern pattern = Pattern.compile("(\\d+)");
                Matcher matcher = pattern.matcher(hiveType);

                List<Integer> numberList = new ArrayList<>();
                while (matcher.find()) {
                    numberList.add(Integer.parseInt(matcher.group()));
                }

                int precision = numberList.get(0);
                int scale = numberList.get(1);

                return new DecimalType(precision, scale);
            case "string":
            case "varchar":
                return DataTypes.StringType;
            case "binary":
                return DataTypes.BinaryType;
            case "boolean":
                return DataTypes.BooleanType;
            case "timestamp":
                return DataTypes.TimestampType;
            case "date":
                return DataTypes.DateType;
            default:
                return null;
        }
    }

}
