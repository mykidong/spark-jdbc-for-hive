package mykidong.datasources.jdbc.hive;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class JsonUtils {

    public static Map<String, Object> toMap(ObjectMapper mapper, String json)
    {
        try {
            Map<String, Object> map = mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
            return map;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List toList(ObjectMapper mapper, String json)
    {
        try {
            List list = mapper.readValue(json, new TypeReference<List>() {});
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String toJson(ObjectMapper mapper, Object obj)
    {
        try {
            return mapper.writeValueAsString(obj);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
