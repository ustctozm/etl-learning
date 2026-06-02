package net.qihoo.ads.flink.ticdc;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * detail field could reference following url
 * <a href="https://github.com/pingcap/docs-cn/blob/master/ticdc/ticdc-canal-json.md">...</a>
 */
public class TiCDCMetaData implements Serializable {

    private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();

     public enum Type {
         UPDATE,
         DELETE,
         INSERT,
         OTHER
    }

    private final int id;
    private final String database;
    private final String tableName;
    private final Boolean isDdl;
    private final Type type;
    // 产生该条消息的事件发生时的13位时间戳
    private final Long eventTimestamp;
    // TiCDC 生成该条消息时的13位时间戳
    private final Long tiSinkTimestamp;
    private final HashMap<String, String> dataType;
    private final HashMap<String, String> oldData;
    private final HashMap<String, String> newData;
    public static TiCDCMetaData generateTiCDCMetaData(String tiCDCJsonInfoStr) {
        return generateTiCDCMetaData(tiCDCJsonInfoStr, null);
    }
    public static TiCDCMetaData generateTiCDCMetaData(byte[] tiCDCJsonInfoBytes) {
        return generateTiCDCMetaData(tiCDCJsonInfoBytes, null);
    }
    public static TiCDCMetaData generateTiCDCMetaData(String tiCDCJsonInfoStr, List<String> tables) {
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonMap = objectMapper.readValue(tiCDCJsonInfoStr, new TypeReference<HashMap<String, Object>>() {});
            return transformJsonMap2TiCDCMetaData(jsonMap, tables);
        } catch (IOException e) {
            LOG.error("generate TiCDCMetaData by canal json failed! ", e);
            return null;
        }
    }
    public static TiCDCMetaData generateTiCDCMetaData(byte[] tiCDCJsonInfoBytes, List<String> tables) {
        try{
            ObjectMapper objectMapper = new ObjectMapper();
            Map<String, Object> jsonMap = objectMapper.readValue(tiCDCJsonInfoBytes, new TypeReference<HashMap<String, Object>>() {});
            return transformJsonMap2TiCDCMetaData(jsonMap, tables);
        } catch (IOException e) {
            LOG.error("generate TiCDCMetaData by canal json failed! ", e);
            return null;
        }
    }

    public static TiCDCMetaData transformJsonMap2TiCDCMetaData(Map<String, Object> jsonMap, List<String> tables) {
        String tableName = (String) jsonMap.get("table");
        if (tables!=null && tables.size() >0) {
            if (!tables.contains(tableName)) {
                return null;
            }
        }
        Integer id = (Integer) jsonMap.get("id");
        String database = (String) jsonMap.get("database");
        Boolean isDdl = (Boolean) jsonMap.get("isDdl");
        String typeString = (String) jsonMap.get("type");
        Type type;
        switch (typeString){
            case "DELETE":
                type = Type.DELETE;
                break;
            case "UPDATE":
                type = Type.UPDATE;
                break;
            case "INSERT":
                type = Type.INSERT;
                break;
            default:
                type = Type.OTHER;
        }
        if (type == Type.OTHER) {
            return null;
        }
        Long eventTimestamp = (Long) jsonMap.get("es");
        Long tiSinkTimestamp = (Long) jsonMap.get("ts");

        HashMap<String, String> dataType =  (HashMap<String, String>) jsonMap.get("mysqlType");
        List<Object> oldDataList  = (ArrayList<Object>)  jsonMap.get("old");
        HashMap<String, String> oldData =  new HashMap<>();
        if (oldDataList!=null && oldDataList.size()>0) {
            oldData =  (HashMap<String, String>) oldDataList.get(0);
        }
        List<Object> newDataList  = (ArrayList<Object>)  jsonMap.get("data");
        HashMap<String, String> newData =  new HashMap<>();
        if (newDataList!=null && newDataList.size()>0) {
            newData =  (HashMap<String, String>) newDataList.get(0);
        }
        return new TiCDCMetaData(id, database, tableName, isDdl, type, eventTimestamp, tiSinkTimestamp, dataType, oldData, newData);
    }

    public TiCDCMetaData(int id,
                         String database,
                         String tableName,
                         Boolean isDdl,
                         Type type,
                         Long eventTimestamp,
                         Long tiSinkTimestamp,
                         HashMap<String, String> dataType,
                         HashMap<String, String> oldData,
                         HashMap<String, String> newData) {
        this.id = id;
        this.database = database;
        this.tableName = tableName;
        this.isDdl = isDdl;
        this.type = type;
        this.eventTimestamp = eventTimestamp;
        this.tiSinkTimestamp = tiSinkTimestamp;
        this.dataType = dataType;
        this.oldData = oldData;
        this.newData = newData;
    }

    public int getId() {
        return id;
    }

    public String getDatabase() {
        return database;
    }

    public String getTableName() {
        return tableName;
    }


    public Boolean getDdl() {
        return isDdl;
    }

    public Type getType() {
        return type;
    }

    public Long getEventTimestamp() {
        return eventTimestamp;
    }

    public Long getTiSinkTimestamp() {
        return tiSinkTimestamp;
    }

    public HashMap<String, String> getDataType() {
        return dataType;
    }

    public HashMap<String, String> getOldData() {
        return oldData;
    }

    public HashMap<String, String> getNewData() {
        return newData;
    }

    @Override
    public String toString() {
        return "TiCDCMetaData{" +
                "id=" + id +
                ", database='" + database + '\'' +
                ", tableName='" + tableName + '\'' +
                ", isDdl=" + isDdl +
                ", type=" + type +
                ", eventTimestamp=" + eventTimestamp +
                ", tiSinkTimestamp=" + tiSinkTimestamp +
                ", dataType=" + dataType +
                ", oldData=" + oldData +
                ", newData=" + newData +
                '}';
    }
}
