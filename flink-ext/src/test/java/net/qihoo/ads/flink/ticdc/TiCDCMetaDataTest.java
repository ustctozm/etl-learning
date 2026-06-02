package net.qihoo.ads.flink.ticdc;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;

public class TiCDCMetaDataTest {

    String ticdcInfo = "{\"id\":97479394,\"database\":\"mediav_base\",\"table\":\"banner_level\",\"pkNames\":[\"id\"],\"isDdl\":false," +
            "\"type\":\"UPDATE\",\"es\":1694588272844,\"ts\":1694588273360,\"sql\":\"\"," +
            "\"sqlType\":{\"id\":-5,\"banner_id\":-5,\"dsp_id\":4,\"level\":-6,\"last_modified\":93}," +
            "\"mysqlType\":{\"id\":\"bigint\",\"banner_id\":\"bigint\",\"dsp_id\":\"int\",\"level\":\"tinyint\"," +
            "\"last_modified\":\"timestamp\"},\"old\":[{\"id\":\"97479394\",\"banner_id\":\"106809632\",\"dsp_id\":" +
            "\"100\",\"level\":\"6\",\"last_modified\":\"2023-09-13 14:50:16\"}],\"data\":[{\"id\":\"97479394\"," +
            "\"banner_id\":\"106809632\",\"dsp_id\":\"100\",\"level\":\"4\",\"last_modified\":\"2023-09-13 14:57:52\"}]}";

    byte[] ticdcBytes = {123, 34, 105, 100, 34, 58, 48, 44, 34, 100, 97, 116, 97, 98, 97, 115, 101, 34, 58, 34, 109, 101,
            100, 105, 97, 118, 95, 98, 97, 115, 101, 34, 44, 34, 116, 97, 98, 108, 101, 34, 58, 34, 98, 97, 110, 110, 101,
            114, 95, 108, 101, 118, 101, 108, 34, 44, 34, 112, 107, 78, 97, 109, 101, 115, 34, 58, 91, 34, 105, 100, 34, 93,
            44, 34, 105, 115, 68, 100, 108, 34, 58, 102, 97, 108, 115, 101, 44, 34, 116, 121, 112, 101, 34, 58, 34, 85, 80,
            68, 65, 84, 69, 34, 44, 34, 101, 115, 34, 58, 49, 54, 57, 53, 55, 49, 56, 55, 52, 52, 57, 52, 52, 44, 34, 116,
            115, 34, 58, 49, 54, 57, 53, 55, 49, 56, 55, 52, 53, 55, 52, 49, 44, 34, 115, 113, 108, 34, 58, 34, 34, 44, 34,
            115, 113, 108, 84, 121, 112, 101, 34, 58, 123, 34, 105, 100, 34, 58, 45, 53, 44, 34, 98, 97, 110, 110, 101, 114,
            95, 105, 100, 34, 58, 45, 53, 44, 34, 100, 115, 112, 95, 105, 100, 34, 58, 52, 44, 34, 108, 101, 118, 101, 108,
            34, 58, 45, 54, 44, 34, 108, 97, 115, 116, 95, 109, 111, 100, 105, 102, 105, 101, 100, 34, 58, 57, 51, 125, 44,
            34, 109, 121, 115, 113, 108, 84, 121, 112, 101, 34, 58, 123, 34, 108, 101, 118, 101, 108, 34, 58, 34, 116, 105,
            110, 121, 105, 110, 116, 34, 44, 34, 108, 97, 115, 116, 95, 109, 111, 100, 105, 102, 105, 101, 100, 34, 58, 34,
            116, 105, 109, 101, 115, 116, 97, 109, 112, 34, 44, 34, 105, 100, 34, 58, 34, 98, 105, 103, 105, 110, 116, 34, 44,
            34, 98, 97, 110, 110, 101, 114, 95, 105, 100, 34, 58, 34, 98, 105, 103, 105, 110, 116, 34, 44, 34, 100, 115, 112,
            95, 105, 100, 34, 58, 34, 105, 110, 116, 34, 125, 44, 34, 111, 108, 100, 34, 58, 91, 123, 34, 105, 100, 34, 58,
            34, 57, 55, 53, 49, 51, 51, 56, 56, 34, 44, 34, 98, 97, 110, 110, 101, 114, 95, 105, 100, 34, 58, 34, 49, 48,
            54, 56, 52, 51, 54, 50, 54, 34, 44, 34, 100, 115, 112, 95, 105, 100, 34, 58, 34, 49, 48, 48, 34, 44, 34, 108, 101,
            118, 101, 108, 34, 58, 34, 54, 34, 44, 34, 108, 97, 115, 116, 95, 109, 111, 100, 105, 102, 105, 101, 100, 34, 58,
            34, 50, 48, 50, 51, 45, 48, 57, 45, 50, 54, 32, 49, 54, 58, 48, 57, 58, 53, 49, 34, 125, 93, 44, 34, 100, 97, 116,
            97, 34, 58, 91, 123, 34, 105, 100, 34, 58, 34, 57, 55, 53, 49, 51, 51, 56, 56, 34, 44, 34, 98, 97, 110, 110, 101,
            114, 95, 105, 100, 34, 58, 34, 49, 48, 54, 56, 52, 51, 54, 50, 54, 34, 44, 34, 100, 115, 112, 95, 105, 100, 34, 58,
            34, 49, 48, 48, 34, 44, 34, 108, 101, 118, 101, 108, 34, 58, 34, 52, 34, 44, 34, 108, 97, 115, 116, 95, 109, 111,
            100, 105, 102, 105, 101, 100, 34, 58, 34, 50, 48, 50, 51, 45, 48, 57, 45, 50, 54, 32, 49, 54, 58, 53, 57, 58, 48, 52, 34, 125, 93, 125, 13, 10}
;


    @Test
    public void testConvert(){
        TiCDCMetaData metaData = TiCDCMetaData.generateTiCDCMetaData(ticdcInfo, Lists.newArrayList("banner_level"));
        if (metaData != null) {
            HashMap<String,String> oldData = metaData.getOldData();
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                // 将HashMap转换为JSON字符串
                String json = objectMapper.writeValueAsString(oldData);
                Assert.assertEquals(json, "{\"id\":\"97479394\",\"banner_id\":\"106809632\",\"dsp_id\":\"100\",\"level\":\"6\",\"last_modified\":\"2023-09-13 14:50:16\"}");
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            Assert.assertEquals(metaData.getTableName(), "banner_level");
            Assert.assertEquals(metaData.getEventTimestamp().longValue(), 1694588272844L);
            Assert.assertEquals(metaData.getTiSinkTimestamp().longValue(), 1694588273360L);
            Assert.assertFalse(metaData.getDdl());
            Assert.assertEquals(metaData.getType(), TiCDCMetaData.Type.UPDATE);
            Assert.assertEquals(metaData.getId(), 97479394);
        }

        TiCDCMetaData byteMetaData = TiCDCMetaData.generateTiCDCMetaData(ticdcBytes);
        Assert.assertEquals(byteMetaData.toString(), "TiCDCMetaData{id=0, database='mediav_base', " +
                "tableName='banner_level', isDdl=false, type=UPDATE, eventTimestamp=1695718744944, " +
                "tiSinkTimestamp=1695718745741, " +
                "dataType={level=tinyint, last_modified=timestamp, id=bigint, banner_id=bigint, dsp_id=int}, " +
                "oldData={id=97513388, banner_id=106843626, dsp_id=100, level=6, last_modified=2023-09-26 16:09:51}," +
                " newData={id=97513388, banner_id=106843626, dsp_id=100, level=4, last_modified=2023-09-26 16:59:04}}");
    }


}
