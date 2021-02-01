package cn.jiayeli.utils;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import org.apache.avro.data.Json;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BinlogUtil {

    private static Map<String, Object> columnDataMap = new HashMap();

    public String binLogToJson(Message message) {


        for (CanalEntry.Entry entry : message.getEntries()) {
            //是否在事务内，在则跳过此次处理
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            //变更的数据结构
            CanalEntry.RowChange rowChange = null;
            try {
                //解析binlog
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            //数据变更类型
            CanalEntry.EventType eventType = rowChange.getEventType();
            System.out.println(String.format("================> binlog[fileName-> %s: offset-%s] ," +
                            " name[schemeName-> %s,tabName-> %s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));

            columnDataMap.put("fileName", entry.getHeader().getLogfileName());
            columnDataMap.put("offset",  entry.getHeader().getLogfileOffset());
            columnDataMap.put("schemeName", entry.getHeader().getSchemaName());
            columnDataMap.put("tabName", entry.getHeader().getTableName());
            columnDataMap.put("eventType", eventType);

            //变更的数据，一到多行
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    collectColumn(rowData.getBeforeColumnsList());
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    collectColumn(rowData.getAfterColumnsList());
//                    printColumn(rowData.getAfterColumnsList());
                } else {
//                    System.out.println("-------> 修改前的数据");
//                    collectColumn(rowData.getBeforeColumnsList());
//                    printColumn(rowData.getBeforeColumnsList());
                    System.out.println("-------> 修改后的数据");
                    collectColumn(rowData.getAfterColumnsList());
//                    printColumn(rowData.getAfterColumnsList());
                }
            }
        }

        return Json.toString(columnDataMap);
    }

    private static void collectColumn(List<CanalEntry.Column> datas) {

        List<Map<String, Object>> tabValues = new ArrayList<Map<String, Object>>();
        for (CanalEntry.Column column : datas) {
            System.out.println(column.getName() + " : " + column.getValue() + "    update=" + column.getUpdated());
            HashMap<String, Object> valueMap = new HashMap<>();
            valueMap.put("value", column.getValue());
            valueMap.put("update", column.getUpdated());
            columnDataMap.put("values", valueMap);
        }
    }

}
