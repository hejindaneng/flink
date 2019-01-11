package com.hejin.syn;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Package: com.hejin.syn
 * Description： TODO
 * Author: Hejin
 * Date: Created in 2019/1/7 17:59
 * Company: 公司
 * Copyright: Copyright (c) 2017
 * Version: 0.0.1
 * Modified By:
 */
public class CanalClient {
    public static void main(String[] args) {
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("node01", 1111), "example", "root", "root");
        int batchSize = 100;
        int emptyCount = 1;
        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();
            int totalCount = 120;
            while (emptyCount < totalCount) {
                Message message = connector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (batchId == -1 || size == 0) {
//                    System.out.println("=========当前mysql没有触发任何操作=====");
                } else {
                    Analysis(message.getEntries(), emptyCount);
                    emptyCount++;
                }
            }
        } catch (CanalClientException e) {
            e.printStackTrace();
        } finally {
            connector.disconnect();
        }
    }

    public static void Analysis(List<CanalEntry.Entry> entries, int emptyCount) {
        for (CanalEntry.Entry entry : entries) {
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }
            CanalEntry.RowChange rowChange = null;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }
            //获取关键字段值
            CanalEntry.EventType eventType = rowChange.getEventType(); //当前mysql的操作类型
            String logfileName = entry.getHeader().getLogfileName(); //获取当前binlog日志的名称
            long logfileOffset = entry.getHeader().getLogfileOffset();//获取解析到binlog的位置
            String dbName = entry.getHeader().getSchemaName();//获取当前mysql的库名
            String tableName = entry.getHeader().getTableName();//获取当前mysql的表名
            System.out.println("logfileName:" + logfileName + "logfileOffset" + logfileOffset + "dbName:"+ dbName + "tableName" + tableName);
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                if (eventType == CanalEntry.EventType.DELETE) {
                    dataDetails(rowData.getBeforeColumnsList(), eventType, logfileName, logfileOffset, dbName, tableName, emptyCount);
                } else if (eventType == CanalEntry.EventType.INSERT) {
                    dataDetails(rowData.getAfterColumnsList(), eventType, logfileName, logfileOffset, dbName, tableName, emptyCount);
                } else {
                    dataDetails(rowData.getAfterColumnsList(), eventType, logfileName, logfileOffset, dbName, tableName, emptyCount);
                }
            }
        }
    }

    private static void dataDetails(List<CanalEntry.Column> columnsList, CanalEntry.EventType eventType, String logfileName, long logfileOffset, String dbName, String tableName, int emptyCount) {
        ArrayList<Object> list = new ArrayList<>();
        for (CanalEntry.Column column : columnsList) {
            List<Object> cl = new ArrayList<>();
            String fileName = column.getName();
            String fileValue = column.getValue();
            boolean isUpdated = column.getUpdated();
            cl.add(fileName);
            cl.add(fileValue);
            cl.add(isUpdated);
            list.add(cl);
        }
        String sendKey = UUID.randomUUID().toString();
        String data = logfileName + "##" + logfileOffset + "##" + dbName + "##" + tableName + "##" + eventType + "##" + emptyCount + "##" + list;
        KafkaProducer.sendMessage("myCanal",sendKey,data);
    }
}
