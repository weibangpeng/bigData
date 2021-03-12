package com.wbp.storage;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class HbaseStorege implements Storege {

    //哈希列族
    private byte[] shrFamily = "i".getBytes();
    //字符列族
    private byte[] strFamily = "i".getBytes();
    //集合列族
    private byte[] setFamily = "i".getBytes();
    //列表列族
    private byte[] listFamily = "i".getBytes();
    private Connection connection;

    public HbaseStorege(Connection connection) {
        this.connection = connection;
    }

    @Override
    public Map<String, String> get(String table, String key) {

        Result result = getResrlt(table,key,shrFamily);
        Map<String, String> returnMap = new HashMap<>();
        List<Cell> cellList = result.listCells();
        for (Cell cell:
        cellList) {
            String qualifier = Bytes.toString(cell.getQualifierArray(),cell.getFamilyOffset(),cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
            returnMap.put(qualifier,value);
        }
        return returnMap;
    }


    @Override
    public boolean exits(String table, String key) {
        Get get = new Get(Bytes.toBytes(key));
        try(Table t = connection.getTable(TableName.valueOf(table))){
            return t.exists(get);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean setIfAnsent(String table, String key, String val, Long timeout, TimeUnit timeUnit) {
        byte[] rowKey = Bytes.toBytes(key);
        Put put = new Put(rowKey);
        put.addColumn(strFamily,strFamily,Bytes.toBytes(val));
        if(timeout>0){
            long ttl = TimeUnit.MILLISECONDS.convert(timeout,timeUnit);
            put.setTTL(ttl);
        }
        try(Table t = connection.getTable(TableName.valueOf(table))){
            return t.checkAndMutate(rowKey,strFamily).qualifier(strFamily).ifNotExists().thenPut(put);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(String table, String key) {
        Delete delete = new Delete(Bytes.toBytes(key));
        try(Table t = connection.getTable(TableName.valueOf(table))){
            t.delete(delete);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(String table, String key, String val, Long timeout, TimeUnit timeUnit) {
        Put put = new Put(Bytes.toBytes(key));
        put.addColumn(strFamily,strFamily,Bytes.toBytes(val));
        if(timeout>0){
            long ttl = TimeUnit.MILLISECONDS.convert(timeout,timeUnit);
            put.setTTL(ttl);
        }
        try(Table t = connection.getTable(TableName.valueOf(table))){
            t.put(put);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }

    private Result getResrlt(String table, String key, byte[] family){
         Get get =new Get(Bytes.toBytes(key));
        if(family!=null){
            get.addFamily(family);
        }
        TableName tableName = TableName.valueOf(table);
        try(Table t = connection.getTable(tableName)) {
            return t.get(get);
        }catch (IOException e){
            throw new RuntimeException(e);
        }
    }
}
