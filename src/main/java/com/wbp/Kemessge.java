package com.wbp;


import com.alibaba.fastjson.JSONObject;

public class Kemessge {
    private String key;
    private JSONObject message;
    private String Topic;
    private Integer Partition;
    private Long offset;

    public Kemessge(String key, JSONObject message, String topic, Integer partition, Long offset) {
        this.key = key;
        this.message = message;
        Topic = topic;
        Partition = partition;
        this.offset = offset;
    }

    public String getKey() {
        return key;
    }

    public JSONObject getMessage() {
        return message;
    }

    public String getTopic() {
        return Topic;
    }

    public Integer getPartition() {
        return Partition;
    }

    public Long getOffset() {
        return offset;
    }
}
