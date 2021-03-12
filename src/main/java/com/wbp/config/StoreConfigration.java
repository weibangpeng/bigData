package com.wbp.config;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


import java.io.IOException;


@Configuration
public class StoreConfigration {

    @Value("${zookeeperPort:2181}")
    private String zookeeperPort;

    @Value("${hbaseZookeeperQuorum}")
    private String hbaseZookeeperQuorum;
    @Value("${zookeeperNode:/hbase-unsecure}")
    private String zookeeperNode;

    @Bean
    @ConditionalOnMissingBean(Connection.class)
    public Connection connection() throws IOException {
        org.apache.hadoop.conf.Configuration config  = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", hbaseZookeeperQuorum);// zookeeper地址
        config.set("hbase.zookeeper.property.clientPort",zookeeperPort);// zookeeper端口
        config.set("zookeeper.znode.parent", zookeeperNode);
        return ConnectionFactory.createConnection(config);
    }


}
