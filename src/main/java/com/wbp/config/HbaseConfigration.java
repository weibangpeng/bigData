package com.wbp.config;

import com.wbp.storage.HbaseStorege;
import com.wbp.storage.Storege;
import org.apache.hadoop.hbase.client.Connection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class HbaseConfigration {

    @Autowired
    Connection connection;


    @Bean
    @ConditionalOnMissingBean(Storege.class)
    public Storege storege(){
        return new HbaseStorege(connection);
    }
}
