package com.wbp.app;

import com.alibaba.fastjson.JSONObject;
import com.wbp.SparkKafkaWordCount;
import com.wbp.excutor.Kemessge;
import com.wbp.storage.Storege;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class JarProcessorImpl implements JarProcessor{
    @Value("${tableName:wbptest}")
    String tableName;

    @Autowired(required=true)
    Storege storege;
    Logger logger = LoggerFactory.getLogger(SparkKafkaWordCount.class);
    @Override
    public List<Kemessge> flatmap(List<Kemessge> kemessgeList) {
        for (int i = 0;i<kemessgeList.size();i++){
            logger.info(kemessgeList.get(i).toString());
            Kemessge kemessge = kemessgeList.get(i);
            JSONObject jsonObject= kemessge.getMessage();
            String id = jsonObject.getString("id");
            String val = jsonObject.getString("val");
            storege.setIfAnsent(tableName,id,val,10L, TimeUnit.DAYS);
        }
        return kemessgeList;
    }
}
