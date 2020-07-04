package com.wbp;

import org.apache.spark.api.java.function.VoidFunction;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StreamExcutor  implements VoidFunction<Iterator<Kemessge>> {

    private static ConfigurableApplicationContext applicationContext;

    @Override
    public void call(Iterator<Kemessge> iterator) throws Exception {

        List<Kemessge> kemessges = new ArrayList<>();
        while (iterator.hasNext()){
            Kemessge kemessge = iterator.next();
            kemessges.add(kemessge);
        }
        SpringApplication application = new SpringApplication(EnableWorkerSpringBoot.class);
        application.setWebApplicationType(WebApplicationType.NONE);
        applicationContext = application.run();
        MainService mainService = applicationContext.getBean(MainService.class);
        mainService.handle(kemessges);
    }
}
