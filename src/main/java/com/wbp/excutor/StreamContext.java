package com.wbp.excutor;

import com.wbp.annotation.EnableWorkerSpringBoot;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

 public class StreamContext implements Serializable , Closeable {
    private static   ConfigurableApplicationContext applicationContext;

    public StreamContext(ConfigurableApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public <T> T getBean(Class<T> requeType) {
        if (null == applicationContext) {
            SpringApplication application = new SpringApplication(EnableWorkerSpringBoot.class);
            application.setWebEnvironment(false);
            application.addInitializers((ApplicationContextInitializer<ConfigurableApplicationContext>) applicationContext -> {
                ConfigurableListableBeanFactory beanFactory = applicationContext.getBeanFactory();
                beanFactory.registerSingleton("context", this);
            });
            applicationContext = application.run();
        }
        return applicationContext.getBean(requeType);
    }

     @Override
     public void close() throws IOException {
         if(applicationContext != null){
             applicationContext.close();
         }
     }
 }
