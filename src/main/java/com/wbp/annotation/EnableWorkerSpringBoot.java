package com.wbp.annotation;

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.context.annotation.ComponentScan;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@SpringBootConfiguration
@ComponentScan(basePackages = {"com.wbp"})
public @interface EnableWorkerSpringBoot {
}
