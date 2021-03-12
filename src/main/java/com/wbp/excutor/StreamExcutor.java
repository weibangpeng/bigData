package com.wbp.excutor;

import org.apache.spark.api.java.function.VoidFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class StreamExcutor implements VoidFunction<Iterator<Kemessge>> {

    private StreamContext context;

    public StreamExcutor(StreamContext context) {
        this.context = context;
    }

    @Override
    public void call(Iterator<Kemessge> iterator) throws Exception {

        List<Kemessge> kemessges = new ArrayList<>();
        while (iterator.hasNext()) {
            Kemessge kemessge = iterator.next();
            kemessges.add(kemessge);
        }
        if (!kemessges.isEmpty()) {
            MainService mainService = context.getBean(MainService.class);
            mainService.handle(kemessges);
        }

    }
}
