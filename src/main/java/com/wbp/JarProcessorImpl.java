package com.wbp;

import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class JarProcessorImpl implements JarProcessor{
    @Override
    public List<Kemessge> flatmap(List<Kemessge> kemessgeList) {
        for (int i = 0;i<kemessgeList.size();i++){
            System.out.println(kemessgeList.get(i));
        }
        return kemessgeList;
    }
}
