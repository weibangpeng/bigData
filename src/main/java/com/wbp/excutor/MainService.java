package com.wbp.excutor;


import com.wbp.app.JarProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;


import java.util.List;

@Service
public class MainService {

    @Autowired
    private JarProcessor jarProcessor;

    public int handle(List<Kemessge> kemessgeList){
        int cout = 0;
        List<Kemessge> result ;
        result = jarProcessor.flatmap(kemessgeList);
        cout = result.size();
        return cout;
    }
}
