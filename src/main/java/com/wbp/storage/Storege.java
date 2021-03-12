package com.wbp.storage;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public interface Storege {
    Map<String,String> get(String table, String key);
    boolean exits(String table, String key);
    boolean setIfAnsent(String table, String key, String val, Long timeout, TimeUnit timeUnit);
    void delete (String table, String key);
    void set(String table, String key, String val, Long timeout, TimeUnit timeUnit);
}
