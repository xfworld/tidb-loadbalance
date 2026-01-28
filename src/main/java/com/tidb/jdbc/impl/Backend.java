package com.tidb.jdbc.impl;

import java.sql.Driver;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Backend {

    private Driver driver;

    private Properties info;

    private String[] backend;

    private Map<String,Weight> weightBackend = new HashMap<>();


    public Driver getDriver() {
        return driver;
    }

    public void setDriver(Driver driver) {
        this.driver = driver;
    }

    public Properties getInfo() {
        return info;
    }

    public void setInfo(Properties info) {
        this.info = info;
    }

    public String[] getBackend() {
        return backend;
    }

    public void setBackend(String[] backend) {
        this.backend = backend;
    }

    public Map<String, Weight> getWeightBackend() {
        return weightBackend;
    }

    public void setWeightBackend(Map<String, Weight> weightBackend) {
        this.weightBackend = weightBackend;
    }
}
