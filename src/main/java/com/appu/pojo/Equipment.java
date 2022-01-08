package com.appu.pojo;

public class Equipment {
    String key;
    String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "Equipment{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
