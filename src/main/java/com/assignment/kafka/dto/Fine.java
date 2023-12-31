package com.assignment.kafka.dto;

public class Fine {

    private String id;
    private String ssn;
    private String name;
    private float value;

    public Fine() {

    }

    public Fine(String id, String ssn, String name, float value) {
        this();
        this.id = id;
        this.ssn = ssn;
        this.name = name;
        this.value = value;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSsn() {
        return ssn;
    }

    public void setSsn(String ssn) {
        this.ssn = ssn;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getValue() {
        return value;
    }

    public void setValue(float value) {
        this.value = value;
    }
}
