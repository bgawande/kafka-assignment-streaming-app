package com.assignment.kafka.dto;

import java.util.List;

public class License {

    private String id;
    private List<String> typeList;
    private String licenseNumber;

    public License() {

    }

    public License(String id, List<String> typeList, String licenseNumber) {
        this();
        this.id = id;
        this.typeList = typeList;
        this.licenseNumber = licenseNumber;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public List<String> getTypeList() {
        return typeList;
    }

    public void setTypeList(List<String> typeList) {
        this.typeList = typeList;
    }

    public String getLicenseNumber() {
        return licenseNumber;
    }

    public void setLicenseNumber(String licenseNumber) {
        this.licenseNumber = licenseNumber;
    }
}
