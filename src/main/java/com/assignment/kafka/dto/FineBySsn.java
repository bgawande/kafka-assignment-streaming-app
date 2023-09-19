package com.assignment.kafka.dto;

import java.util.ArrayList;
import java.util.List;

public class FineBySsn {

    private String ssn;

    private List<Fine> fineList;

    public FineBySsn() {
        this.fineList = new ArrayList<>();
    }

    public String getSsn() {
        return ssn;
    }

    public void setSsn(String ssn) {
        this.ssn = ssn;
    }

    public List<Fine> getFineList() {
        return fineList;
    }

    public void setFineList(List<Fine> feeList) {
        this.fineList = fineList;
    }
}
