package com.assignment.kafka.dto;

import java.time.LocalDate;

public class User {


    private String ssn;
    private String name;
    private String surname;
    private String phoneNumber;

    public User() {
    }

    public User(String ssn, String name, String surname, String phoneNumber) {
        this();
        this.ssn = ssn;
        this.name = name;
        this.surname = surname;
        this.phoneNumber = phoneNumber;
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

    public String getSurname() {
        return surname;
    }

    public void setSurname(String surname) {
        this.surname = surname;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    @Override
    public String toString() {
        return "Person{" +
                "scn='" + ssn + '\'' +
                ", name='" + name + '\'' +
                ", surname='" + surname + '\'' +
                ", phoneNumber='" + phoneNumber + '\'' +
                '}';
    }

    public enum SEX {M, F}

}
