package com.assignment.kafka.dto;

public class UserFine {
    private User user;
    private License license;

    public UserFine(User user, License license) {
        this();
        this.user = user;
        this.license = license;
    }


    public UserFine() {

    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public License getLicense() {
        return license;
    }

    public void setLicense(License license) {
        this.license = license;
    }
}
