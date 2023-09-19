package com.assignment.kafka.dto;

public class UserLicenseFine {

    private User user;
    private License license;
    private FineBySsn fineBySsn;


    public UserLicenseFine() {

    }

    public UserLicenseFine(User user, License license, FineBySsn fineBySsn) {
        this.user = user;
        this.license = license;
        this.fineBySsn = fineBySsn;
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

    public FineBySsn getFineBySsn() {
        return fineBySsn;
    }

    public void setFineBySsn(FineBySsn fineBySsn) {
        this.fineBySsn = fineBySsn;
    }
}
