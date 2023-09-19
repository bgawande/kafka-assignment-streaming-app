package com.assignment.kafka.dto.serde;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.assignment.kafka.dto.License;
import com.assignment.kafka.dto.Fine;
import com.assignment.kafka.dto.FineBySsn;
import com.assignment.kafka.dto.User;
import com.assignment.kafka.dto.UserFine;
import com.assignment.kafka.dto.UserLicenseFine;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FactorySerde {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Logger logger = LoggerFactory.getLogger(FactorySerde.class.getName());

    static {
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    public static Serde<User> getUserSerde() {
        return new Serde<>() {
            @Override
            public Serializer<User> serializer() {
                return new UserSerializer();
            }

            @Override
            public Deserializer<User> deserializer() {
                return new UserDeserializer();
            }
        };
    }

    public static Serde<License> getLicenseSerde() {
        return new Serde<>() {
            @Override
            public Serializer<License> serializer() {
                return new LicenseSerde();
            }

            @Override
            public Deserializer<License> deserializer() {
                return new LicenseDeserializer();
            }
        };
    }

    public static Serde<UserFine> getUserLicenseSerde() {
        return new Serde<>() {
            @Override
            public Serializer<UserFine> serializer() {
                return new UserLicenseSerializer();
            }

            @Override
            public Deserializer<UserFine> deserializer() {
                return new UserLicenseDeserializer();
            }
        };
    }

    public static Serde<Fine> getFineSerde() {
        return new Serde<>() {
            @Override
            public Serializer<Fine> serializer() {
                return new FineSerializer();
            }

            @Override
            public Deserializer<Fine> deserializer() {
                return new FineDeserializer();
            }
        };
    }

    public static Serde<FineBySsn> getFineBySsnSerde() {
        return new Serde<>() {
            @Override
            public Serializer<FineBySsn> serializer() {
                return new FineBySsnSerializer();
            }

            @Override
            public Deserializer<FineBySsn> deserializer() {
                return new FineBySsnDeserializer();
            }
        };
    }

    public static Serde<UserLicenseFine> getUserLicenseFineSerde() {
        return new Serde<>() {
            @Override
            public Serializer<UserLicenseFine> serializer() {
                return new UserLicenseFineSerializer();
            }

            @Override
            public Deserializer<UserLicenseFine> deserializer() {
                return new UserLicenseFineDeserializer();
            }
        };
    }


    public static final class UserSerializer implements Serializer<User> {


        @Override
        public byte[] serialize(String topic, User user) {
            byte[] res = null;
            try {
                res = objectMapper.writeValueAsBytes(user);
            } catch (JsonProcessingException e) {
                logger.error("ERROR", e);
            }
            return res;
        }
    }


    public static final class UserDeserializer implements Deserializer<User> {

        @Override
        public User deserialize(String topic, byte[] bytes) {
            User user = null;
            try {
                user = objectMapper.readValue(bytes, User.class);
            } catch (IOException e) {
                logger.error("ERROR", e);
            }
            return user;
        }
    }


    static final class LicenseSerde implements Serializer<License> {


        @Override
        public byte[] serialize(String topic, License user) {
            byte[] res = null;
            try {
                res = objectMapper.writeValueAsBytes(user);
            } catch (JsonProcessingException e) {
                logger.error("ERROR", e);
            }
            return res;
        }
    }


    public static final class LicenseDeserializer implements Deserializer<License> {

        @Override
        public License deserialize(String topic, byte[] bytes) {
            License license = null;
            try {
                license = objectMapper.readValue(bytes, License.class);
            } catch (IOException e) {
                logger.error("ERROR", e);
            }
            return license;
        }
    }

    public static final class UserLicenseSerializer implements Serializer<UserFine> {


        @Override
        public byte[] serialize(String topic, UserFine userLicense) {
            byte[] res = null;
            try {
                res = objectMapper.writeValueAsBytes(userLicense);
            } catch (JsonProcessingException e) {
                logger.error("ERROR", e);
            }
            return res;
        }
    }

    public static final class UserLicenseDeserializer implements Deserializer<UserFine> {


        @Override
        public UserFine deserialize(String topic, byte[] bytes) {
            UserFine license = null;
            try {
                license = objectMapper.readValue(bytes, UserFine.class);
            } catch (IOException e) {
                logger.error("ERROR", e);
            }
            return license;
        }
    }

    public static final class FineSerializer implements Serializer<Fine> {


        @Override
        public byte[] serialize(String topic, Fine fine) {
            byte[] res = null;
            try {
                res = objectMapper.writeValueAsBytes(fine);
            } catch (JsonProcessingException e) {
                logger.error("ERROR", e);
            }
            return res;
        }
    }

    public static final class FineDeserializer implements Deserializer<Fine> {


        @Override
        public Fine deserialize(String topic, byte[] bytes) {
            Fine fine = null;
            try {
                fine = objectMapper.readValue(bytes, Fine.class);
            } catch (IOException e) {
                logger.error("ERROR", e);
            }
            return fine;
        }
    }

    public static final class FineBySsnSerializer implements Serializer<FineBySsn> {


        @Override
        public byte[] serialize(String topic, FineBySsn fine) {
            byte[] res = null;
            try {
                res = objectMapper.writeValueAsBytes(fine);
            } catch (JsonProcessingException e) {
                logger.error("ERROR", e);
            }
            return res;
        }
    }

    public static final class FineBySsnDeserializer implements Deserializer<FineBySsn> {


        @Override
        public FineBySsn deserialize(String topic, byte[] bytes) {
            FineBySsn fine = null;
            try {
                fine = objectMapper.readValue(bytes, FineBySsn.class);
            } catch (IOException e) {
                logger.error("ERROR", e);
            }
            return fine;
        }
    }

    public static final class UserLicenseFineSerializer implements Serializer<UserLicenseFine> {


        @Override
        public byte[] serialize(String topic, UserLicenseFine fine) {
            byte[] res = null;
            try {
                res = objectMapper.writeValueAsBytes(fine);
            } catch (JsonProcessingException e) {
                logger.error("ERROR", e);
            }
            return res;
        }
    }

    public static final class UserLicenseFineDeserializer implements Deserializer<UserLicenseFine> {


        @Override
        public UserLicenseFine deserialize(String topic, byte[] bytes) {
            UserLicenseFine fine = null;
            try {
                fine = objectMapper.readValue(bytes, UserLicenseFine.class);
            } catch (IOException e) {
                logger.error("ERROR", e);
            }
            return fine;
        }
    }
}
