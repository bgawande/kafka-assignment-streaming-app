package com.assignment.kafka.streaming;

import com.assignment.kafka.dto.serde.FactorySerde;
import com.assignment.kafka.dto.License;
import com.assignment.kafka.dto.Fine;
import com.assignment.kafka.dto.FineBySsn;
import com.assignment.kafka.dto.User;
import com.assignment.kafka.dto.UserFine;
import com.assignment.kafka.dto.UserLicenseFine;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.ForeachAction;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.inject.Produces;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

@ApplicationScoped
public class AssignmentStreamingApp {

    private final static Logger logger = LoggerFactory.getLogger(AssignmentStreamingApp.class.getName());

    private final static String MERGE = "merge";
    private final static String USER_WITH_FINES = "user-with-fines";
    private final static String LICENSE = "license";
    private final static String FINE = "fine";
    private final static String USER = "user";

    @PostConstruct
    public void postInit() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FactorySerde.UserSerializer.class);
        AdminClient adminClient = KafkaAdminClient.create(props);
        adminClient.createTopics(List.of(new NewTopic(USER, 3, (short) 1)));
        adminClient.createTopics(List.of(new NewTopic(FINE, 5, (short) 1)));
        adminClient.createTopics(List.of(new NewTopic(LICENSE, 3, (short) 1)));
        adminClient.createTopics(List.of(new NewTopic(USER_WITH_FINES, 4, (short) 1)));
        CreateTopicsResult resultAllTogether = adminClient.createTopics(List.of(new NewTopic(MERGE, 4, (short) 1)));

        resultAllTogether.all().whenComplete((unused, throwable) -> {
            if (throwable == null) {
                List<String> carType = Collections.singletonList("CAR");
                List<String> motorbikeAndCarType = List.of("CAR", "MOTORBIKE");
                KafkaProducer kafkaUserProducer = new KafkaProducer<>(props, Serdes.String().serializer(), FactorySerde.getUserSerde().serializer());
                KafkaProducer kafkaFineProducer = new KafkaProducer<>(props, Serdes.String().serializer(), FactorySerde.getFineSerde().serializer());
                KafkaProducer kafkaLicenseProducer = new KafkaProducer<>(props, Serdes.String().serializer(), FactorySerde.getLicenseSerde().serializer());

                ProducerRecord<String, User> userProducerRecord1 = new ProducerRecord<>(USER, "ssn001", new User("ssn001", "Amit", "Gupta", "7863245768"));
                ProducerRecord<String, User> userProducerRecord2 = new ProducerRecord<>(USER, "ssn002", new User("ssn002", "Balram", "Jadhav", "7653245778"));
                ProducerRecord<String, User> userProducerRecord3 = new ProducerRecord<>(USER, "ssn003", new User("ssn003", "Chetan", "Patil", "7343245788"));
                ProducerRecord<String, User> userProducerRecord4 = new ProducerRecord<>(USER, "ssn004", new User("ssn004", "Dilip", "Joshi", "7213245798"));

                ProducerRecord<String, Fine> fineProducerRecord1 = new ProducerRecord<>(FINE, "fine001", new Fine("fine001", "ssn001", "ABC", 500));
                ProducerRecord<String, Fine> fineProducerRecord2 = new ProducerRecord<>(FINE, "fine002", new Fine("fine002", "ssn002", "PQR", 600));
                ProducerRecord<String, Fine> fineProducerRecord3 = new ProducerRecord<>(FINE, "fine003", new Fine("fine003", "ssn003", "MNO", 400));
                ProducerRecord<String, Fine> fineProducerRecord4 = new ProducerRecord<>(FINE, "fine004", new Fine("fine004", "ssn004", "XYZ", 300));
                ProducerRecord<String, Fine> fineProducerRecord5 = new ProducerRecord<>(FINE, "fine005", new Fine("fine005", "ssn004", "ABC", 600));
                ProducerRecord<String, Fine> fineProducerRecord6 = new ProducerRecord<>(FINE, "fine006", new Fine("fine006", "ssn001", "PQR", 500));

                ProducerRecord<String, License> driverProducerRecord1 = new ProducerRecord<>(LICENSE, "ssn001", new License("ssn001", carType, "1213245768"));
                ProducerRecord<String, License> driverProducerRecord2 = new ProducerRecord<>(LICENSE, "ssn001", new License("ssn001", motorbikeAndCarType, "1213245768"));
                ProducerRecord<String, License> driverProducerRecord3 = new ProducerRecord<>(LICENSE, "ssn002", new License("ssn002", carType, "2213245768"));
                ProducerRecord<String, License> driverProducerRecord4 = new ProducerRecord<>(LICENSE, "ssn003", new License("ssn003", carType, "3213245768"));
                ProducerRecord<String, License> driverProducerRecord5 = new ProducerRecord<>(LICENSE, "ssn004", new License("ssn004", carType, "4213245768"));


                kafkaUserProducer.send(userProducerRecord1);
                kafkaUserProducer.send(userProducerRecord2);
                kafkaUserProducer.send(userProducerRecord3);
                kafkaUserProducer.send(userProducerRecord4);

                kafkaFineProducer.send(fineProducerRecord1);
                kafkaFineProducer.send(fineProducerRecord2);
                kafkaFineProducer.send(fineProducerRecord3);
                kafkaFineProducer.send(fineProducerRecord4);
                kafkaFineProducer.send(fineProducerRecord5);
                kafkaFineProducer.send(fineProducerRecord6);

                kafkaLicenseProducer.send(driverProducerRecord1);
                kafkaLicenseProducer.send(driverProducerRecord2);
                kafkaLicenseProducer.send(driverProducerRecord3);
                kafkaLicenseProducer.send(driverProducerRecord4);
                kafkaLicenseProducer.send(driverProducerRecord5);
            }
        });

    }


    final StreamsBuilder builder = new StreamsBuilder();

    final ForeachAction<String, User> loggingForEach = (key, user) -> {
        if (user != null)
            logger.info("Key: {}, Value: {}", key, user);
    };

    //@Produces
    public Topology appJoinUserAndLicense() {

        final KStream<String, User> userStream = builder.stream(USER,
                Consumed.with(Serdes.String(), FactorySerde.getUserSerde()));
        final KStream<String, License> licenseStream = builder.stream(LICENSE,
                Consumed.with(Serdes.String(), FactorySerde.getLicenseSerde()));

        KStream<String, User> peekStream = userStream.peek(loggingForEach);

        KStream<String, UserFine> joined = peekStream.join(licenseStream,
                (user, license) -> new UserFine(user, license),
                JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES)), StreamJoined.with(
                        Serdes.String(),  //key
                        FactorySerde.getUserSerde(),    //left value
                        FactorySerde.getLicenseSerde()   //right value
                ));

        joined.to(MERGE, Produced.with(Serdes.String(), FactorySerde.getUserLicenseSerde()));

        Topology build = builder.build();
        logger.info(build.describe().toString());
        return build;
    }

    @Produces
    public Topology appJoinUserAndLicenseAndFine() {

        final KStream<String, User> userStream = builder.stream(USER,
                Consumed.with(Serdes.String(), FactorySerde.getUserSerde()));
        final KStream<String, License> licenseStream = builder.stream(LICENSE,
                Consumed.with(Serdes.String(), FactorySerde.getLicenseSerde()));


        KStream<String, User> peekStream = userStream.peek(loggingForEach);

        KStream<String, UserFine> joinedUserLicense = peekStream.join(licenseStream,
                (user, license) -> new UserFine(user, license),
                JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES)), StreamJoined.with(
                        Serdes.String(), /* key */
                        FactorySerde.getUserSerde(),   /* left value */
                        FactorySerde.getLicenseSerde()  /* right value */
                ));

        final KStream<String, Fine> fineStream = builder.stream(FINE,
                Consumed.with(Serdes.String(), FactorySerde.getFineSerde()));

        KeyValueBytesStoreSupplier fineBySsnTableSupplier = Stores.persistentKeyValueStore("fineBySsnTableSupplier");

        final Materialized<String, FineBySsn, KeyValueStore<Bytes, byte[]>> materialized =
                Materialized.<String, FineBySsn>as(fineBySsnTableSupplier)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(FactorySerde.getFineBySsnSerde());


        joinedUserLicense.to(MERGE, Produced.with(Serdes.String(), FactorySerde.getUserLicenseSerde()));

        KStream<String, UserLicenseFine> joinedUserLicenseFine = fineStream
                .selectKey((key, value) -> value.getSsn())
                .groupByKey(Grouped.with(Serdes.String(), FactorySerde.getFineSerde()))
                .aggregate(() -> new FineBySsn(), (key, value, aggregate) -> {
                    aggregate.setSsn(key);
                    aggregate.getFineList().add(value);
                    return aggregate;
                }, materialized)
                .toStream()
                .join(joinedUserLicense,
                        (fineBySsn, userDl) -> new UserLicenseFine(userDl.getUser(), userDl.getLicense(), fineBySsn),
                        JoinWindows.of(Duration.of(5, ChronoUnit.MINUTES)), StreamJoined.with(
                                Serdes.String(), /* key */
                                FactorySerde.getFineBySsnSerde(),   /* left value */
                                FactorySerde.getUserLicenseSerde()  /* right value */
                        ));
        joinedUserLicenseFine.to(USER_WITH_FINES, Produced.with(Serdes.String(), FactorySerde.getUserLicenseFineSerde()));

        Topology build = builder.build();
        logger.info(build.describe().toString());
        return build;
    }
}
