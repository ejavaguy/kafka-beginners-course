package learn.kafka.tutorial1;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerDemoWithThreads {
    String server="127.0.0.1:9092";
    String groupdId="my-sixth-application";
    String topic="first_topic";

    public static void main(String...args) {
        new ConsumerDemoWithThreads().run();
    }

    public void run() {
        CountDownLatch latch = new CountDownLatch(1);
        log.info("creating consumer thread");
        ConsumerThread myConsumer = new ConsumerThread(server, groupdId,latch);
        new Thread(myConsumer).start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            myConsumer.shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("application has exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("application interrupted",e);
        } finally {
            log.info("application is closing");
        }
    }

    public class ConsumerThread implements Runnable {
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(String bootstrapServers,
                              String groupId,
                              CountDownLatch latch) {
            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
            this.latch = latch;

            consumer = new KafkaConsumer<String, String>(props);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(Collections.singleton(topic));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record: records) {
                        log.info("Key: {}, Value: {}, Partition: {}, Offset: {}",
                                record.key(), record.value(), record.partition(), record.offset());
                    }
                }
            } catch (WakeupException ex) {
                log.info("shutting down");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
