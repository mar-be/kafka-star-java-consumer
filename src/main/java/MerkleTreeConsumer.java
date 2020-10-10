import com.goterl.lazycode.lazysodium.exceptions.SodiumException;
import com.goterl.lazycode.lazysodium.utils.Key;
import de.marvin.merkletree.Proof;
import de.marvin.sodium.KeyIO;
import de.marvin.sodium.SignatureUtil;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Properties;

public class MerkleTreeConsumer {

    public static String KAFKA_BROKERS = "localhost:9092";
    public static Integer MESSAGE_COUNT = 1000;
    public static String CLIENT_ID = "client1";
    public static String TOPIC_NAME = "test";
    public static String GROUP_ID_CONFIG = "consumerGroup1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 1000;
    public static String OFFSET_RESET_LATEST = "latest";
    public static String OFFSET_RESET_EARLIER = "earliest";
    public static Integer MAX_POLL_RECORDS = 1;

    public static void main(String[] args) throws IOException, SodiumException {
        runConsumer();
    }

    public static Consumer<Long, byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER);
        Consumer<Long, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        return consumer;
    }

    static void runConsumer() throws IOException {

        Consumer<Long, byte[]> consumer = createConsumer();
        Key key = KeyIO.readKeyFromFile("public1.key");

        int noMessageFound = 0;

        while (true) {
            ConsumerRecords<Long, byte[]> consumerRecords = consumer.poll(1000);
            // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
            if (consumerRecords.count() == 0) {
                noMessageFound++;
                if (noMessageFound > MAX_NO_MESSAGE_FOUND_COUNT)
                    // If no message found count is reached to threshold exit loop.
                    break;
                else
                    continue;
            }
            //print each record.
            for(ConsumerRecord<Long, byte[]> record: consumerRecords){
                System.out.println("-----------------------------------------------------");
                System.out.println("Record Key: " + record.key());
                System.out.println("Record value: " + new String(record.value()));
                System.out.println("Record partition: " + record.partition());
                System.out.println("Record offset: " + record.offset());
                Header merkleProof = record.headers().lastHeader("merkleProof");
                Proof proof = Proof.getFromBytes(merkleProof.value());
                byte[] value = record.value();
                ByteBuffer buffer = ByteBuffer.allocate(2*Long.BYTES + value.length);
                buffer.putLong(record.offset()).putLong(record.timestamp()).put(value);
                boolean valid = false;
                try {
                    valid = Proof.verify(buffer.array(), proof);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                System.out.println("Valid Merkle Proof: " + valid);
                Header signature = record.headers().lastHeader("signature");
                boolean validSign = SignatureUtil.verifySign(proof.getRootHash(), signature.value(), key);
                System.out.println("Valid Signature: " + validSign);
            }


            // commits the offset of record to broker.
            consumer.commitAsync();
        }
        consumer.close();
    }


}
