package counter;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.text.SimpleDateFormat;
import java.util.*;

public class kafkaProducerTest {
    public static void main(String[] args) throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", "localhost:9092");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);
        Date now = new Date(2019 - 1900, Calendar.JULY, 15, 11, 29, 0);
        Random r = new Random(1000);
        HashMap<String, Long> pvCounter = new HashMap<String, Long>();
        HashMap<String, HashSet<Long>> uvCounter = new HashMap<String, HashSet<Long>>();
        for (Long i = 0L; i < 100; i++) {
            Date newDate = new Date(now.getTime() + i * 1 * 60 * 1000);
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            ObjectMapper mapper = new ObjectMapper();
            long uid = (long) r.nextInt(100);
            String date = new SimpleDateFormat("yyyy-MM-dd").format(newDate);
            if (!pvCounter.containsKey(date)) {
                pvCounter.put(date, 0L);
            }
            pvCounter.replace(date, pvCounter.get(date) + 1);
            if (!uvCounter.containsKey(date)) {
                uvCounter.put(date, new HashSet<>());
            }
            uvCounter.get(date).add(uid);
            TestEvent testEvent = new TestEvent(i, newDate.getTime(), uid);
            String json = mapper.writeValueAsString(testEvent);
            ProducerRecord<String, String> record = new ProducerRecord<>("test", i.toString(), json);
            try {
                producer.send(record).get();
            } catch (Exception e) {
                e.printStackTrace();
            }
            Thread.sleep(100);
        }
        for (String date : pvCounter.keySet()) {
            System.out.println("date:" + date + " pv:" + pvCounter.get(date));
            System.out.println("date:" + date + " uv:" + uvCounter.get(date).size());
        }
    }

}
