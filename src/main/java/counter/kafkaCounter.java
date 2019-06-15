package counter;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Properties;

public class kafkaCounter {

    public static void main(String[] args) throws Exception {

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("zookeeper.connect", "localhost:2181");
        properties.setProperty("group.id", "test");
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5 * 60 * 1000); // checkpoint every 5000 msecs
        env.setStateBackend(new FsStateBackend("file:///home/reyzal/flink/checkpoint"));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), properties);
        DataStreamSource<String> stream = env
                .addSource(consumer.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
                    long currentMaxTimestamp = 0L;
                    long maxOutOfOrderness = 60 * 1000L;

                    @Override
                    public long extractTimestamp(String element, long previousElementTimestamp) {
                        ObjectMapper mapper = new ObjectMapper();
                        try {
                            TestEvent testEvent = mapper.readValue(element, TestEvent.class);
                            currentMaxTimestamp = testEvent.getAt();
                            return testEvent.getAt();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        return 0L;
                    }

                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                }));

        // parse the data, group it, window it, and aggregate the counts
        DataStream<String> windowCounts = stream
                .flatMap(new FlatMapFunction<String, TestEvent>() {
                    @Override
                    public void flatMap(String value, Collector<TestEvent> out) {
                        ObjectMapper mapper = new ObjectMapper();
                        try {
                            out.collect(mapper.readValue(value, TestEvent.class));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                })
                .windowAll(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(60)))
//                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(1)))
//                .trigger(CountTrigger.of(1))
                .aggregate(new AggregateFunction<TestEvent, myCounter, myCounter>() {
                    @Override
                    public myCounter createAccumulator() {
                        return new myCounter();
                    }

                    @Override
                    public myCounter add(TestEvent testEvent, myCounter myCounter) {
                        return myCounter.add(testEvent);
                    }

                    @Override
                    public myCounter getResult(myCounter myCounter) {
                        return myCounter;
                    }

                    @Override
                    public myCounter merge(myCounter myCounter, myCounter acc1) {
                        return myCounter.merge(acc1);
                    }
                }, new ProcessAllWindowFunction<myCounter, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<myCounter> iterable, Collector<String> collector) throws Exception {
                        myCounter first = iterable.iterator().next();
                        collector.collect("start:" + sdf.format(new Date(context.window().getStart())) +
                                " end:" + sdf.format(new Date(context.window().getEnd())) + " " + first);
                    }
                });
        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    static class myCounter {
        private Integer pvCounter;
        private HashSet<Long> uvCounter;

        myCounter() {
            this.pvCounter = 0;
            this.uvCounter = new HashSet<>();
        }

        public myCounter add(TestEvent e) {
            this.pvCounter += 1;
            this.uvCounter.add(e.getUid());
            return this;
        }

        public myCounter merge(myCounter other) {
            this.uvCounter.addAll(other.uvCounter);
            this.pvCounter += other.pvCounter;
            return this;
        }

        @Override
        public String toString() {
            return "myCounter{" +
                    "pvCounter=" + pvCounter +
                    ", uvCounter=" + uvCounter.size() +
                    '}';
        }
    }
}
