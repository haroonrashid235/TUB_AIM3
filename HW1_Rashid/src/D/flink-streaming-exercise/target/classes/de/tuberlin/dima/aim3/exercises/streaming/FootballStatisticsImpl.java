package de.tuberlin.dima.aim3.exercises.streaming;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import de.tuberlin.dima.aim3.exercises.model.DebsFeature;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.mesos.shaded.com.fasterxml.jackson.databind.node.BigIntegerNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;
import org.apache.flink.table.sources.wmstrategies.PeriodicWatermarkAssigner;
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import scala.tools.nsc.transform.patmat.Logic;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * The implementation of {@link FootballStatistics} to perform analytics on DEBS dataset.
 *
 * @author Imran, Muhammad
 */
public class FootballStatisticsImpl implements FootballStatistics {
    /*
        The StreamExecutionEnvironment is the context in which a streaming program is executed.
    */
    final StreamExecutionEnvironment STREAM_EXECUTION_ENVIRONMENT =
            StreamExecutionEnvironment.getExecutionEnvironment();

    private static final BigInteger START = BigInteger.valueOf(10753295594424116L);
    private static final BigInteger FIRST_HALF_END = BigInteger.valueOf(12557295594424116L);
    private static final BigInteger SECOND_HALF_START = BigInteger.valueOf(13086639146403495L);
    private static final BigInteger END = BigInteger.valueOf(14879639146403495L);
    /*
        File path of the dataset.
    */
    private final String filePath;
    /**
     * stream of events to be evaluated lazily
     */
    private DataStream<DebsFeature> events;

    /**
     * @param filePath dataset file path as a {@link String}
     */
    FootballStatisticsImpl(String filePath) {
        this.filePath = filePath;
    }

    /**
     * write the events that show that ball almost came near goal (within penalty area),
     * but the ball was kicked out of the penalty area by a player of the opposite team.
     */
    @Override
    public void writeAvertedGoalEvents() {
        STREAM_EXECUTION_ENVIRONMENT.setParallelism(1);

        events.assignTimestampsAndWatermarks(new WatermarkAssigner()).
                filter(new BallSensorInMatchFilter())
                .keyBy(DebsFeature::getSensorId)
                .timeWindow(Time.minutes(1))
                .apply(new AvertedGoalWindowFunction())
                .keyBy(0)
                .map(new AvertedGoalAggregator())
                .writeAsCsv("src/main/resources/avertedGoals", FileSystem.WriteMode.OVERWRITE);
    }

    /**
     * Highest average distance of player A1 (of Team A) ran in every 5 minutes duration.
     */
    @Override
    public void writeHighestAvgDistanceCovered() {
        STREAM_EXECUTION_ENVIRONMENT.setParallelism(1);

        events.assignTimestampsAndWatermarks(new WatermarkAssigner())
                .filter(new SingleSensorPlayerInMatchFilter())
                .keyBy(DebsFeature::getSensorId)
                .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
                .apply(new SingleSensorDistanceWindowFunction())
                .writeAsCsv("src/main/resources/highestAvg", FileSystem.WriteMode.OVERWRITE);

    }

    /**
     * Creates {@link StreamExecutionEnvironment} and {@link DataStream} before each streaming task
     * to initialize a stream from the beginning.
     */
    @Override
    public void initStreamExecEnv() {

         /*
          Setting the default parallelism of our execution environment.
          Feel free to change it according to your environment/number of operators, sources, and sinks you would use.
          However, it should not have any impact on your results whatsoever.
         */
        STREAM_EXECUTION_ENVIRONMENT.setParallelism(1);

        /*
          Event time is the time that each individual event occurred on its producing device.
          This time is typically embedded within the records before they enter Flink.
         */
        STREAM_EXECUTION_ENVIRONMENT.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*
          Reads the file as a text file.
         */
        DataStream<String> dataStream = STREAM_EXECUTION_ENVIRONMENT.readTextFile(filePath);

        /*
          Creates DebsFeature for each record.
          ALTERNATIVELY You can use Tuple type (For that you would need to change generic type of 'event').
         */
        events = dataStream.map(DebsFeature::fromString);
    }

    public void execEnv() throws Exception {
        STREAM_EXECUTION_ENVIRONMENT.execute("Flink2");
    }

    private class SingleSensorPlayerInMatchFilter implements FilterFunction<DebsFeature> {
        @Override
        public boolean filter(DebsFeature debsFeature) throws Exception {
            return (debsFeature.getSensorId() == 16) && valueInMatchTimeBounds(debsFeature.getTimeStamp());
        }

        private boolean valueInMatchTimeBounds(BigInteger timeStamp) {
            return (timeStamp.compareTo(START) >= 0 &&
                    timeStamp.compareTo(FIRST_HALF_END) < 0) || (
                    timeStamp.compareTo(SECOND_HALF_START) >= 0 && timeStamp.compareTo(END) < 0);
        }
    }

    private static class SingleSensorDistanceWindowFunction extends RichWindowFunction<DebsFeature, Tuple3<BigInteger, BigInteger, Double>, Long, TimeWindow> {

        private transient ValueState<Tuple3<BigInteger, BigInteger, Double>> maxDistance;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Tuple3<BigInteger, BigInteger, Double>> stateDescriptor =
                    new ValueStateDescriptor<Tuple3<BigInteger, BigInteger, Double>>(
                            "max-avg",
                            TypeInformation.of(new TypeHint<Tuple3<BigInteger, BigInteger, Double>>() {}));
            maxDistance = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<DebsFeature> input, Collector<Tuple3<BigInteger, BigInteger, Double>> out) throws Exception {
            List<DebsFeature> inputs = new ArrayList<>();
            Iterables.addAll(inputs, input);

            inputs.sort(Comparator.comparing(DebsFeature::getTimeStamp));

            double aggDistance = 0;

            for (int i = 1; i < inputs.size(); i++) {
                aggDistance = aggDistance + euclidean(inputs.get(i - 1).getPositionX(), inputs.get(i - 1).getPositionY(), inputs.get(i).getPositionX(),
                        inputs.get(i).getPositionY());
            }

            double avgDistance = aggDistance / (inputs.size() - 1);
            if (maxDistance.value() == null || maxDistance.value().f2 < avgDistance) {
                maxDistance.update(new Tuple3<>(inputs.get(0).getTimeStamp(), inputs.get(inputs.size() - 1).getTimeStamp(), avgDistance));
            }

            out.collect(maxDistance.value());

        }

        private double euclidean(int X1, int Y1, int X2, int Y2) {
            return Math.sqrt(
                    Math.pow(X1 - X2, 2) + Math.pow(Y1 - Y2, 2)
            );
        }
    }


    private class WatermarkAssigner implements AssignerWithPunctuatedWatermarks<DebsFeature> {

        @Nullable
        @Override
        public Watermark checkAndGetNextWatermark(DebsFeature debsFeature, long l) {
            return new Watermark(l);
        }

        @Override
        public long extractTimestamp(DebsFeature debsFeature, long l) {
            return debsFeature.getTimeStamp().divide(new BigInteger("1000000000")).longValue();
        }
    }

    private class BallSensorInMatchFilter implements FilterFunction<DebsFeature> {
        private final List<Long> ids = Arrays.asList(4L, 8L, 10L, 12L);

        @Override
        public boolean filter(DebsFeature debsFeature) throws Exception {
            return ids.contains(debsFeature.getSensorId()) && valueInMatchTimeBounds(debsFeature.getTimeStamp());
        }

        private boolean valueInMatchTimeBounds(BigInteger timeStamp) {
            return (timeStamp.compareTo(START) >= 0 &&
                    timeStamp.compareTo(FIRST_HALF_END) < 0) || (
                    timeStamp.compareTo(SECOND_HALF_START) >= 0 && timeStamp.compareTo(END) < 0);
        }
    }

    public static class AvertedGoalWindowFunction extends RichWindowFunction<DebsFeature, Tuple2<String, Integer>, Long, TimeWindow> {

        private transient ValueState<Boolean> inA;
        private transient ValueState<Boolean> inB;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Boolean> inAdescriptor = new ValueStateDescriptor<>("inA", TypeInformation.of(Boolean.class));
            inA = getRuntimeContext().getState(inAdescriptor);

            ValueStateDescriptor<Boolean> inBdescriptor = new ValueStateDescriptor<>("inB", TypeInformation.of(Boolean.class));
            inB = getRuntimeContext().getState(inBdescriptor);

        }

        @Override
        public void apply(Long aLong, TimeWindow window, Iterable<DebsFeature> input, Collector<Tuple2<String, Integer>> out) throws Exception {
            List<DebsFeature> inputs = new ArrayList<>();
            Iterables.addAll(inputs, input);

            inputs.sort(Comparator.comparing(DebsFeature::getTimeStamp)); // Handle out of order events.

            if (inA.value() == null) {
                inA.update(false);
            }

            if (inB.value() == null) {
                inB.update(false);
            }


            for (DebsFeature f : inputs) {
                if (!inA.value() && isInA(f)) {
                    inA.update(true);
                }
                else if (inA.value() && !isInA(f)) {
                    if (!isGoalA(f)) {
                        out.collect(new Tuple2<>("A", 1));
                    }

                    inA.update(false);
                }

                else if (!inB.value() && isInB(f)) {
                    inB.update(true);
                }
                else if (inB.value() && !isInB(f)) {
                    if (!isGoalB(f)) {
                        out.collect(new Tuple2<>("B", 1));
                    }
                    inB.update(false);
                }
            }

        }

        /**
         * 6300 < x < 46183 and 15940 < y < 33940
         *
         * @param feature
         * @return
         */
        private boolean isInA(DebsFeature feature) {
            return feature.getPositionX() > 6300 && feature.getPositionX() < 46183 &&
                    15940 < feature.getPositionY() && feature.getPositionY() < 33940;
        }

        /**
         * goes in to the goal
         *
         * @param feature
         * @return
         */
        private boolean isGoalA(DebsFeature feature) {
            return feature.getPositionX() > 6300 && feature.getPositionX() < 46183 &&
                    feature.getPositionY() >= 33940;
        }


        /**
         * 6300 < x < 46183 and -33968 < y < -15965
         *
         * @param feature
         * @return
         */
        private boolean isInB(DebsFeature feature) {
            return feature.getPositionX() > 6300 && feature.getPositionX() < 46183 &&
                    -33968 < feature.getPositionY() && feature.getPositionY() < -15965;
        }

        private boolean isGoalB(DebsFeature feature) {
            return feature.getPositionX() > 6300 && feature.getPositionX() < 46183 &&
                    -33968 >= feature.getPositionY();
        }


    }

    public static class AvertedGoalAggregator extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private transient ValueState<Integer> avA;
        private transient ValueState<Integer> avB;

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> avAdescriptor = new ValueStateDescriptor<>("avA", TypeInformation.of(Integer.class));
            avA = getRuntimeContext().getState(avAdescriptor);

            ValueStateDescriptor<Integer> avBdescriptor = new ValueStateDescriptor<>("avB", TypeInformation.of(Integer.class));
            avB = getRuntimeContext().getState(avBdescriptor);

        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {

            if (avA.value() == null) {
                avA.update(0);
            }

            if (avB.value() == null) {
                avB.update(0);
            }

            if ("A".equals(value.f0)) {
                avA.update(avA.value() + value.f1);
                return new Tuple2<>(value.f0, avA.value());
            }
            else {
                avB.update(avB.value() + value.f1);
                return new Tuple2<>(value.f0, avB.value());
            }

        }
    }


}
