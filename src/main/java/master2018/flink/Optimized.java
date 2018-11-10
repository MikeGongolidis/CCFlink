package master2018.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class Optimized {
    public static void main(String[] args) throws Exception {
        if (args.length <2 ) {
            throw new IllegalArgumentException("Please provide <Input file> <output folder>");
        }

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = args[0];
        String outFilePath = args[1];
        DataStreamSource<String> source = env.readTextFile(inFilePath).setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        SingleOutputStreamOperator<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> mappedStream = source.map(new LoadData());
        //KeyedStream<Tuple7<Integer, Integer, Byte,Byte,Byte,Integer, Integer>,Tuple> keyedStream = mappedStream.keyBy(1);
        // PART 1


        SingleOutputStreamOperator<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> speedFilter = mappedStream.filter(new FilterSpeed());

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Byte, Byte, Integer, Integer>> finalSpeedFilter = speedFilter.map(new MapData());

        //PART2
        SingleOutputStreamOperator<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> avgFilter = mappedStream.filter(new FilterSegment());


        KeyedStream<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>, Tuple> avgKeyedStream = avgFilter.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> element) {
                        return element.f0 * 1;
                    }
                }).keyBy(1);

        SingleOutputStreamOperator<Tuple6<Integer, Integer, Integer, Byte, Byte, Double>> averageSpeedStream;
        averageSpeedStream = avgKeyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(60))).apply(new Average());

        // PART 3


        KeyedStream<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>, Tuple> accKeyedStream = mappedStream.filter(new FilterSpeed3()).setParallelism(1).assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> element) {
                        return element.f0 * 1;
                    }
                }).keyBy(1);

        SingleOutputStreamOperator<Tuple7<Integer, Integer, Integer, Byte, Byte, Byte, Integer>> accidents;
        accidents = accKeyedStream.countWindow(4, 1).apply(new Accidents());


        //Output

        finalSpeedFilter.writeAsCsv(outFilePath + "speedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        averageSpeedStream.writeAsCsv(outFilePath + "avgspeedfines.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        accidents.writeAsCsv(outFilePath + "accidents.csv", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static class LoadData implements MapFunction<String, Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> {
        @Override
        public Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> map(String in) throws Exception {
            String[] fieldArray = in.split(",");
            Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> out = new Tuple7(Integer.parseInt(fieldArray[0]), Integer.parseInt(fieldArray[1]), Byte.parseByte(fieldArray[3]), Byte.parseByte(fieldArray[6]), Byte.parseByte(fieldArray[5]), Integer.parseInt(fieldArray[7]), Integer.parseInt(fieldArray[2]));
            return out;
        }
    }

    private static class MapData implements MapFunction<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>, Tuple6<Integer, Integer, Byte, Byte, Integer, Integer>> {
        @Override
        public Tuple6<Integer, Integer, Byte, Byte, Integer, Integer> map(Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> in) throws Exception {

            Tuple6<Integer, Integer, Byte, Byte, Integer, Integer> out = new Tuple6<Integer, Integer, Byte, Byte, Integer, Integer>(in.f0, in.f1, in.f2, in.f3, in.f5, in.f6);
            return out;
        }
    }

    private static class FilterSpeed implements FilterFunction<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> {
        @Override
        public boolean filter(Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> in) throws Exception {
            if (in.f6 > 90) {
                return true;
            } else {
                return false;
            }
        }
    }

    private static class FilterSpeed3 implements FilterFunction<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> {
        @Override
        public boolean filter(Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> in) throws Exception {
            if (in.f6 == 0) {
                return true;
            } else {
                return false;
            }
        }
    }


    private static class FilterSegment implements FilterFunction<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> {
        @Override
        public boolean filter(Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> in) throws Exception {
            if (in.f3 > 51 && in.f3 < 57) {
                return true;
            } else {
                return false;
            }
        }
    }

    private static class Average implements WindowFunction<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>, Tuple6<Integer, Integer, Integer, Byte, Byte, Double>, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> iterable, Collector<Tuple6<Integer, Integer, Integer, Byte, Byte, Double>> collector) {
            Iterator<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> iterator = iterable.iterator();

            Integer Time1 = 0, Time2 = 0;
            Integer Pos1 = 0, Pos2 = 0;
            Double Avg;
            Integer VID = 0;
            Byte Dir1 = 0, Dir2 = 0;
            Byte XWay = 0;

            while (iterator.hasNext()) {
                Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> next = iterator.next();

                if (next.f3 == 52) { //check if segment is already exists and keeps the biggest
                    if (Pos1 < next.f5 && Pos1 != 0) {
                        //Do nothing
                    } else {
                        Time1 = next.f0;
                        Pos1 = next.f5;
                        VID = next.f1;
                        Dir1 = next.f4;
                        XWay = next.f2;
                    }

                } else if (next.f3 == 56) {
                    if (Pos2 > next.f5) {
                        //do nothing
                    } else {
                        Time2 = next.f0;
                        Pos2 = next.f5;
                        Dir2 = next.f4;
                    }
                }

            }
            if ((Pos1 != 0 && Pos2 != 0) && (Dir1 - Dir2 == 0)) {
                Avg = 2.2369 * (Math.abs(Pos2 - Pos1)) / (Math.abs(Time2 - Time1)); //to Miles per hour

                if (Avg > 60) {
                    collector.collect(new Tuple6<Integer, Integer, Integer, Byte, Byte, Double>(Time1, Time2, VID, XWay, Dir1, Avg));

                }
            }
        }
    }


    private static class Accidents implements WindowFunction<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>, Tuple7<Integer, Integer, Integer, Byte, Byte, Byte, Integer>, Tuple, GlobalWindow> {
        @Override
        public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> iterable, Collector<Tuple7<Integer, Integer, Integer, Byte, Byte, Byte, Integer>> collector) throws Exception {
            Iterator<Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer>> iterator = iterable.iterator();
            Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> first = iterator.next();

            Integer time1 = 0, time2 = 0;
            Integer VID = 0;
            Byte Xway = 0;
            Byte Seg = 0;
            Byte Dir1 = 0, Dir2 = 0;
            Integer Pos1 = 0, Pos2 = 0;
            Integer Flag = 0;

            if (first != null) {
                Pos1 = first.f5;
                time1 = first.f0;
                VID = first.f1;
                Xway = first.f2;
                Seg = first.f3;
                Dir1 = first.f4;

            }
            while (iterator.hasNext()) {

                Tuple7<Integer, Integer, Byte, Byte, Byte, Integer, Integer> next = iterator.next();
                Pos2 = next.f5;
                time2 = next.f0;
                Dir2 = next.f4;
                if (Pos1 - Pos2 == 0 && Dir1 - Dir2 == 0) {
                    Flag = Flag + 1;
                }
            }
            if (Flag == 3) {
                collector.collect(new Tuple7<Integer, Integer, Integer, Byte, Byte, Byte, Integer>(time1, time2, VID, Xway, Seg, Dir1, Pos1));
            }
        }
    }

}