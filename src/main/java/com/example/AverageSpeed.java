package com.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/*
* 		We need the function that calculates the average speed (Average)
* 		and the output to be created as it is requested in the pdf.
* 	    Then parallelism
* */


public class AverageSpeed {
    public static void main(String[] args) throws Exception {
        // Program Starts
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = "/media/bart/SSD_DATA/EIT/year_1/Cloud_computers/mike/data/input_lite.csv";
        String outFilePath = "/media/bart/SSD_DATA/EIT/year_1/Cloud_computers/mike/data/output.csv";
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	//mapping
	SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> mapFilterStream = source
            .map(new LoadData())//filtering
            .filter(new FilterSegment());

	//timestamping and keying
	KeyedStream<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>,Tuple> keyedStream = mapFilterStream.assignTimestampsAndWatermarks(
		new AscendingTimestampExtractor<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>(){
		@Override
		public long extractAscendingTimestamp(Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> element) {

            return element.f0 * 1;
        }
		}).keyBy(1);
	

	//We have to call this but with an Average function that calculates the average.Its like SimpleSum on the examples

	SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> averageSpeedStream =  keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(60))).apply(new Average());

		keyedStream.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Program Ends

    }

    private static class Average implements WindowFunction<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>, Tuple, TimeWindow> {
        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterable, Collector<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> collector) {
            Iterator<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterator = iterable.iterator();

            Integer Time1 = 0;
            Integer Time2 = 0;
            Integer Pos1 = 0;
            Integer Pos2 = 0;
            Integer AvgSpd = 0;
            Integer VID = 0;
            Integer Dir1 = 0;
            Integer Dir2 = 0;
            Integer XWay = 0;

            while (iterator.hasNext()) {
                Tuple6<Integer, Integer, Integer, Integer, Integer, Integer> next = iterator.next();

                if (next.f3 == 52) { //TODO fix multiple segments of the same
                        Time1 = next.f0;
                        Pos1 = next.f5;
                        VID = next.f1;
                        Dir1 = next.f4;
                        XWay = next.f2;
                } else if (next.f3 == 56) {
                        Time2 = next.f0;
                        Pos2 = next.f5;
                        Dir2 = next.f4;
                }


            }
            if ((Pos1 != 0 && Pos2 != 0) && (Dir1 == Dir2)) {
                AvgSpd = Math.abs(Pos2 - Pos1) / Math.abs(Time2 - Time1); //TODO convert to miles/hour
                if (AvgSpd > 60) {
                    collector.collect(new Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>(Time1, Time2, VID, XWay, Dir1, AvgSpd));
                }
            }
        }

    }

    //load Data
    private static class LoadData implements MapFunction<String, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {
        @Override
        public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map(String in) throws Exception {
            String[] fieldArray = in.split(",");
            Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> out = new Tuple6(Integer.parseInt (fieldArray[0]), Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[6]),Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[2]));
            return out;
        }
    }

    //Filter segments
    private static class FilterSegment implements FilterFunction<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {
        @Override
        public boolean filter(Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> in) throws Exception {
            if (in.f3 > 51 && in.f3 < 57) {
                return true;
            } else {
                return false;
            }
        }
    }

}


