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

/*
* 		We need the function that calculates the average speed (Average)
* 		and the output to be created as it is requested in the pdf.
* 	    Then parallelism
* */


public class AverageSpeed {
    public static void main(String[] args) throws Exception {
        // Program Starts
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = "cars.csv";
        String outFilePath = "averageSpeed.csv";
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	//mapping
	SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> mapFilterStream = source.map(
            new MapFunction<String, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>() {
                @Override
                public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map(String in) throws Exception {
                    String[] fieldArray = in.split(",");
                    Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> out = new Tuple6(Integer.parseInt (fieldArray[0]), Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[6]),Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[7]));
                    return out;
                }
            } //filtering
        ).filter(new FilterFunction<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>() {
            @Override
            public boolean filter(Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> in) throws Exception {
                if (in.f3 > 51 && in.f3 < 57) {
                    return true;
                } else {
                    return false;
                }
            }
        });
	//timestamping and keying
	KeyedStream<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>,Tuple> keyedStream = mapFilterStream.assignTimestampsAndWatermarks(
		new AscendingTimestampExtractor<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>(){
		@Override
		public long extractAscendingTimestamp(Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> element) {

            return element.f0 * 1;
        }
		}).keyBy(1);
	

	//We have to call this but with an Average function that calculates the average.Its like SimpleSum on the examples

	//SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> averageSpeedStream;
		//averageSpeedStream =  keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(60))).apply(new Average());


		keyedStream.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Program Ends

    }
}


