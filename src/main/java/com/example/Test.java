package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class Test {
    public static void main(String[] args) throws Exception {
        // Program Starts
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = "file.csv";
        String outFilePath = "out.csv";
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

	SingleOutputStreamOperator<Tuple3<Long, String, Double>> mapStream = source.map(
                new MapFunction<String, Tuple3<Long, String, Double>>() {
                    @Override
                    public Tuple3<Long, String, Double> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple3<Long, String, Double> out = new Tuple3(Long.parseLong(fieldArray[0]), fieldArray[1], Double.parseDouble(fieldArray[2]));
                        return out;
                    }
                }
        );

	KeyedStream<Tuple3<Long,String,Double>,Tuple> keyedStream = mapStream.assignTimestampsAndWatermarks(
		new AscendingTimestampExtractor<Tuple3<Long,String,Double>>(){
		@Override
		public long extractAscendingTimestamp(Tuple3<Long, String, Double> element) {

            return element.f0 * 1000;
        }
		}).keyBy(1);
	


	SingleOutputStreamOperator<Tuple3<Long, String, Double>> sum = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(3),Time.seconds(1))).apply(new SimpleSum());


        sum.writeAsCsv(outFilePath, FileSystem.WriteMode.OVERWRITE);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Program Ends

    }
}


class SimpleSum implements WindowFunction<Tuple3<Long,String,Double>, Tuple3<Long, String, Double>, Tuple, TimeWindow> {
@Override
public void apply(Tuple key, TimeWindow window, Iterable<Tuple3<Long, String, Double>> input,
Collector<Tuple3<Long, String, Double>> out) throws Exception {
Iterator<Tuple3<Long, String, Double>> iterator = input.iterator();
Tuple3<Long, String, Double> first = iterator.next();
String id = "";
Long ts = 0L;
Double temp = 0.0;
if(first != null){
id=first.f1;
ts=first.f0;
temp=first.f2;
}
while(iterator.hasNext()){
Tuple3<Long, String, Double> next = iterator.next();
ts=next.f0;
temp += next.f2;
}
out.collect(new Tuple3<Long, String, Double>(ts, id, temp));
}
}
