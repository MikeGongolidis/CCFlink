package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RollingAggregations {
    public static void main(String[] args) throws Exception {
        // Program Starts
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = "file.csv";
        String outFilePath = "out.txt";
        DataStreamSource<String> source = env.readTextFile(inFilePath);

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

        //keyBy sensorID
        KeyedStream<Tuple3<Long, String, Double>, Tuple> keyedStream = mapStream.keyBy(1);
        SingleOutputStreamOperator<Tuple3<Long, String, Double>> min = keyedStream.min(2);
        SingleOutputStreamOperator<Tuple3<Long, String, Double>> max = keyedStream.max(2);
        SingleOutputStreamOperator<Tuple3<Long, String, Double>> sum = keyedStream.sum(2);
        min.print();
        max.print();
        sum.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Program Ends
    }
}
