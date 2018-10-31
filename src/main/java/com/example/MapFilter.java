package com.example;



import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapFilter {
    public static void main(String[] args) throws Exception {
        // Program Starts
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = "file.csv";
        String outFilePath = "out.csv";
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple3<Long, String, Double>> filterOut = source.map(
            new MapFunction<String, Tuple3<Long, String, Double>>() {
                @Override
                public Tuple3<Long, String, Double> map(String in) throws Exception {
                    String[] fieldArray = in.split(",");
                    Tuple3<Long, String, Double> out = new Tuple3(Long.parseLong(fieldArray[0]), fieldArray[1], Double.parseDouble(fieldArray[2]));
                    return out;
                }
            }
        ).filter(new FilterFunction<Tuple3<Long, String, Double>>() {
            @Override
            public boolean filter(Tuple3<Long, String, Double> in) throws Exception {
                if (in.f2 > 20) {
                    return true;
                } else {
                    return false;
                }
            }
        });
        filterOut.writeAsText(outFilePath, FileSystem.WriteMode.OVERWRITE);
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Program Ends
    }
}
