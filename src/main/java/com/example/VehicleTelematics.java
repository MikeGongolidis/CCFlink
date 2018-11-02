package com.example;

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
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;


// NOW YOU NEED TO SPECIFY THE INPUT FILE AND OUTPUT FOLDER IN THE ARGUMENTS.
public class VehicleTelematics {
    public static void main(String[] args) throws Exception {
        // Program Starts
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = args[0];
        String outFilePath = args[1];
        DataStreamSource<String> source = env.readTextFile(inFilePath);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // PART 1

        SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> filterOut = source.map(
                new LoadData())
                .filter(new FilterSpeed());

        filterOut.writeAsText(outFilePath+"speedfines.csv", FileSystem.WriteMode.OVERWRITE);


        // PART 3

        SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> mapStream = source.map(
                new MapFunction<String, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>() {
                    @Override
                    public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map(String in) throws Exception {
                        String[] fieldArray = in.split(",");
                        Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> out = new Tuple6(Integer.parseInt (fieldArray[0]), Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[6]),Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[7]));
                        return out;
                    }
                }
        );

        KeyedStream<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>,Tuple> keyedStream = mapStream.assignTimestampsAndWatermarks(
                new AscendingTimestampExtractor<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>>(){
                    @Override
                    public long extractAscendingTimestamp(Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> element) {

                        return element.f0 * 1;
                    }
                }).keyBy(1);



        SingleOutputStreamOperator<Tuple7<Integer,Integer,Integer,Integer,Integer,Integer,Integer>> accidents;
        accidents = keyedStream.countWindow(4,1).apply(new WindowFunction<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>, Tuple7<Integer, Integer, Integer, Integer, Integer, Integer,Integer>, Tuple, GlobalWindow>() {
            @Override
            public void apply(Tuple tuple, GlobalWindow globalWindow, Iterable<Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> iterable, Collector<Tuple7<Integer, Integer, Integer, Integer, Integer, Integer,Integer>> collector) throws Exception {
                Iterator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> iterator = iterable.iterator();
                Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> first = iterator.next();

                Integer time1 = 0;
                Integer time2 = 0;
                Integer VID = 0;
                Integer Xway = 0;
                Integer Seg = 0;
                Integer Dir = 0;
                Integer Pos1 = 0;
                Integer Pos2 = 0;
                Integer Flag = 0;
                if(first != null){
                    Pos1=first.f5;
                    time1=first.f0;
                    VID = first.f1;
                    Xway = first.f2;
                    Seg = first.f3;
                    Dir=first.f4;
                }
                while(iterator.hasNext()){
                    Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> next = iterator.next();
                    Pos2=next.f5;
                    time2=next.f0;
                    if(Pos1 - Pos2 == 0 ){
                        Flag=Flag+1;
                    }
                }
                if(Flag==3){
                    collector.collect(new Tuple7<Integer, Integer, Integer, Integer, Integer, Integer,Integer>(time1, time2, VID,Xway,Seg,Dir,Pos1));
                }
            }
        });


        accidents.writeAsCsv(outFilePath+"accidents.csv", FileSystem.WriteMode.OVERWRITE);




        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
        // Program Ends
    }

    private static class LoadData implements MapFunction<String, Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {
        @Override
        public Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> map(String in) throws Exception {
            String[] fieldArray = in.split(",");
            Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> out = new Tuple6(Integer.parseInt (fieldArray[0]), Integer.parseInt(fieldArray[1]),Integer.parseInt(fieldArray[3]),Integer.parseInt(fieldArray[6]),Integer.parseInt(fieldArray[5]),Integer.parseInt(fieldArray[2]));
            return out;
        }
    }


    private static class FilterSpeed implements FilterFunction<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> {
        @Override
        public boolean filter(Tuple6<Integer,Integer,Integer,Integer,Integer,Integer> in) throws Exception {
            if (in.f5 > 90) {
                return true;
            } else {
                return false;
            }
        }
    }

}