package master2018.flink;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


// NOW YOU NEED TO SPECIFY THE INPUT FILE AND OUTPUT FOLDER IN THE ARGUMENTS.
public class SpeedRadar {
    public static void main(String[] args) throws Exception {
        // Program Starts
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String inFilePath = args[0];
        String outFilePath = args[1];
        DataStreamSource<String> source = env.readTextFile(inFilePath);

        SingleOutputStreamOperator<Tuple6<Integer,Integer,Integer,Integer,Integer,Integer>> filterOut = source.map(
                new LoadData())
        .filter(new FilterSpeed());
        filterOut.writeAsText(outFilePath+"speedfines.csv", FileSystem.WriteMode.OVERWRITE);
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
