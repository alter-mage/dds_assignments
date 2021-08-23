import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class equijoin {
    public static class TupleMapper extends Mapper<Object, Text, Text, Text> {
        private Text Tuple = new Text();
        private Text JoinKey = new Text();

        public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
            String tokens[] = value.toString().split(",");
            int numTokens = tokens.length;
            String tupleBuilder = "";
            String tupleKey = tokens[1];
            for (String token : tokens) {
                tupleBuilder += token+",";
            }
            JoinKey.set(tupleKey);
            Tuple.set(tupleBuilder.substring(0, tupleBuilder.length()-1));
            context.write(JoinKey, Tuple);
        }
    }

    public static class TupleJoiner extends Reducer<Text, Text, Text, Text> {
        private Text EquiJoinTuple = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
            List<String> STupleList = new ArrayList<String>();
            List<String> RTupleList = new ArrayList<String>();
            String relationTrack = "";
            for (Text value : values) {
                String tuple = value.toString();
                String tokens[] = tuple.split(",");
                if (relationTrack.equals("")) {
                    relationTrack = tokens[0];
                }
                if (tokens[0].equals(relationTrack)) {
                    RTupleList.add(tuple);
                } else {
                    STupleList.add(tuple);
                }
            }
            if (STupleList.size() == 0 || RTupleList.size() == 0) {
                key.clear();
            } else {
                for (String rTuple : RTupleList) {
                    for (String sTuple : STupleList) {
                        String equiJoinTupleBuilder = rTuple + "," + sTuple;
                        EquiJoinTuple.set(equiJoinTupleBuilder);
                        context.write(key, EquiJoinTuple);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "equi join");
        job.setJarByClass(equijoin.class);
        job.setMapperClass(TupleMapper.class);
        job.setReducerClass(TupleJoiner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}