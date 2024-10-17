package com.example.bigdata;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class AvgSizeStations extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new AvgSizeStations(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "AvgSizeStations");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.setMapperClass(AvgSizeStationMapper.class);
        job.setCombinerClass(AvgSizeStationCombiner.class);
        job.setReducerClass(AvgSizeStationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SumCount.class);


        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AvgSizeStationMapper extends Mapper<LongWritable, Text, Text, SumCount> {

        private final Text year = new Text();
        private final SumCount sizeCount = new SumCount();

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                if (offset.get() != 0) {
                    String line = lineText.toString();
                    int i = 0;
                    for (String word : line
                            .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")) {
                        if (i == 4) {
                            year.set(word.substring(word.lastIndexOf('/') + 1,
                                    word.lastIndexOf('/') + 5));
                        }
                        if (i == 5) {
                            int size = Integer.parseInt(word);
                            sizeCount.set(new DoubleWritable(size), new IntWritable(1));
                        }
                        i++;
                    }
                    // Przekazanie SumCount do Combiner/Reducer
                    context.write(year, sizeCount);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static class AvgSizeStationReducer extends Reducer<Text, SumCount, Text, DoubleWritable> {

        private final DoubleWritable resultValue = new DoubleWritable();
        Float average;
        Float count;
        int sum;

        @Override
        public void reduce(Text key, Iterable<SumCount> values,
                           Context context) throws IOException, InterruptedException {
            average = 0f;
            count = 0f;
            double sum = 0;

            Text resultKey = new Text("average station size in " + key + " was: ");

            for (SumCount val : values) {
                sum += val.getSum().get();
                count += 1;
            }

            average = (float) (sum / count);

            resultValue.set(average);
            context.write(resultKey, resultValue);

        }
    }

    public static class AvgSizeStationCombiner extends Reducer<Text, SumCount, Text, SumCount> {

        private final SumCount sum = new SumCount(0.0d, 0);

        @Override
        public void reduce(Text key, Iterable<SumCount> values, Context context) throws IOException, InterruptedException {

            sum.set(new DoubleWritable(0.0d), new IntWritable(0));

            for (SumCount val : values) {
                sum.addSumCount(val);
            }
            context.write(key, sum);
        }
    }
}