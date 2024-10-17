package com.example.bigdata;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

public class Main extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Main(), args);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf(), "AccidentAnalysis");
        job.setJarByClass(this.getClass());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(AccidentMapper.class);
        job.setReducerClass(AccidentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class AccidentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final Text outputKey = new Text();
        private final IntWritable outputValue = new IntWritable();

        public void map(LongWritable offset, Text lineText, Context context) {
            try {
                String line = lineText.toString();
                String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                // Sprawdzamy datę i kod pocztowy
                String crashDate = fields[0];
                String zipCode = fields[2];

                // Dodajemy walidację daty
                if (zipCode.isEmpty() || crashDate.length() < 10) {
                    return; // Pomijamy wpisy bez kodu pocztowego i nieprawidłowo sformatowane daty
                }

                String yearString = crashDate.substring(crashDate.length() - 4);
                int year;
                try {
                    year = Integer.parseInt(yearString);
                } catch (NumberFormatException e) {
                    return; // Pomijamy wpisy z nieprawidłowym rokiem
                }

                if (year <= 2012) {
                    return; // Pomijamy wpisy przed 2013 rokiem
                }

                // Pobieramy nazwy ulic
                String[] streets = { fields[6], fields[7], fields[8] };

                // Liczba poszkodowanych
                int injuredPedestrians = Integer.parseInt(fields[11]);
                int killedPedestrians = Integer.parseInt(fields[12]);
                int injuredCyclists = Integer.parseInt(fields[13]);
                int killedCyclists = Integer.parseInt(fields[14]);
                int injuredMotorists = Integer.parseInt(fields[15]);
                int killedMotorists = Integer.parseInt(fields[16]);

                // Generowanie kluczy dla każdej ulicy powiązanej z wypadkiem
                for (String street : streets) {
                    if (street != null && !street.isEmpty()) {
                        // Ranni piesi
                        if (injuredPedestrians > 0) {
                            outputKey.set(street + "," + zipCode + ",Pedestrians,Injured");
                            outputValue.set(injuredPedestrians);
                            context.write(outputKey, outputValue);
                        }
                        // Zabici piesi
                        if (killedPedestrians > 0) {
                            outputKey.set(street + "," + zipCode + ",Pedestrians,Killed");
                            outputValue.set(killedPedestrians);
                            context.write(outputKey, outputValue);
                        }
                        // Ranni rowerzyści
                        if (injuredCyclists > 0) {
                            outputKey.set(street + "," + zipCode + ",Cyclists,Injured");
                            outputValue.set(injuredCyclists);
                            context.write(outputKey, outputValue);
                        }
                        // Zabici rowerzyści
                        if (killedCyclists > 0) {
                            outputKey.set(street + "," + zipCode + ",Cyclists,Killed");
                            outputValue.set(killedCyclists);
                            context.write(outputKey, outputValue);
                        }
                        // Ranni kierowcy
                        if (injuredMotorists > 0) {
                            outputKey.set(street + "," + zipCode + ",Motorists,Injured");
                            outputValue.set(injuredMotorists);
                            context.write(outputKey, outputValue);
                        }
                        // Zabici kierowcy
                        if (killedMotorists > 0) {
                            outputKey.set(street + "," + zipCode + ",Motorists,Killed");
                            outputValue.set(killedMotorists);
                            context.write(outputKey, outputValue);
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    public static class AccidentReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }
}
