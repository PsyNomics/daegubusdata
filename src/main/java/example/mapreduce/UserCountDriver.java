package example.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class UserCountDriver extends Configured implements Tool {
    public static void main(String[] args) throws Exception {

        int exitCode = ToolRunner.run(new Configuration(), new UserCountDriver(), args);
        System.out.println("MapRedcue Job Result: " + exitCode);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: example.hadoop.BusLineUserCountDriver <input> <output> <output2>");
            System.exit(3);
        }

        Configuration conf = this.getConf();
        Job job1 = Job.getInstance(conf, "BusStationUserCount");
        job1.setNumReduceTasks(1);
        job1.setJarByClass(UserCountDriver.class);
        job1.setMapperClass(UserCountMapper.class);
        job1.setReducerClass(UserCountReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

//        MultipleOutputs.addNamedOutput(job1, "DayOfWeek", TextOutputFormat.class, Text.class, IntWritable.class);
        MultipleOutputs.addNamedOutput(job1, "Week", TextOutputFormat.class, Text.class, IntWritable.class);
        job1.waitForCompletion(true);

        getConf().set(TextOutputFormat.SEPARATOR, ",");
        Configuration conf2 = this.getConf();
        Job job2 = Job.getInstance(conf2, "Top");
        job2.setNumReduceTasks(1);
        job2.setJarByClass(UserCountDriver.class);
        job2.setMapperClass(TopUserMapper.class);
        job2.setReducerClass(TopUserReducer.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        boolean success = job2.waitForCompletion(true);
        return (success ? 0 : 1);

    }
}
