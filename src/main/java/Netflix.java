import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Netflix {

    public static class FirstMapperFunc extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String str[] = value.toString().split(",");
            String val = value.toString();
            if (!val.contains(":")) {
                int num1 = Integer.parseInt(str[0]);
                int num2 = Integer.parseInt(str[1]);
                context.write(new IntWritable(num1), new IntWritable(num2));
            }
        }
    }

    public static class FirstReducerFunc extends Reducer<IntWritable, IntWritable, IntWritable, DoubleWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            double sumOfVals = 0.0;
            double res = 0.0;
            int len = 0;
        
            for(IntWritable val : values)
            {
                sumOfVals =sumOfVals+val.get();
                len++;
            }

            res = sumOfVals / len;
            res = res * 10;
            res = Math.floor(res);
            context.write(key, new DoubleWritable(res));

        }

    }

    public static class SecondMapperFunc extends Mapper<Object, Text, DoubleWritable, IntWritable> {
    @Override
        public void map ( Object key, Text value, Context context )
        throws IOException, InterruptedException {

		String str[] = value.toString().split("\t");
		context.write(new DoubleWritable(Double.parseDouble(str[1])),new IntWritable(1));

        }

    }

    public static class SecondReducerFunc extends Reducer<DoubleWritable, IntWritable, DoubleWritable, IntWritable> {
        @Override
        public void reduce(DoubleWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sumOfVals = 0;
            double val = key.get();
            val = val / 10.0;

            for(IntWritable vall: values) {

                sumOfVals = sumOfVals + vall.get();
            }

            context.write(new DoubleWritable(val), new IntWritable(sumOfVals));

        }

    }

    public static void main(String[] args) throws Exception {

        Job firstjob = Job.getInstance();
        firstjob.setJobName("FirstJob");
        firstjob.setJarByClass(Netflix.class);
        firstjob.setOutputKeyClass(IntWritable.class);
        firstjob.setOutputValueClass(DoubleWritable.class);
        firstjob.setMapOutputKeyClass(IntWritable.class);
        firstjob.setMapOutputValueClass(IntWritable.class);
        firstjob.setMapperClass(FirstMapperFunc.class);
        firstjob.setReducerClass(FirstReducerFunc.class);
        firstjob.setInputFormatClass(TextInputFormat.class);
        firstjob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(firstjob,new Path(args[0]));
        FileOutputFormat.setOutputPath(firstjob,new Path(args[1]));
        firstjob.waitForCompletion(true);

        Job secondjob = Job.getInstance();
        secondjob.setJobName("FirstJob");
        secondjob.setJarByClass(Netflix.class);
        secondjob.setOutputKeyClass(DoubleWritable.class);
        secondjob.setOutputValueClass(IntWritable.class);
        secondjob.setMapOutputKeyClass(DoubleWritable.class);
        secondjob.setMapOutputValueClass(IntWritable.class);
        secondjob.setMapperClass(SecondMapperFunc.class);
        secondjob.setReducerClass(SecondReducerFunc.class);
        secondjob.setInputFormatClass(TextInputFormat.class);
        secondjob.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(secondjob,new Path(args[1]));
        FileOutputFormat.setOutputPath(secondjob,new Path(args[2]));
        secondjob.waitForCompletion(true);

    }

}

