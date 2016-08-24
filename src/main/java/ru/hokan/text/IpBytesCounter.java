package ru.hokan.text;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.MarkLogicNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class IpBytesCounter extends Configured implements Tool {

    private static final String ML_DATABASE_USERNAME = "admin";
    private static final String ML_DATABASE_PASSWORD = "admin";
    private static final String ML_XCC_SERVER_HOST = "localhost";
    private static final String ML_XCC_SERVER_PORT = "9999";
    private static final String ML_DATABASE_NAME = "ML-TEST";

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.println("Usage: <configFile> <inDir> <outDir>");
            ToolRunner.printGenericCommandUsage(System.out);
            System.exit(-1);
        }

        int res = ToolRunner.run(new Configuration(), new IpBytesCounter(), args);
        System.exit(res);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int run(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
        config.addResource(otherArgs[0]);
        config.set("mapred.textoutputformat.separator", ",");
        config.setInt(NLineInputFormat.LINES_PER_MAP, 1000);

        Job job = Job.getInstance(config);
        job.setJarByClass(IpBytesCounter.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setInputFormatClass(NLineInputFormat.class);
        job.setOutputFormatClass(ContentOutputFormat.class);

        job.setMapperClass(IpDataCountMapper.class);
        job.setReducerClass(IpDataCountReducer.class);
        job.setCombinerClass(IpDataCountCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(DocumentURI.class);
        job.setOutputValueClass(MarkLogicNode.class);

        boolean jobCompletion = job.waitForCompletion(true);

//        MarkLogicService.INSTANCE.setUserName(ML_DATABASE_USERNAME);
//        MarkLogicService.INSTANCE.setPassword(ML_DATABASE_PASSWORD);
//        MarkLogicService.INSTANCE.setHostIp(ML_XCC_SERVER_HOST);
//        MarkLogicService.INSTANCE.setPort(ML_XCC_SERVER_PORT);
//        MarkLogicService.INSTANCE.setDatabaseName(ML_DATABASE_NAME);
        return jobCompletion ? 0 : 1;
    }
}