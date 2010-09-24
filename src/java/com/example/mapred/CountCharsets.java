package com.example.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.archive.io.hdfs.HDFSWriterDocument;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 * This Map/Reduce application generates counts for each unique
 * character encoding (charset) encountered in a Heritrix crawl.
 *
 * @author Doug Judd
 */
public class CountCharsets extends Configured implements Tool {

    /**
    * If character encoding can be determined, emits it as
    * (<b>charset</b>, <b>1</b>).
    */
    public static class MapClass extends MapReduceBase implements Mapper<Text, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);

        public void map(Text uri, Text docText, OutputCollector<Text, LongWritable> collector, Reporter reporter) throws IOException {

            HDFSWriterDocument hdfsDoc = new HDFSWriterDocument();
            hdfsDoc.readFields(new DataInputStream(new ByteArrayInputStream(docText.getBytes())));
            
            if (hdfsDoc.getCharset() != null) {
                collector.collect(new Text(hdfsDoc.getCharset()), one);
            }
        }
    }

    public int run(String[] args) throws Exception {
        JobConf jobConf = new JobConf(getConf(), CountCharsets.class);
        jobConf.setJobName("Count Charsets");

        FileInputFormat.setInputPaths(jobConf, new Path(args[0]));
	    FileOutputFormat.setOutputPath(jobConf, new Path(args[1]));

        jobConf.setInputFormat(SequenceFileInputFormat.class);

        // the output keys are words (strings)
        jobConf.setOutputKeyClass(Text.class);
        // the output values are counts (ints)
        jobConf.setOutputValueClass(LongWritable.class);

        jobConf.setMapperClass(MapClass.class);
        jobConf.setCombinerClass(LongSumReducer.class);
        jobConf.setReducerClass(LongSumReducer.class);

        jobConf.setNumReduceTasks(1);

        RunningJob job = JobClient.runJob(jobConf);
        job.waitForCompletion();
        return 0;
    }

    /**
    * The main driver for CountCharsets map/reduce program.
    * Invoke this method to submit the map/reduce job.
    * @throws IOException When there is communication problems with the
    *                     job tracker.
    */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CountCharsets(), args);
        System.exit(res);
    }

}
