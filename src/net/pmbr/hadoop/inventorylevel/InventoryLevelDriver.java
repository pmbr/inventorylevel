package net.pmbr.hadoop.inventorylevel;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InventoryLevelDriver extends Configured implements Tool {

	protected static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("YYYYMMDDHHmmSS");
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new InventoryLevelDriver(), args);

		System.exit(res);
	}

	public Configuration getConf() {
		return super.getConf();
	}

	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf());

		job.setJarByClass(InventoryLevelDriver.class);

		job.setMapperClass(InventoryLevelMapper.class);

		job.setReducerClass(InventoryLevelReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		Path input = new Path("/inventory/input");
		FileInputFormat.setInputPaths(job, input);

		String outputPath = "/inventory/output_" + DATE_FORMATTER.format(new Date());
		
		Path output = new Path(outputPath);
		FileOutputFormat.setOutputPath(job, output);

		if (job.waitForCompletion(true)) {
			return 0;
		}

		return 1;
	}
	
}