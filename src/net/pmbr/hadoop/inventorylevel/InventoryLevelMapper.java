package net.pmbr.hadoop.inventorylevel;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InventoryLevelMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
	}

	@Override
	protected void map(LongWritable key, Text inventoryLevelLine, Context context) throws IOException, InterruptedException {
		String[] inventoryLevelLineSplit = inventoryLevelLine.toString().split(",");
		String productId = inventoryLevelLineSplit[0];
		Integer inventoryLevel = Integer.parseInt(inventoryLevelLineSplit[1]);
		context.write(new Text(productId), new LongWritable(inventoryLevel));
	}

}
