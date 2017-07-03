package net.pmbr.hadoop.inventorylevel;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InventoryLevelReducer extends Reducer<Text, LongWritable, Text, Text> {

	public void reduce(Text productId, Iterable<LongWritable> inventoryLevelsByWarehouse, Context context) throws IOException, InterruptedException {
		long inventoryLevel = 0;
		for (LongWritable inventoryLevelByWarehouse: inventoryLevelsByWarehouse) {
			inventoryLevel += inventoryLevelByWarehouse.get();
		}
		context.write(new Text(productId), new Text(String.valueOf(inventoryLevel)));

	}

}
