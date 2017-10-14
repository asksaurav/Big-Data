import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class PlusminusStock extends Mapper<LongWritable, Text, Text, DoubleWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String inputLine = value.toString();

		if (inputLine.startsWith("exchange,")) {
			// Line is the header, ignore it
			return;
		}

		String[] columns = inputLine.split(",");

		if (columns.length != 9) {
			// Line isn't the correct number of columns or formatted properly
			return;
		}


		double close = Double.parseDouble(columns[8]);
		context.write(new Text(columns[1]), new DoubleWritable(close));
	}
}
