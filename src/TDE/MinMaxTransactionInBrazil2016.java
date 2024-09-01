package TDE;

import org.apache.hadoop.conf.Configuration;
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

import java.io.IOException;

public class MinMaxTransactionInBrazil2016 {

    public static class Brazil2016Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text transaction = new Text();
        private DoubleWritable price = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if (fields.length == 10 && fields[0].equalsIgnoreCase("Brazil") && fields[1].equals("2016")) {
                double transactionPrice = Double.parseDouble(fields[5]); // Campo que contém o preço (sexta coluna)
                price.set(transactionPrice);
                transaction.set(value.toString());
                context.write(transaction, price);
            }
        }
    }

    public static class MinMaxReducer extends Reducer<Text, DoubleWritable, Text, Text> {
        private Text minTransaction = new Text();
        private Text maxTransaction = new Text();
        private double minPrice = Double.MAX_VALUE;
        private double maxPrice = Double.MIN_VALUE;

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            for (DoubleWritable val : values) {
                double currentPrice = val.get();
                if (currentPrice < minPrice) {
                    minPrice = currentPrice;
                    minTransaction.set(key);
                }
                if (currentPrice > maxPrice) {
                    maxPrice = currentPrice;
                    maxTransaction.set(key);
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("Min Transaction"), formatTransaction(minTransaction.toString(), minPrice));
            context.write(new Text("Max Transaction"), formatTransaction(maxTransaction.toString(), maxPrice));
        }

        private Text formatTransaction(String transaction, double price) {
            String[] fields = transaction.split(";");
            String formatted = String.format(
                    "Country: %s, Year: %s, Commodity Code: %s, Commodity: %s, Flow: %s, Price: $%.2f, Weight: %s, Unit: %s, Amount: %s, Category: %s",
                    fields[0], fields[1], fields[2], fields[3], fields[4], price, fields[6], fields[7], fields[8], fields[9]
            );
            return new Text(formatted);
        }
    }
}
