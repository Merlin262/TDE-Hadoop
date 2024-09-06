package TDE;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Exe1_BrazilTransactionCount {

    public static class BrazilTransactionMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text country = new Text("Brasil");

        // Método Map
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if (fields.length == 10) {
                String countryField = fields[0];  // Campo que contém o país
                if (countryField.equalsIgnoreCase("Brazil")) {
                    context.write(country, one);
                    //System.out.println("Map: " + country.toString() + " -> " + one.toString());
                }
            }
        }
    }
    public static class BrazilTransactionReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Método Reduce
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
            //System.out.println("Reduce: " + key.toString() + " -> " + sum);
        }
    }
}

