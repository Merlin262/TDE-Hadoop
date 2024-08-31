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


public class AverageTransactionValuePerYearInBrazil {

    public static class BrazilMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text year = new Text();
        private DoubleWritable transactionValue = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if (fields.length == 10 && fields[0].equalsIgnoreCase("Brazil")) {  // Filtrar pelo Brasil
                year.set(fields[1]);  // Campo que contém o ano (segunda coluna)
                double price = Double.parseDouble(fields[5]);  // Campo que contém o preço (sexta coluna)
                transactionValue.set(price);
                context.write(year, transactionValue);
            }
        }
    }

    public static class BrazilReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double average = sum / count;
            context.write(key, new DoubleWritable(average));
        }
    }
}
