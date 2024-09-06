package TDE;

import TDE.CustomWritable.TransactionAverageWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Exe7_AverageExportValuePerYear {
    public static class ExportMapper extends Mapper<LongWritable, Text, Text, TransactionAverageWritable> {
        //private Text year = new Text();
        //private DoubleWritable transactionValue = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if (fields.length == 10 && fields[0].equalsIgnoreCase("Brazil") && fields[4].equalsIgnoreCase("Export") && !fields[1].isEmpty() && !fields[5].isEmpty()) {
                {
                    //year.set(fields[1]); // Ano da transação
                    //double price = Double.parseDouble(fields[5]); // Valor da transação
                    //transactionValue.set(price);
                    context.write(new Text(fields[1]), new TransactionAverageWritable(Float.parseFloat(fields[5]), 1));
                }
            }
        }
    }

        public static class AverageReducer extends Reducer<Text, TransactionAverageWritable, Text, FloatWritable> {
            public void reduce(Text key, Iterable<TransactionAverageWritable> values, Context context) throws IOException, InterruptedException {
                float sum = 0;
                int count = 0;
                for (TransactionAverageWritable value : values) {
                    sum += value.getTotalValue();
                    count += value.getTransactionCount();
                }
                float average = sum / count;
                context.write(key, new FloatWritable(average));
            }
        }

}
