package TDE;

import TDE.CustomWritable.TransactionAverageWritable;
//import TDE.Teste.AverageWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EXE5_AverageTransactionValuePerYearInBrazil {
    public static class AverageTransactionMapper extends Mapper<LongWritable, Text, Text, TransactionAverageWritable> {

        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            if (isHeader) {
                isHeader = false;
                return;
            }

            String target = "Brazil";
            String[] fields = value.toString().split(";");

            if (fields.length > 0 && target.equals(fields[0]) && !fields[5].isEmpty()) {  // Filtrar pelo Brasil
                float price = Float.parseFloat(fields[5]);  // Campo que contém o preço (sexta coluna)
                Text year = new Text(fields[1]);
                context.write(year, new TransactionAverageWritable(price, 1));  // Usar o ano como chave

            }
        }
    }

    public static class AverageTransactionReducer extends Reducer<Text, TransactionAverageWritable, Text, FloatWritable> {

        public void reduce(Text key, Iterable<TransactionAverageWritable> values, Context context) throws IOException, InterruptedException {
            float sumPrice = 0;
            int sumN = 0;

            for (TransactionAverageWritable obj : values) {
                sumPrice += obj.getTotalValue();
                sumN += obj.getTransactionCount();
            }

            float average = sumPrice / sumN;
            context.write(key, new FloatWritable(average));
        }
    }
}

