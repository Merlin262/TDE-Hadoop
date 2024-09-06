package TDE;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Exe2_TransactionPerYear {

    public static class TransactionPerYearMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text year = new Text();
        private boolean isHeader = true;  // Variável para verificar se é o cabeçalho

        // Método Map
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");

            // Verifica se a linha é o cabeçalho (primeira linha) e pula ela
            if (isHeader) {
                isHeader = false;
                return;
            }

            if (fields.length == 10) {
                year.set(fields[1]);  // Campo que contém o ano (segunda coluna)
                context.write(year, one);
            }
        }
    }


    public static class YearReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

}
