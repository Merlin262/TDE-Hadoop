package TDE;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Exe4_TransactionsPerFlow {

    public static class FlowMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text flow = new Text();
        private boolean isHeader = true;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");

            // Verifica se a linha é o cabeçalho (primeira linha) e pula ela
            if (isHeader) {
                isHeader = false;
                return;
            }

            if (fields.length == 10) { //tratativa para dados faltantes
                flow.set(fields[4]);  // Campo que contém o fluxo
                context.write(flow, one);
            }
        }
    }

    public static class FlowReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
