package TDE;

import TDE.CustomWritable.CountryYearWritable;
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

public class MinMaxTransactionByYearCountry {

    public static class TransactionMapper extends Mapper<LongWritable, Text, CountryYearWritable, DoubleWritable> {
        private CountryYearWritable countryYear = new CountryYearWritable();
        private DoubleWritable price = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if (fields.length == 10) {
                String country = fields[0];  // País
                String year = fields[1];     // Ano
                double amount = 0;

                try {
                    amount = Double.parseDouble(fields[5]); // Valor da transação (Price)
                } catch (NumberFormatException e) {
                    // Ignora se o valor não puder ser convertido
                    return;
                }

                // Define a chave como CountryYearWritable
                countryYear.setCountry(country);
                countryYear.setYear(year);
                price.set(amount);

                // Emite (CountryYearWritable, valor da transação)
                context.write(countryYear, price);
            }
        }
    }

    public static class TransactionReducer extends Reducer<CountryYearWritable, DoubleWritable, CountryYearWritable, Text> {
        public void reduce(CountryYearWritable key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double minTransaction = Double.MAX_VALUE;
            double maxTransaction = Double.MIN_VALUE;

            for (DoubleWritable val : values) {
                double transactionValue = val.get();
                if (transactionValue < minTransaction) {
                    minTransaction = transactionValue;
                }
                if (transactionValue > maxTransaction) {
                    maxTransaction = transactionValue;
                }
            }

            // Emite (CountryYearWritable, menor transação; maior transação)
            context.write(key, new Text("Min: " + minTransaction + ", Max: " + maxTransaction));
        }
    }
}
