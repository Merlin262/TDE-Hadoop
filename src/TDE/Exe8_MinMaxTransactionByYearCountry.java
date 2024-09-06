package TDE;

import TDE.CustomWritable.CountryYearWritable;
import TDE.CustomWritable.PriceAmountWritable;
import TDE.CustomWritable.TransactionAverageWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Exe8_MinMaxTransactionByYearCountry {

    public static class TransactionMapper extends Mapper<LongWritable, Text, CountryYearWritable, PriceAmountWritable> {
        private CountryYearWritable countryYear = new CountryYearWritable();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");
            if (fields.length == 10) {
                String country = fields[0];  // País
                String year = fields[1];     // Ano
                Long price = Long.parseLong(fields[5]);
                float amount;

                try {
                    amount = Float.parseFloat(fields[8]); //amount
                } catch (NumberFormatException e) {
                    // Ignora se o valor não puder ser convertido
                    return;
                }

                context.write(new CountryYearWritable(country,year), new PriceAmountWritable(price,amount));
            }
        }
    }

    public static class TransactionReducer extends Reducer<CountryYearWritable, PriceAmountWritable, CountryYearWritable, Text> {
        public void reduce(CountryYearWritable key, Iterable<PriceAmountWritable> values, Context context) throws IOException, InterruptedException {
            float minAmount = Float.MAX_VALUE;
            float maxAmount = Float.MIN_VALUE;
            Long priceMin = 0L;
            Long priceMax = 0L;
            for (PriceAmountWritable amt : values) {
                float amount = amt.getAmount();
                if (amount < minAmount) {
                    minAmount = amount;
                    priceMin = amt.getPrice();
                }
                if (amount > maxAmount) {
                    maxAmount = amount;
                    priceMax = amt.getPrice();
                }
            }

            // Emite (CountryYearWritable, menor transação; maior transação)
            context.write(key,new Text( "Minimal price" + priceMin + "Minimal amount: " + minAmount));
        }
    }
}
