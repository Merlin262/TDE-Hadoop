package TDE;

import TDE.CustomWritable.CountryYearWritable;
import TDE.CustomWritable.PriceAmountWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Objects;

public class Exe8_MinMaxTransactionByYearCountry {

    public static class TransactionMapper extends Mapper<LongWritable, Text, CountryYearWritable, PriceAmountWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(";");

            if (fields.length > 0 && fields[0] != null && !fields[0].isEmpty()) {
                char first = fields[0].charAt(0);
                // Verifica se o primeiro caractere é uma letra maiúscula
                if (Character.isUpperCase(first) && !Objects.equals(fields[7], "No Quantity")) {
                    String country = fields[0];  // País
                    String year = fields[1];
                    try {
                        long price = Long.parseLong(fields[5]);
                        float amount;
                        amount = Float.parseFloat(fields[8]); //amount
                        context.write(new CountryYearWritable(country, year), new PriceAmountWritable(price, amount));
                    } catch (NumberFormatException e) {
                        // Ignora se o valor não puder ser convertido
                        return;
                    }
                }





            }
        }
    }

    public static class TransactionReducer extends Reducer<CountryYearWritable, PriceAmountWritable, CountryYearWritable, Text> {
        public void reduce(CountryYearWritable key, Iterable<PriceAmountWritable> values, Context context) throws IOException, InterruptedException {
            Float minAmount = null;
            Float maxAmount = null;
            Long priceMin = null;
            Long priceMax = null;
            for (PriceAmountWritable amt : values) {
                if (minAmount == null){
                    minAmount = amt.getAmount();
                    maxAmount = amt.getAmount();
                    priceMax = amt.getPrice();
                    priceMin = amt.getPrice();
                }
                if (amt.getAmount() > maxAmount){
                    maxAmount = amt.getAmount();
                    priceMax = amt.getPrice();
                }
                else if (amt.getAmount() < minAmount){
                    minAmount = amt.getAmount();
                    priceMin = amt.getPrice();
                }
            }

            // Emite (CountryYearWritable, menor transação; maior transação)
            context.write(key,new Text( "Min price: " + priceMin + " /  " + "Min amount: " + minAmount));
            context.write(key,new Text( "Max price: " + priceMax   + " / " + "Max amount: " + maxAmount));
        }
    }
}
