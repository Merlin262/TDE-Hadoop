package TDE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class TDE2 {

    public static void main(String[] args) throws Exception {
        // Configuração básica do Log4j
        BasicConfigurator.configure();

        // Configuração do Hadoop
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // Caminho de entrada e saída no HDFS
        Path input = new Path("C:/work/Hadoop/Hadoop/in/operacoes_comerciais_inteira.csv");
        Path outputBrazilTransaction = new Path("output/brasil_transaction_count");

        // Configuração do Job para contagem de transações no Brasil
        Job jobBrazil = Job.getInstance(c, "Brazil Transaction Count");

        // Registro das classes
        jobBrazil.setJarByClass(BrazilTransactionCount.class);
        jobBrazil.setMapperClass(BrazilTransactionCount.BrazilTransactionMapper.class);
        jobBrazil.setReducerClass(BrazilTransactionCount.BrazilTransactionReducer.class);

        // Definição dos tipos de saída
        jobBrazil.setMapOutputKeyClass(Text.class);
        jobBrazil.setMapOutputValueClass(IntWritable.class);
        jobBrazil.setOutputKeyClass(Text.class);
        jobBrazil.setOutputValueClass(IntWritable.class);

        // Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(jobBrazil, input);
        FileOutputFormat.setOutputPath(jobBrazil, outputBrazilTransaction);

        // Lança o job e aguarda sua execução
        if (!jobBrazil.waitForCompletion(true)) {
            System.exit(1);
        }

        // Caminho de saída para transações por ano
        Path outputTransactionsPerYear = new Path("output/transactions_per_year");

        // Configuração do Job para transações por ano
        Job jobPerYear = Job.getInstance(c, "Transactions Per Year");

        // Registro das classes
        jobPerYear.setJarByClass(TransactionPerYear.class);
        jobPerYear.setMapperClass(TransactionPerYear.TransactionPerYearMapper.class);
        jobPerYear.setReducerClass(TransactionPerYear.YearReducer.class);

        // Definição dos tipos de saída
        jobPerYear.setMapOutputKeyClass(Text.class);
        jobPerYear.setMapOutputValueClass(IntWritable.class);
        jobPerYear.setOutputKeyClass(Text.class);
        jobPerYear.setOutputValueClass(IntWritable.class);

        // Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(jobPerYear, input);
        FileOutputFormat.setOutputPath(jobPerYear, outputTransactionsPerYear);

        // Lança o job e aguarda sua execução
        if (!jobPerYear.waitForCompletion(true)) {
            System.exit(1);
        }

        // Caminho de saída para transações por categoria
        Path outputTransactionsPerCategory = new Path("output/transactions_per_category");

        // Configuração do Job para transações por categoria
        Job jobPerCategory = Job.getInstance(c, "Transactions Per Category");

        // Registro das classes
        jobPerCategory.setJarByClass(TransactionsPerCategory.class);
        jobPerCategory.setMapperClass(TransactionsPerCategory.CategoryMapper.class);
        jobPerCategory.setReducerClass(TransactionsPerCategory.CategoryReducer.class);

        // Definição dos tipos de saída
        jobPerCategory.setMapOutputKeyClass(Text.class);
        jobPerCategory.setMapOutputValueClass(IntWritable.class);
        jobPerCategory.setOutputKeyClass(Text.class);
        jobPerCategory.setOutputValueClass(IntWritable.class);

        // Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(jobPerCategory, input);
        FileOutputFormat.setOutputPath(jobPerCategory, outputTransactionsPerCategory);

        // Lança o job e aguarda sua execução
        if (!jobPerCategory.waitForCompletion(true)) {
            System.exit(1);
        }

        // Caminho de saída para transações por fluxo
        Path outputTransactionsPerFlow = new Path("output/transactions_per_flow");

        // Configuração do Job para transações por fluxo
        Job jobPerFlow = Job.getInstance(c, "Transactions Per Flow");

        // Registro das classes
        jobPerFlow.setJarByClass(TransactionsPerFlow.class);
        jobPerFlow.setMapperClass(TransactionsPerFlow.FlowMapper.class);
        jobPerFlow.setReducerClass(TransactionsPerFlow.FlowReducer.class);

        // Definição dos tipos de saída
        jobPerFlow.setMapOutputKeyClass(Text.class);
        jobPerFlow.setMapOutputValueClass(IntWritable.class);
        jobPerFlow.setOutputKeyClass(Text.class);
        jobPerFlow.setOutputValueClass(IntWritable.class);

        // Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(jobPerFlow, input);
        FileOutputFormat.setOutputPath(jobPerFlow, outputTransactionsPerFlow);

        // Lança o job e aguarda sua execução
        if (!jobPerFlow.waitForCompletion(true)) {
            System.exit(1);
        }

        // Caminho de saída para valor médio das transações por ano no Brasil
        Path outputAverageTransactionValuePerYearInBrazil = new Path("output/average_transaction_value_per_year_in_brazil");

        // Configuração do Job para valor médio das transações por ano no Brasil
        Job jobAverageValue = Job.getInstance(c, "Average Transaction Value Per Year In Brazil");

        // Registro das classes
        jobAverageValue.setJarByClass(AverageTransactionValuePerYearInBrazil.class);
        jobAverageValue.setMapperClass(AverageTransactionValuePerYearInBrazil.BrazilMapper.class);
        jobAverageValue.setReducerClass(AverageTransactionValuePerYearInBrazil.BrazilReducer.class);

        // Definição dos tipos de saída
        jobAverageValue.setMapOutputKeyClass(Text.class);
        jobAverageValue.setMapOutputValueClass(DoubleWritable.class);
        jobAverageValue.setOutputKeyClass(Text.class);
        jobAverageValue.setOutputValueClass(DoubleWritable.class);

        // Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(jobAverageValue, input);
        FileOutputFormat.setOutputPath(jobAverageValue, outputAverageTransactionValuePerYearInBrazil);

        // Lança o job e aguarda sua execução
        if (!jobAverageValue.waitForCompletion(true)) {
            System.exit(1);
        }

        // Caminho de saída para transação mínima e máxima no Brasil em 2016
        Path outputMinMaxTransactionInBrazil2016 = new Path("output/min_max_transaction_in_brazil_2016");

        // Configuração do Job para transação mínima e máxima no Brasil em 2016
        Job jobMinMax = Job.getInstance(c, "Min Max Transaction In Brazil 2016");

        // Registro das classes
        jobMinMax.setJarByClass(MinMaxTransactionInBrazil2016.class);
        jobMinMax.setMapperClass(MinMaxTransactionInBrazil2016.Brazil2016Mapper.class);
        jobMinMax.setReducerClass(MinMaxTransactionInBrazil2016.MinMaxReducer.class);

        // Definição dos tipos de saída
        jobMinMax.setMapOutputKeyClass(Text.class);
        jobMinMax.setMapOutputValueClass(DoubleWritable.class);
        jobMinMax.setOutputKeyClass(Text.class);
        jobMinMax.setOutputValueClass(Text.class);

        // Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(jobMinMax, input);
        FileOutputFormat.setOutputPath(jobMinMax, outputMinMaxTransactionInBrazil2016);

        // Lança o job e aguarda sua execução
        if (!jobMinMax.waitForCompletion(true)) {
            System.exit(1);
        }

        // Caminho de saída para valor médio das exportações por ano no Brasil
        Path outputAverageExportValuePerYear = new Path("output/average_export_value_per_year");

        // Configuração do Job para valor médio das exportações por ano no Brasil
        Job jobAverageExportValue = Job.getInstance(c, "Average Export Value Per Year");

        // Registro das classes
        jobAverageExportValue.setJarByClass(AverageExportValuePerYear.class);
        jobAverageExportValue.setMapperClass(AverageExportValuePerYear.ExportMapper.class);
        jobAverageExportValue.setReducerClass(AverageExportValuePerYear.AverageReducer.class);

        // Definição dos tipos de saída
        jobAverageExportValue.setMapOutputKeyClass(Text.class);
        jobAverageExportValue.setMapOutputValueClass(DoubleWritable.class);
        jobAverageExportValue.setOutputKeyClass(Text.class);
        jobAverageExportValue.setOutputValueClass(DoubleWritable.class);

        // Cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(jobAverageExportValue, input);
        FileOutputFormat.setOutputPath(jobAverageExportValue, outputAverageExportValuePerYear);

        // Lança o job e aguarda sua execução
        System.exit(jobAverageExportValue.waitForCompletion(true) ? 0 : 1);
    }
}
