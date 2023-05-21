package org.example;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class Main {
    public static class MapperImplementacao1 extends Mapper<Object, Text, Text, IntWritable>{

        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            String linha = valor.toString();
            String[] campos = linha.split(";");
            if (campos.length == 9) {
                String ano = campos[2];
                int ocorrencia = 1;

                Text chaveMap = new Text(ano);
                IntWritable valorMap = new IntWritable(ocorrencia);

                context.write(chaveMap, valorMap);
            }
        }
    }

  public static class ReduceImplementacao1 extends Reducer<Text, IntWritable, Text, IntWritable>{
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException {
            int soma = 0;
            for (IntWritable val : valores){
                soma += val.get();
            }
            IntWritable valorSaida = new IntWritable(soma);
            context.write(chave, valorSaida);
        }
  }



    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    String arquivoEntrada = "~/implementacaoLocalMR/arquivodeEntrada/arquivo.csv";
    String arquivoSaida = "~/implementacaoLocalMR/arquivoSaida/implementacaoLocal1";

    //verificar no video em 41:06 https://www.youtube.com/watch?v=TFdSxamAJ-M
    if(args.length == 2 ){
        arquivoEntrada = args[0];
        arquivoSaida = args[1];
    }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "atividade1");

        job.setJarByClass(Main.class);
        job.setMapperClass(MapperImplementacao1.class);
        job.setReducerClass(ReduceImplementacao1.class);
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);
    }
}