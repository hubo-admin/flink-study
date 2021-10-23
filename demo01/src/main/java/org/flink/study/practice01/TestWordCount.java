package org.flink.study.practice01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/***
 * flink 批处理测试  从文件中读取单词，计数
 */
public class TestWordCount {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 读取文件
        String inputPath = "E:\\Projects\\bigdata\\flink\\flink-study\\demo01\\src\\main\\resources\\textfile.txt";
        DataSource<String> dataSource = env.readTextFile(inputPath);
        // 处理数据
        DataSet<Tuple2<String, Integer>> wordCount = dataSource
                .flatMap(new LineToWordOne())
                .groupBy(0)
                .sum(1);
        // output
        wordCount.print();
    }

    // 处理数据：每一条记录用空格隔开，每个单词组成一个二元组（word , 1）
    public static class LineToWordOne implements FlatMapFunction<String, Tuple2<String,Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String,Integer>> out) throws Exception {
            String[] split = line.split(" ");
            for (String wold: split){
                out.collect(new Tuple2<String,Integer>(wold,1));
            }
        }
    }
}
