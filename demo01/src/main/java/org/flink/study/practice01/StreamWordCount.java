package org.flink.study.practice01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 流处理 word count
 * 有界流模拟：读取文件流  （--isNetcat false）
 * 无界流模拟：利用远程Linux主机的 netcat 工具开启一个 socket 端口，实时的输入数据  命令： nc -lk 9999
 * 故障恢复不漏算演示：关闭程序，在 netcat 开启的 socket 中继续追加输入，然后启动程序，发现程序会从之前的位置继续处理数据
 *
 * param 配置启动参数
 *    --isNetcat true
 *    --filepath E:\Projects\bigdata\flink\flink-study\demo01\src\main\resources\textfile.txt
 *    --host 192.168.116.100
 *    --port 9999
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 获取启动参数
        ParameterTool parameters = ParameterTool.fromArgs(args);
        boolean isNetcat = parameters.getBoolean("isNetcat");

        // 获取流处理环境
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        streamEnv.setParallelism(3);

        DataStreamSource<String> dataSource;
        if (!isNetcat ){
            System.out.println("测试 1：从文件中读取文件流，其实还是一个有界流");
            String filePath = parameters.get("filepath");
            dataSource = streamEnv.readTextFile(filePath);
        } else {
            System.out.println("测试 2：从中 socket 中读取流，无界流，实时处理");
            String host = parameters.get("host");
            int port = parameters.getInt("port");
            System.out.println(host+":"+port);
            dataSource = streamEnv.socketTextStream(host,port);
        }

        // 处理流数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> resultStream = dataSource.flatMap(new LineToWordOne())
                .keyBy(0)
                .sum(1);

        resultStream.print();

        //执行任务
        streamEnv.execute();
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
