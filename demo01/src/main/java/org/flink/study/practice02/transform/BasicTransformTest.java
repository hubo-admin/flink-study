package org.flink.study.practice02.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.flink.study.entity.UserClickRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * <p> 基本数据流转换算子示例 map flatmap filter</p>
 *
 * @author hubo
 * @since 2021/11/3 21:36
 * 启动参数：
 */
public class BasicTransformTest {

    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String testInputFilePath = parameterTool.get("file.path");
        testInputFilePath = testInputFilePath == null ?
                "E:\\Projects\\bigdata\\flink\\flink-study\\demo01\\src\\main\\resources\\userclickrecordtempdata.txt":
                testInputFilePath;

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream dataSource = executionEnvironment.readTextFile(testInputFilePath);

        /*
         * 1. Map 算子
         *      Takes one element and produces one element. 获取一个元素并生成一个元素。
         *      DataStream → DataStream
         *      例如：将获得的 String 转换成 POJO 实体对象，并将 label 设置成 0
         **/
        SingleOutputStreamOperator mapOutputStreamOperator = dataSource.map((MapFunction<String, UserClickRecord>) line -> {
            String[] lineIrems = line.split(",");
            UserClickRecord record = new UserClickRecord(lineIrems[0],
                    lineIrems[1],
                    lineIrems[2],
                    lineIrems[3],
                    lineIrems[4]);
            record.setLabel("0");
            return record;
        });

        mapOutputStreamOperator.print("mapOpt");


        /*
         * 1. FlatMap 算子
         *      Takes one element and produces zero, one, or more elements. 获取一个元素并生成零个或一个或多个元素。
         *      DataStream → DataStream
         *      例如：将获得的 String 转换成 POJO 实体对象，再提前里面的数据特征，把数据特征分成产品特征和用户特征输出(假设前 5 个是商品的特征)
         **/
        SingleOutputStreamOperator flatMapOutputStream = dataSource.flatMap((FlatMapFunction<String, List<Double>>) (line, out) -> {
            String[] lineIrems = line.split(",");
            UserClickRecord record = new UserClickRecord(lineIrems[0], lineIrems[1], lineIrems[2], lineIrems[3], lineIrems[4]);

            String features = record.getFeatures();
            String[] featureArr = features.split(" ");
            ArrayList<Double> goodsFeatures = new ArrayList<>();
            ArrayList<Double> userFeatures = new ArrayList<>();
            for (int i = 0; i < featureArr.length; i++) {
                if (i < 5) {
                    goodsFeatures.add(Double.parseDouble(featureArr[i]));
                } else {
                    userFeatures.add(Double.parseDouble(featureArr[i]));
                }
            }
            out.collect(goodsFeatures);
            out.collect(userFeatures);
        });

        flatMapOutputStream.print("flatOpt");


        /*
         * 1. Filter 算子
         *      Evaluates a boolean function for each element and retains those for which the function returns true.
         *      保留该函数返回true的元素，不可以改变输入的数据类型
         *      DataStream → DataStream
         *      例如：将获得的 String 解析成 POJO 实体对象，再判断 Label 的值，为 1 时输出
         **/
        SingleOutputStreamOperator filterOutputStreamOperator = dataSource.filter((FilterFunction<String>) line -> {
            String[] lineIrems = line.split(",");
            UserClickRecord record = new UserClickRecord(lineIrems[0], lineIrems[1], lineIrems[2], lineIrems[3], lineIrems[4],lineIrems[5]);
            if ("1".equals(record.getLabel())) {
                return true;
            }
            return false;
        });

        filterOutputStreamOperator.print("filterOpt");

        executionEnvironment.execute("BasicTransformTestJob");

    }

}
