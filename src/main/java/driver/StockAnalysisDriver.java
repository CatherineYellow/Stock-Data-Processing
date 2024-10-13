package driver;

//import ds.CustomTextComparator;
import mapper.MultipleInputMapper1;
import mapper.MultipleInputMapper2;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import reducer.StockAnalysisReducer;
import java.io.*;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class StockAnalysisDriver {
    public static void main(String[] args) throws Exception {
        long t1 = System.currentTimeMillis();

        // 创建配置对象和作业对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock Analysis");

        // 设置主类
        job.setJarByClass(StockAnalysisDriver.class);

        // 设置Mapper和Reducer类
//        MultipleInputs.addInputPath(job, new Path("data/order/am_hq_order_spot.txt"), TextInputFormat.class, MultipleInputMapper1.class);
//        MultipleInputs.addInputPath(job, new Path("data/order/pm_hq_order_spot.txt"), TextInputFormat.class, MultipleInputMapper1.class);
//        MultipleInputs.addInputPath(job, new Path("data/trade/am_hq_trade_spot.txt"), TextInputFormat.class, MultipleInputMapper2.class);
//        MultipleInputs.addInputPath(job, new Path("data/trade/pm_hq_trade_spot.txt"), TextInputFormat.class, MultipleInputMapper2.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MultipleInputMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MultipleInputMapper1.class);
        MultipleInputs.addInputPath(job, new Path(args[2]), TextInputFormat.class, MultipleInputMapper2.class);
        MultipleInputs.addInputPath(job, new Path(args[3]), TextInputFormat.class, MultipleInputMapper2.class);


        job.setReducerClass(StockAnalysisReducer.class);

        // 设置Mapper输出键值对类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reducer输出键值对类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        job.setGroupingComparatorClass(CustomTextComparator.class);

        // 设置输入输出路径
//        FileOutputFormat.setOutputPath(job, new Path("Output"));
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        job.waitForCompletion(true);
//        long t2 = System.currentTimeMillis();
//        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

        try {
            // 读取文件

//            BufferedReader reader = new BufferedReader(new FileReader("Output/part-r-00000"));
            Path filePath = new Path(args[5]);
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(filePath);
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            List<String> lines = new ArrayList<>();
            String header = "TIMESTAMP,PRICE,SIZE,BUY_SELL_FLAG,ORDER_TYPE,ORDER_ID,MARKET_ORDER_TYPE,CANCEL_TYPE";

            String line;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            // 关闭文件读取器
            reader.close();


            // 根据索引为0分隔排序，若索引为0的一样则根据第5个归并排序
            lines.sort(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    String[] parts1 = o1.split(",");
                    String[] parts2 = o2.split(",");

                    // 转化索引为 0 的元素为 date 类型
//                    LocalDateTime date1 = LocalDateTime.parse(parts1[0], formatter);
//                    LocalDateTime date2 = LocalDateTime.parse(parts2[0], formatter);

                    // 比较委托时间（下标为0）
                    int compareResult = parts1[0].compareTo(parts2[0]);

                    // 如果委托时间（下标为0）相同，先比较成交类型（下标为7，先成交（2）在撤单（1）），再比较委托索引（下标为5），即
                    if (compareResult == 0) {
                        int compareResult2 = parts2[7].compareTo(parts1[7]);
                        if(compareResult2 == 0) {
                            Long value3 = Long.parseLong(parts1[5]);
                            Long value4 = Long.parseLong(parts2[5]);
                            return value3.compareTo(value4);
                        }
                        return compareResult2;
                    }
                    return compareResult;
                }
            });
            lines.add(0, header);


            // 输出到文件
            BufferedWriter writer = new BufferedWriter(new FileWriter("you.csv"));
            for (String sortedLine : lines) {
                writer.write(sortedLine);
                writer.newLine();
            }
            // 关闭文件写入器
            writer.close();
//            System.out.println("排序并输出成功！");

        } catch (IOException e) {
            e.printStackTrace();
        }
        long t3 = System.currentTimeMillis();
        System.out.printf("%d\n", t3 - t1);
    }
}
