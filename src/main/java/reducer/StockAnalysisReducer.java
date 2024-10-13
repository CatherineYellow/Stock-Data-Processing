package reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;


public class StockAnalysisReducer extends Reducer<Text, Text, Text, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        // 存储Order_ID
        String ORDER_ID= key.toString();

        // 存储order和trade表
        Text order = new Text();
        ArrayList<String> trade = new ArrayList<>();

        // 判断是否有委托表和撤单
        boolean haveOrder = false;
//        boolean have_cancel = false;

        // 根据values存储order和trade表
        for (Text value : values) {
            String[] split = value.toString().split(",");
            // 判断在order表
            if(!haveOrder && split[0].equals("O")){
                order.set(value.toString());
                haveOrder = true;
            }else{
                // 判断在trade表
                // 判断撤单
                if(split[3].equals("4")){
                    // 直接写入  撤单时间，撤单价格(删去)，数量，买卖方向，类型(4)，委托索引，市价单类型(删去)，撤单类型
                    // 有撤单
//                    have_cancel = true;
                    context.write(new Text(split[1]+ ",," + split[5] +"," +
                                split[4] + ",4," + ORDER_ID + ",," + "1"),NullWritable.get());
                }else{
                    // 成交
                    trade.add(value.toString());
                }
            }
        }
        // 没有order表
        if(order.toString().equals("")){
            return;
        }
        // 有order表
        String[] split_order = order.toString().split(",");

        // 处理限价，市价，本方最优，成交单
        if(split_order[5].equals("2")){
            // 限价单  委托时间，价格，数量，买卖方向，类型，委托索引，市价单类型，撤单类型
            context.write(new Text(
                split_order[1] + "," + split_order[2] + "," + split_order[3] + "," + split_order[4] + "," +split_order[5] + "," + ORDER_ID  + ",,2"
            ),NullWritable.get());
        }else if(split_order[5].equals("1")) {
            // 市价单
            // 用于存储价格表
            HashSet<Float> hashSet = new HashSet<>();
            // 存储价格
            for (String text : trade) {
                String[] split_trade = text.split(",");
                hashSet.add(Float.parseFloat(split_trade[2]));
            }
            // 市价单  委托时间，价格，数量，买卖方向，类型，委托索引，市价单类型，撤单类型
            if(hashSet.size()==0){
//                if(have_cancel){
                    context.write( new Text(
                            split_order[1] + ",," + split_order[3] + "," + split_order[4] + "," + split_order[5] + "," + ORDER_ID + ",0,2"
                    ),NullWritable.get());
//                }
            }else {
                context.write(new Text(
                        split_order[1] + ",," + split_order[3] + "," + split_order[4] + "," + split_order[5] + "," + ORDER_ID + "," + hashSet.size() + ",2"
                ),NullWritable.get());
            }
        }else{
            // 本方最优 委托时间，价格，数量，买卖方向，类型，委托索引，市价单类型，撤单类型
            context.write( new Text(
                    split_order[1] + ",,"  + split_order[3] + "," + split_order[4] + "," +split_order[5] + "," + ORDER_ID  + ",,2"
            ),NullWritable.get());
        }
    }
}


