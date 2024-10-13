package mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.StringTokenizer;

public class MultipleInputMapper2 extends Mapper<LongWritable, Text, Text, Text> {
    Text k = new Text();
    Text v = new Text();
    SimpleDateFormat inputFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS");
    SimpleDateFormat outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000");
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String input = value.toString();
        String[] split = input.split("\t");
        long tradeTime = Long.parseLong(split[15]);
        // 判断为平安银行且时间为连续竞价
        if(split[8].equals("000001") &&
//                (tradeTime >= 20190102093000000L && tradeTime <= 20190102093030000L)
                ((tradeTime >= 20190102093000000L && tradeTime < 20190102113000000L) ||
                 (tradeTime >= 20190102130000000L && tradeTime < 20190102145700000L))
        ){
            // 转化价格
            float price = Float.parseFloat(split[12]);
            Date date = null;
            try {
                // 转化时间
                date = inputFormat.parse(split[15]);
                String time = outputFormat.format(date);
                // 写入k, v
                if(!split[10].equals("0")) {
                    // 将k设置成, BidAppSeqNum, 表示买方委托索引
                    k.set(split[10]);
                    // 将v设置成T，TIMESTAMP，PRICE，BidApplSeqNum,OfferAppISeqNum,ExecType
                    // 表示Trade表，成交时间，成交价格，是否撤单，买方，数量
                    StringBuilder vBuilder = new StringBuilder("T,");
                    vBuilder.append(time).append(",").append(price).append(",").append(split[14]).append(",").append("1").append(",").append(split[13]);
                    v.set(vBuilder.toString());
//                    v.set("T," + time + "," + price +  "," + split[14] +","+ "1" + "," + split[13]);
                    context.write(k, v);
                }
                if(!split[11].equals("0")) {
                    // 将k设置成OfferAppSeqNum, 表示卖方委托索引
                    k.set(split[11]);
                    // 将v设置成T，TIMESTAMP，PRICE，BidApplSeqNum,OfferAppISeqNum,ExecType
                    // 表示Trade表，成交时间，成交价格，是否撤单，卖方, 数量
                    StringBuilder vBuilder = new StringBuilder("T,");
                    vBuilder.append(time).append(",").append(price).append(",").append(split[14]).append(",").append("2").append(",").append(split[13]);
                    v.set(vBuilder.toString());
//                    v.set("T," + time + "," + price +  "," + split[14] + ","+ "2" +","+ split[13]);
                    context.write(k, v);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }
}
