//package ds;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//
//public class CustomTextWritable implements WritableComparable<CustomTextWritable> {
//    private Text text;
//
//    public CustomTextWritable() {
//        this.text = new Text();
//    }
//
//    public CustomTextWritable(Text text) {
//        this.text = text;
//    }
//
//    public int compare(String o1, String o2) {
//        try {
//            String[] parts1 = o1.split(",");
//            String[] parts2 = o2.split(",");
//
//            // 转化索引为 0 的元素为 date 类型
////                    LocalDateTime date1 = LocalDateTime.parse(parts1[0], formatter);
////                    LocalDateTime date2 = LocalDateTime.parse(parts2[0], formatter);
//
//            // 比较委托时间（下标为0）
//            int compareResult = parts1[0].compareTo(parts2[0]);
//
//            // 如果委托时间（下标为0）相同，先比较成交类型（下标为7，先成交（2）在撤单（1）），再比较委托索引（下标为5），即
//            if (compareResult == 0) {
//                int compareResult2 = parts2[7].compareTo(parts1[7]);
//                if (compareResult2 == 0) {
//                    Long value3 = Long.parseLong(parts1[5]);
//                    Long value4 = Long.parseLong(parts2[5]);
//                    return value3.compareTo(value4);
//                }
//                return compareResult2;
//            }
//            return compareResult;
//        } catch (Exception e){
//            return 0;
//        }
//    }
//    // Getters and setters for text
//
//    @Override
//    public void write(DataOutput out) throws IOException {
//        text.write(out);
//    }
//
//    @Override
//    public void readFields(DataInput in) throws IOException {
//        text.readFields(in);
//    }
//
//    @Override
//    public int compareTo(CustomTextWritable o) {
//        // Implement your custom comparison logic here
//        return compare(text.toString(), o.text.toString());
//    }
//}
