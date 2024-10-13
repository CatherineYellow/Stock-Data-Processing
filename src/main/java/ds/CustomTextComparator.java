//package ds;
//
////import org.apache.hadoop.io.WritableComparable;
////import org.apache.hadoop.io.WritableComparator;
////
////public class CustomTextComparator extends WritableComparator {
////    protected CustomTextComparator() {
////        super(CustomTextWritable.class, true);
////    }
////
////    @Override
////    public int compare(WritableComparable a, WritableComparable b) {
////        // Implement your custom comparison logic here
////        return ((CustomTextWritable) a).compareTo((CustomTextWritable) b);
////    }
////}
//
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.WritableComparable;
//import org.apache.hadoop.io.WritableComparator;
//
//public class CustomTextComparator extends WritableComparator {
//    protected CustomTextComparator() {
//        super(Text.class, true);
//    }
//
//    @Override
//    public int compare(WritableComparable a, WritableComparable b) {
//        // 在这里定义分组逻辑，按照委托时间（下标为0）分组
//        String[] parts1 = a.toString().split(",");
//        String[] parts2 = b.toString().split(",");
//        return parts1[0].compareTo(parts2[0]);
//    }
//}
