import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MarksMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable marks = new IntWritable();
    private Text studentName = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        if (itr.countTokens() >= 2) {
            String name = itr.nextToken();
            int mark = Integer.parseInt(itr.nextToken());
            studentName.set(name);
            marks.set(mark);
            context.write(studentName, marks);
        }
    }
}
