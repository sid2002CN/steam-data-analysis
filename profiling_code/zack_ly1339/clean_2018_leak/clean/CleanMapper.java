import java.io.IOException;
import java.util.List;
import java.io.StringReader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.csv.*;

public class CleanMapper extends Mapper<Object, Text, Text, Text> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringReader r = new StringReader(value.toString());
        try (CSVParser csvParser = CSVFormat.RFC4180.parse(r)) {
            List<CSVRecord> recordList = csvParser.getRecords();
            CSVRecord record = recordList.get(0);
            // Filter out rows with more than three records
            if (record.size() == 3) {
                String title = record.get(0).toString(); // Name of Game
                String count = record.get(1).toString(); // Player Count
                String appid = record.get(2).toString(); // Game Id
                count = count.replace(",", ""); // Remove comma in player count
                // title needed to be double quoted
                context.write(new Text('"'+title+'"' +","+ count +","+ appid), new Text(""));
            }
        }

    }

}