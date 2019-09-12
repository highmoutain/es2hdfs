import com.typesafe.config.Config;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import spark.jobserver.api.JobEnvironment;
import spark.jobserver.japi.JSessionJob;

import java.util.List;

/**
 * Created by 长春 on 2018/8/14.
 */



        import java.util.List;
        import com.typesafe.config.Config;
        import org.apache.spark.sql.Row;
        import org.apache.spark.sql.SparkSession;
        import spark.jobserver.api.JobEnvironment;
        import spark.jobserver.japi.JSessionJob;

public class testspark2js extends JSessionJob<Row[]> {

    @Override
    public Row[] run(SparkSession spark, JobEnvironment runtime, Config data) {
        List<Row> rowList = spark.sql(data.getString("sql")).collectAsList();
        Row[] rows = new Row[rowList.size()];
        return rowList.toArray(rows);
    }

    @Override
    public Config verify(SparkSession spark, JobEnvironment runtime, Config config) {
        return config;
    }
}
