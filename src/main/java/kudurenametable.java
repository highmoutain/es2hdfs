import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

/**
 * Created by 长春 on 2019/7/30.
 */
public class kudurenametable {
    //spark-submit --class kudurenametable  /home/hadoop/es2hdfs.jar $kuduIp $tableName $newTableName
    //example:spark-submit --class kudurenametable  /home/hadoop/es2hdfs.jar
    // 172.23.3.1:7051,172.23.3.2:7051,172.23.3.3:7051 ae_profile_carbon_one  ae_profile_carbon_one_new
    private static KuduClient client = null;
    public static void renameTable (String oldName,String newName) {
        try {
            if (client.tableExists(oldName)) {
                client.alterTable(oldName, new AlterTableOptions().renameTable(newName));
                System.out.println(oldName + " renamed");
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
        String kuduIp = args[0];
        String tableName = args[1];
        String newTableName = args[2];
        client = new KuduClient.KuduClientBuilder(kuduIp).build();

        renameTable(tableName,newTableName);


    }

}
