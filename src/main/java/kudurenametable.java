import org.apache.kudu.client.AlterTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;

/**
 * Created by 长春 on 2019/7/30.
 */
public class kudurenametable {
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
