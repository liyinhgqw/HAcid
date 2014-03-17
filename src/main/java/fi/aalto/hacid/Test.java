package fi.aalto.hacid;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.commons.logging.Log;

import java.io.IOException;

/**
 * Created by liyinhgqw on 16/3/14.
 */
public class Test {

    public static final Log LOG = LogFactory.getLog(Test.class);

    private static Configuration conf;

    public static void main(String[] args) throws IOException {
        conf = HBaseConfiguration.create();

        LOG.debug("hi");
//        HTable table = new HTable(conf, "test");

//        HBaseAdmin admin = new HBaseAdmin(conf);

//        System.out.println("" + admin.listTables().length);
    }
}
