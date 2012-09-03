package com.jjathman.cassandra;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;

/**
 * @author jjathman
 * @since 09/02/2012
 */
public class FullRun {
    public static void main(String[] args) throws ConnectionException {
        AstyanaxContext<Keyspace> context = new AstyanaxContext.Builder()
            .forCluster("Test Cluster")
            .forKeyspace("DEMO")
            .withAstyanaxConfiguration(new AstyanaxConfigurationImpl()
                    .setDiscoveryType(NodeDiscoveryType.NONE)
            )
            .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                    .setPort(9160)
                    .setMaxConnsPerHost(1)
                    .setSeeds("127.0.0.1:9160")
            )
            .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
            .buildKeyspace(ThriftFamilyFactory.getInstance());

        context.start();
        Keyspace keyspace = context.getEntity();

        ColumnFamily<String, String> CF_USER_INFO = new ColumnFamily<String, String>(
            "Users",              // Column Family Name
            StringSerializer.get(),   // Key Serializer
            StringSerializer.get());  // Column Serializer

        MutationBatch m = keyspace.prepareMutationBatch();

        StopWatch sw = new StopWatch();
        sw.start();
        int totalRows = 100000;
        for (int i = 0; i < totalRows; i++) {
            m.withRow(CF_USER_INFO, String.valueOf(System.nanoTime()))
                .putColumn("firstname", "First_" + RandomStringUtils.randomAlphabetic(15), null)
                .putColumn("lastname", "Last_" + RandomStringUtils.randomAlphabetic(15), null)
                .putColumn("address", "Addr_" + RandomStringUtils.randomAlphabetic(15), null)
                .putColumn("age", 30, null);
            OperationResult<Void> result = m.execute();
            result.getLatency();
        }
        sw.stop();
        System.out.println("elapsed time in ms: " + sw.getTime());
        double elapsedTime = sw.getTime() / 1000.0;

        System.out.println("elapsed in seconds: " + elapsedTime);
        double rowsPerSecond = totalRows / elapsedTime;
        System.out.println("Writing rows took: " + sw.toString());
        System.out.println("Rows per second: " + rowsPerSecond);
//
//        ColumnFamilyQuery<String,String> query = keyspace.prepareQuery(CF_USER_INFO);
//        AllRowsQuery<String,String> allRowsQuery = query.getAllRows().setRowLimit(10);
//        OperationResult<Rows<String,String>> queryResult = allRowsQuery.execute();
//        Rows<String, String> result = queryResult.getResult();
//        for (Row<String, String> row : result) {
//            String key = row.getKey();
//            RowQuery<String,String> rowQuery = query.getKey(key);
//            OperationResult<ColumnList<String>> keyQuery = rowQuery.execute();
//            ColumnList<String> columns = keyQuery.getResult();
//            System.out.println("First Name: " + columns.getStringValue("firstname", ""));
//        }
    }
}
