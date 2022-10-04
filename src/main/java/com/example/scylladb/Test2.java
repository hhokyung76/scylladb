package com.example.scylladb;

//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.Row;
//import com.datastax.driver.core.Session;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.*;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.net.InetSocketAddress;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.selectFrom;


public class Test2 {
    public static void main(String[] args) {
        //Cluster cluster = Cluster.builder().addContactPoints("scylla-node1", "scylla-node2", "scylla-node3").build();
        CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress("192.168.30.129", 9042))
                .withLocalDatacenter("datacenter1")
                .build();

        Select query = selectFrom("test3", "kb_dev_audience_ads_group").column("advertisingId"); // SELECT release_version FROM system.local
        SimpleStatement statement = query.build();

        ResultSet rs = session.execute(statement);
        Row row = rs.one();
        System.out.println(row.getString("advertisingId"));

        String startTime = ScStringUtils.getCurrentTimeOfLog();
//        for (int ii = 114000 ; ii < 144000; ii++) {
//            String id = "goodId---"+ii;
//            session.execute("insert into test3.kb_dev_audience_ads_group (adsgroupno, id) values (90, 'goodid---"+ii+"')");
//        }
        BatchStatement batch =
                BatchStatement.newInstance(
                        DefaultBatchType.LOGGED
                );
        System.out.println("start batch");
        for (int ii = 0; ii < 100; ii++) {
            SimpleStatement simple = SimpleStatement.newInstance("INSERT INTO test3.kb_dev_audience_ads_group (adsgroupno, id) VALUES (400, 'val33331" + ii + "')");
//        SimpleStatement simple2 = SimpleStatement.newInstance("INSERT INTO test3.kb_dev_audience_ads_group (adsgroupno, id) VALUES (400, 'va3333l1')");
            //RegularStatement built = insertInto(TABLE1).value("k", "batchTest2").value("t", "val2");
            batch.add(simple);
            System.out.println("good ing..."+ii);
        }

        session.execute(batch);

        String endTime = ScStringUtils.getCurrentTimeOfLog();

        System.out.println("## startTime: "+startTime);
        System.out.println("## endTime: "+endTime);

        session.close();

    }
}
