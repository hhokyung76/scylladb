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
//        for (int ii = 1 ; ii < 111; ii++) {
//            String id = "goodId---"+ii;
//            session.execute("insert into test3.kb_dev_audience_ads_group (adsgroupno, id) values (90, 'goodid---"+ii+"'), (91, 'good888--"+ii+"')");
//        }

//        PreparedStatement preparedStatement = session.prepare("insert into test3.kb_dev_audience_ads_group (adsgroupno, id) values (?, ?)");
//        BoundStatement boundStatement = new BoundStatementBuilder(preparedStatement).build();
//        BatchStatement batchableStatement = new BatchStatementBuilder
        String endTime = ScStringUtils.getCurrentTimeOfLog();

        System.out.println("## startTime: "+startTime);
        System.out.println("## endTime: "+endTime);

        session.close();

    }
}
