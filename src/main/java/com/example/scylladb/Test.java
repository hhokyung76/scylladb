package com.example.scylladb;

//import com.datastax.driver.core.Cluster;
//import com.datastax.driver.core.ResultSet;
//import com.datastax.driver.core.Row;
//import com.datastax.driver.core.Session;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.insert.InsertInto;
import com.datastax.oss.driver.api.querybuilder.select.Select;

import java.net.InetSocketAddress;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.*;


public class Test {
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

        InsertInto insert = insertInto("kb_dev_audience_ads_group");
        insert.json("{\"id\":\"good\", \"adsGroupNo\": 455}").defaultNull();
           // insert.
        for (int ii = 1000 ; ii < 100000; ii++) {
            String id = "goodId---"+ii;
            session.execute("insert into test3.kb_dev_audience_ads_group (adsgroupno, id) values (90, 'goodid---"+ii+"')");
        }
        session.close();

    }
}
