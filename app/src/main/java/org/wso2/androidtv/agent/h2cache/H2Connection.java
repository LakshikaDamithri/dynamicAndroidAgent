package org.wso2.androidtv.agent.h2cache;

import android.content.ContextWrapper;
import android.util.Log;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by gathikaratnayaka on 10/4/17.
 */

public class H2Connection {
    private ContextWrapper contextWrapper;
    private Connection conn;

    public H2Connection(ContextWrapper contextWrapper){
        this.contextWrapper =contextWrapper;
    }


    public void initializeConnection() throws SQLException, ClassNotFoundException {

        File directory = contextWrapper.getFilesDir();
        System.out.println("h2 db direcotry :"+directory);
        String url = "jdbc:h2:/data/data/" +
                "agent.androidtv.wso2.org.agent2" +
                "/data/edgeTVGateway" +
                ";FILE_LOCK=FS" +
                ";PAGE_SIZE=1024" +
                ";CACHE_SIZE=8192";
        Class.forName("org.h2.Driver");
        conn = DriverManager.getConnection(url,"admin","admin");
        Statement stat = conn.createStatement();
       /* try (ResultSet rs = stat.executeQuery("select * from ACTABLE")) {
            System.out.println("ababab");
            while (rs.next()) {
                System.out.println("tableIterate :"+rs.getString("name"));
            }
        }
        Log.i("TAG","h2 database connection established");*/
    }

    public void checkIfTableExists() throws SQLException {
        boolean tableExists=false;
        System.out.println("aaaaaaaa");

        try {
            this.initializeConnection();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        Statement stat = conn.createStatement();
        try (ResultSet rs = stat.executeQuery("select * from actable")) {
            while (rs.next()) {
                System.out.println("tableIterate :"+rs.getString("AC"));
            }
        }
        System.out.println("tableExists :"+tableExists);
    }
}
