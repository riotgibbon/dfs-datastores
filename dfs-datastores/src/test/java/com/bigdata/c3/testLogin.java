package com.bigdata.c3;

import com.backtype.hadoop.pail.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

import static com.backtype.support.TestUtils.*;

/**
 * Created with IntelliJ IDEA.
 * User: 36015To
 * Date: 04/02/14
 * Time: 20:15
 * To change this template use File | Settings | File Templates.
 */
public class testLogin {

    private static final String loginPath = "/tmp/logins";

    public static void main(String[] args) throws IOException {
        writeLogins();
        readLogins();
    }

    public static void writeLogins() throws IOException {
        Pail<Login> loginPail = getLoginPail(loginPath);
        Pail.TypedRecordOutputStream out = loginPail.openWrite();
        out.writeObject(new Login("alex", getUnixTime()));
        out.writeObject(new Login("bob", getUnixTime()));
        out.close();
    }

    private static long getUnixTime() {
        return System.currentTimeMillis() / 1000L;
    }

    public static void readLogins() throws IOException {
        Pail<Login> loginPail = new Pail<Login>(loginPath);
        for (Login l:loginPail){
            java.util.Date time=new java.util.Date((long)l.loginUnixTime*1000);
            System.out.println(l.userName + " " + time );
        }
    }

    public static  Pail<Login> getLoginPail(String path) throws IOException {
        FileSystem local = FileSystem.get(new Configuration());
        Pail pail;
        if (local.exists(new Path (path)))
            local.delete(new Path(path), true);
        pail = Pail.create(local, path, new LoginPailStructure() );
        return  pail;
    }
}
