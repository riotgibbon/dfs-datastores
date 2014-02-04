package com.bigdata.c3;

import com.backtype.hadoop.pail.Pail;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;



/**
 * Created with IntelliJ IDEA.
 * User: 36015To
 * Date: 04/02/14
 * Time: 19:29
 * To change this template use File | Settings | File Templates.
 */
public class pail {



    @Test
    public void testSimpleIO() throws IOException   {

        Pail pail = GetPail("c:/tmp/mypail");
        Pail.TypedRecordOutputStream outputStream = pail.openWrite();
        outputStream.writeObject(new byte[]{1,2,3});
        outputStream.writeObject(new byte[]{1,2,3,4});
        outputStream.writeObject(new byte[]{1,2,3,4,5});
        outputStream.close();
    }



    public Pail GetPail(String path) throws IOException {
        FileSystem local = FileSystem.get(new Configuration());
        Pail pail;
        if (local.exists(new Path(path)))
            local.delete(new Path(path), true);

        pail = Pail.create(local, path);
        return  pail;
    }
}
