package cn.nwcdcloud.jingamz;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Date;

public class DemoObjectStream {
    public static void main(String[] args) throws IOException {

        FileOutputStream fp = new FileOutputStream("jingamz.txt");
        ObjectOutputStream oos = new ObjectOutputStream(fp);
        oos.writeObject(new Person("Jingamz",44));
        oos.writeObject("Today");
        oos.writeObject(new Date());
        oos.close();

    }
}
