package cn.nwcdcloud.jingamz;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

public class DemoInputStream {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        ObjectInputStream osi = new ObjectInputStream(new FileInputStream("jingamz.txt"));
        Object o = osi.readObject();
        System.out.println(o);
        Person o1=(Person)o;
        System.out.println(o1.getName());
    }
}
