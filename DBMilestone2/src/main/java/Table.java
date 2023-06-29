package main.java;
import java.io.Serializable;
import java.util.Vector;

public class Table implements Serializable {
    String tableName;
    Vector<String> columnName;
    String clusteringKey;
    Vector<String> pagesLocation;
    Vector<String> min ;
    Vector<String> max;

    public Table (String name, String clustering){
        pagesLocation = new Vector<String>();
        tableName = name;
        columnName = new Vector<String>();
        clusteringKey = clustering;
        min = new Vector<String>();
        max= new Vector<String>();
    }
}