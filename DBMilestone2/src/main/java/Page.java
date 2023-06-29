package main.java;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Vector;

public class Page implements Serializable {
    String pageId;
    String pageCluster;
    Vector<String> pageClusterings;
    Vector<Hashtable<String,Object>> pageList;
    transient Page overflowPage;

    public Page (String id) {
        pageId = id;
        pageList = new Vector<Hashtable<String,Object>>();
        pageClusterings = new Vector<String>();

    }

    public int comparePages(Hashtable<String, Object> page1, Hashtable<String, Object> page2) {
        return (page1.get(pageCluster).toString().compareTo(page2.get(pageCluster).toString()));
    }
}
