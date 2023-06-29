package main.java;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
public interface DBAppInterface {
    void init();

    void createTable(String strTableName,
                     String strClusteringKeyColumn,
                     Hashtable<String,String> htblColNameType,
                     Hashtable<String,String> htblColNameMin,
                     Hashtable<String,String> htblColNameMax ) throws DBAppException, IOException;

    void createIndex(String strTableName,
                            String[] strarrColName) throws DBAppException;

    void insertIntoTable(String strTableName,
                         Hashtable<String,Object> htblColNameValue) throws DBAppException;

    void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String,Object> htblColNameValue ) throws DBAppException;

    void deleteFromTable(String strTableName,
                         Hashtable<String,Object> htblColNameValue) throws DBAppException;

    Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                             String[] strarrOperators) throws DBAppException;


}
