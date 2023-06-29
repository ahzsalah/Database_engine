package main.java;
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.lang.Integer;
import java.lang.String;
import java.util.Set;
import java.util.Vector;
import java.io.BufferedReader;
import java.util.Properties;

import static java.lang.System.*;

public class DBApp implements DBAppInterface {

    FileWriter csvFile;

    //a vector for actual tables
    Vector<String> tables;

    //for faster searching for tables
    Vector<String> tableNames;
    Vector<String> columnNames;

    public DBApp() throws IOException {
        String fileTitle = "Table Name, Column Name, Column Type, ClusteringKey, IndexName, IndexType, min, max";
        csvFile = new FileWriter("src/main/resources/metadata.csv", true);
        csvFile.write(fileTitle);
        csvFile.flush();
        csvFile.close();

        tableNames = new Vector<>();
        tables = new Vector<>();
    }

    public void init() {
//        String fileTitle = "Table Name, Column Name, Column Type, ClusteringKey, IndexName, IndexType, min, max";
//        try {
////            csvFile = new FileWriter("src/main/resources/metadata.csv", true);
//            csvFile.write(fileTitle);
//            csvFile.flush();
//            csvFile.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
    // this does whatever initialization you would like // or leave it empty if there is no code you want to
    // execute at application startup


    // following method creates one table only
    // strClusteringKeyColumn is the name of the column that will be the primary // key and the clustering column as well. The data type of that column will
    // be passed in htblColNameType
    // htblColNameValue will have the column name as key and the data
    // type as value
    // htblColNameMin and htblColNameMax for passing minimum and maximum values // for data in the column. Key is the name of the column
    public void createTable(String strTableName,
                            String strClusteringKeyColumn,
                            Hashtable<String, String> htblColNameType,
                            Hashtable<String, String> htblColNameMin,
                            Hashtable<String, String> htblColNameMax)  {

        try {

            Set<String> columns = htblColNameType.keySet();
            columnNames = new Vector<>();

            for (String column : columns) {
                if (!columnNames.contains(column))
                    columnNames.add(column);
//                System.out.println(columnNames);
            }

//                    throw new DBAppException(column + " column exists" + "\n");

            tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
            tables = deserializeVector("src/main/resources/data/" + strTableName +".bin");

            if (tableNames.contains(strTableName))
                throw new DBAppException(strTableName + " table already exists" + "\n");

            csvFile = new FileWriter("src/main/resources/metadata.csv", true);
            csvFile.write(getTableAttributes(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax));
            csvFile.flush();
            csvFile.close();

            Table newTable = new Table(strTableName, strClusteringKeyColumn);

            tables.add("src/main/resources/data/" + strTableName + ".bin");
            tableNames.add(strTableName);
            serializeTable(newTable);
            serializeVector(tableNames);


        } catch (Exception e) {
            out.print(e.getMessage());
        }

    }

    // following method creates an octree
    // depending on the count of column names passed.
    // If three column names are passed, create an octree.
    // If only one or two column names is passed, throw an Exception.
    public void createIndex(String strTableName,
                            String[] strarrColName)  {

    }


    // following method inserts one row only.
    // htblColNameValue must include a value for the primary key
    public void insertIntoTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {


        boolean columnExists = checkColumnAvailability(strTableName, htblColNameValue);
        if (!columnExists) throw new DBAppException(htblColNameValue.keySet() + " is not a column in table " + strTableName);

        checkDataTypes(strTableName, htblColNameValue);

        int maxRows = 0;
        try {
            maxRows = readConfig("MaximumRowsCountinTablePage");
        } catch (IOException e) {
            e.printStackTrace();
        }
        //finding table
        tables = deserializeVector("src/main/resources/data/tables.bin");
        tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
        Table inputTable = deserializeTable("src/main/resources/data/" + strTableName + ".bin"); //.---------------------

        for (int i = 0; i < tables.size(); i++) {
            if (tableNames.get(i).equals(strTableName)) {
                inputTable = deserializeTable(tables.get(i));
                break;
            }
        }
        try {
            if (inputTable == null) throw new DBAppException("Table doesn't exist");
        } catch (DBAppException e) {
            out.println(e.getMessage());
        }
        try {
            if (inputTable != null && (htblColNameValue.get(inputTable.clusteringKey)) == null) //null to ""
                throw new DBAppException("can not insert without a primary key");
        } catch (DBAppException e) {
            out.println(e.getMessage());
        }

        //base case: no pages
        if (inputTable != null && inputTable.pagesLocation.size() == 0 && htblColNameValue.get(inputTable.clusteringKey) != null) {
            //create a page
            Page newPage = new Page(strTableName + "0");

            checkIfEntryExists(newPage, htblColNameValue, inputTable);

            newPage.pageClusterings.add((inputTable.clusteringKey));
            newPage.pageList.add(htblColNameValue);


//            insertIntoPage(newPage, htblColNameValue, htblColNameValue.get(inputTable.clusteringKey).toString() , inputTable); //recently added


            inputTable.pagesLocation.add("src/main/resources/data/" + strTableName + "0" + ".bin");
            inputTable.min.add(htblColNameValue.get(inputTable.clusteringKey).toString());
            inputTable.max.add(htblColNameValue.get(inputTable.clusteringKey).toString());

            serializePage(newPage);
            serializeTable(inputTable);
            return;

        } else {
            //find page to insert
            String inputPageClusteringKey = null;
            if (inputTable != null && htblColNameValue.get(inputTable.clusteringKey) != null) {
                inputPageClusteringKey = htblColNameValue.get(inputTable.clusteringKey).toString();
            }
            //loop over pages
            int j = 0;
            if (inputTable != null) {
                for (j = 0; j < inputTable.pagesLocation.size(); j++) {

                    //go to next page if rows are full

                    //create new page and insert if all pages are full

                    String min = inputTable.min.get(j);
                    String max = inputTable.max.get(j);
                    out.println(min);
                    String currentPath = "src/main/resources/data/" + strTableName + j + ".bin";
                    Page currentPage = deserializePage(currentPath);
//                    out.println(inputPageClusteringKey);
                    if (inputPageClusteringKey.compareTo(min) < 0) {

                        //entry exists
                        checkIfEntryExists(currentPage, htblColNameValue, inputTable);

                        if (currentPage.pageList.size() < maxRows) { //a5eran
                            insertIntoPage(currentPage, htblColNameValue, inputPageClusteringKey, inputTable, j);

//                            currentPage.pageClusterings.add(inputPageClusteringKey);
//                            currentPage.pageList.add(htblColNameValue);


                            //                        inputTable.pagesLocation.add("src/main/resources/data/"+ currentPage.pageId +".bin");
                            //                        inputTable.min.add((inputTable.min).toString()); //change
                            //                        inputTable.max.add((strTableName + j).toString());

                            serializePage(currentPage);
                            serializeTable(inputTable);
                            return;
                        } else {
                            // >= maxRows
                            String nextPath = "src/main/resources/data/" + strTableName + j + 1 + ".bin";
                            //second page exists
                            if (inputTable.pagesLocation.contains(nextPath)) {
                                Page nextPage = deserializePage(nextPath);
                                //entry exists
                                checkIfEntryExists(nextPage, htblColNameValue, inputTable);

                                if (nextPage.pageList.size() >= maxRows) {
                                    currentPath = "src/main/resources/data/" + strTableName + j + ".bin";
                                    currentPage = deserializePage(currentPath);

                                    //entry exists
                                    checkIfEntryExists(currentPage, htblColNameValue, inputTable);

                                    if (currentPage.overflowPage == null)
                                        currentPage.overflowPage = new Page(currentPage.pageId + "overflowPage");

                                    insertIntoPage(currentPage.overflowPage, htblColNameValue, inputPageClusteringKey, inputTable, j);

                                    currentPage.pageClusterings.add((strTableName + j + "overflowPage").toString());
                                    currentPage.pageList.add(htblColNameValue);

                                    inputTable.pagesLocation.add("src/main/resources/data/" + strTableName + j + "overflowPage" + ".bin");
                                    //                                inputTable.min.add((inputTable.min).toString());
                                    //                                inputTable.max.add((strTableName + j).toString());


                                    serializePage(currentPage);
                                    serializePage(currentPage.overflowPage);
                                    serializeTable(inputTable);
                                    return;

                                } else {


                                    // < max rows

                                    Hashtable entry = currentPage.pageList.get(currentPage.pageList.size() - 1);
                                    currentPage.pageList.removeElementAt(currentPage.pageList.size() - 1);

                                    String tableMax = currentPage.pageList.get(currentPage.pageList.size() - 1).get(inputTable.clusteringKey).toString();


                                    inputTable.max.set(j, tableMax);
                                    currentPage.pageClusterings.removeElementAt(currentPage.pageClusterings.size() - 1);
                                    nextPage.pageList.add(j, htblColNameValue);
                                    nextPage.pageClusterings.add(j, inputPageClusteringKey);
                                    inputTable.min.set(j + 1, (entry.get(inputTable.clusteringKey)).toString());

                                    insertIntoPage(currentPage, htblColNameValue, inputPageClusteringKey, inputTable, j);

                                    //                                System.out.println(nextPage.pageList.size());

                                    serializePage(currentPage);
                                    serializePage(nextPage);
                                    serializeTable(inputTable);
                                }

                            } else {
                                //next path doesn't exist
                                Page newPage = new Page(inputTable.tableName + inputTable.pagesLocation.size());

                                inputTable.pagesLocation.add("src/main/resources/data/" + newPage.pageId + ".bin");

                                currentPath = "src/main/resources/data/" + inputTable.tableName + j + ".bin";
                                currentPage = deserializePage(currentPath);

                                //entry exists
                                checkIfEntryExists(currentPage, htblColNameValue, inputTable);

                                Hashtable entry = currentPage.pageList.get(currentPage.pageList.size() - 1);
                                currentPage.pageList.removeElementAt(currentPage.pageList.size() - 1);

                                String tableMax = currentPage.pageList.get(currentPage.pageList.size() - 1).get(inputTable.clusteringKey).toString();
                                inputTable.max.set(j, tableMax);

                                currentPage.pageClusterings.removeElementAt(currentPage.pageClusterings.size() - 1);

                                newPage.pageList.insertElementAt(entry, 0); //recent change in index 0 to j
                                newPage.pageClusterings.insertElementAt(entry.get(inputTable.clusteringKey).toString(), 0); //recent change

                                inputTable.min.add(entry.get(inputTable.clusteringKey).toString());
                                insertIntoPage(currentPage, htblColNameValue, inputPageClusteringKey, inputTable, j);
                                inputTable.max.add(entry.get(inputTable.clusteringKey).toString());


                                //                            newPage.pageClusterings.add((strTableName + j).toString());
                                //                            newPage.pageList.add(htblColNameValue);

                                inputTable.pagesLocation.add("src/main/resources/data/" + newPage.pageId + ".bin");
                                inputTable.min.add((strTableName + j).toString());
                                inputTable.max.add((strTableName + j).toString());


                                serializePage(currentPage);
                                serializePage(newPage);
                                serializeTable(inputTable);
                                return;
                            }
                    }
                    } else {
                        //inputPageClusteringKey.compareTo(min) >= 0
//                        out.println(inputTable.min.get(j).toString());

                        if (inputPageClusteringKey.toLowerCase().compareTo(min.toLowerCase()) > 0 && inputPageClusteringKey.toLowerCase().compareTo(max.toLowerCase()) < 0) {
                            currentPath = "src/main/resources/data/" + inputTable.tableName + j + ".bin";
                            currentPage = deserializePage(currentPath);

                            //entry exists
                            checkIfEntryExists(currentPage, htblColNameValue, inputTable);

                            if (currentPage.pageList.size() < maxRows) { // henaaaaaa
                                insertIntoPage(currentPage, htblColNameValue, inputPageClusteringKey, inputTable, j);
                                serializeTable(inputTable);
                                serializePage(currentPage);
                                return;
                            }// >= max
                            else {
                                // next page
                                String nextPath = "src/main/resources/data/" + inputTable.tableName + j + 1 + ".bin";
                                if (inputTable.pagesLocation.contains(nextPath)) {
                                    Page nextPage = deserializePage(nextPath);

                                    //entry exists
                                    checkIfEntryExists(nextPage, htblColNameValue, inputTable);

                                    if (nextPage.pageList.size() < maxRows) {
                                        Hashtable entry = currentPage.pageList.get(currentPage.pageList.size() - 1);
                                        currentPage.pageList.removeElementAt(currentPage.pageList.size() - 1);

                                        String tableMax = currentPage.pageList.get(currentPage.pageList.size() - 1).get(inputTable.clusteringKey).toString();

                                        inputTable.max.set(j, tableMax);
                                        currentPage.pageClusterings.removeElementAt(currentPage.pageClusterings.size() - 1);

                                        nextPage.pageList.insertElementAt(entry, 0);
                                        nextPage.pageClusterings.insertElementAt(entry.get(inputTable.clusteringKey).toString(), 0);

                                        inputTable.min.set(j + 1, entry.get(inputTable.clusteringKey).toString());
                                        insertIntoPage(currentPage, htblColNameValue, inputPageClusteringKey, inputTable, j);

                                        serializePage(currentPage);
                                        serializePage(nextPage);
                                        serializeTable(inputTable);
                                    } else {
                                        currentPath = "src/main/resources/data/" + strTableName + j + ".bin";
                                        currentPage = deserializePage(currentPath);

                                        //entry exists
                                        checkIfEntryExists(currentPage, htblColNameValue, inputTable);

                                        if (currentPage.overflowPage == null)
                                            currentPage.overflowPage = new Page(currentPage.pageId + "overflow");

                                        insertIntoPage(currentPage.overflowPage, htblColNameValue, inputPageClusteringKey, inputTable, j);

                                        serializePage(currentPage);
                                        serializePage(currentPage.overflowPage);
                                        serializeTable(inputTable);
                                        return;
                                    }
                                }//no next page
                                else {
                                    Page newPage = new Page(inputTable.tableName + inputTable.pagesLocation.size());
                                    inputTable.pagesLocation.add("src/main/resources/data" + "/" + newPage.pageId + ".bin");

                                    currentPath = "src/main/resources/data/" + inputTable.tableName + j + ".bin";
                                    currentPage = deserializePage(currentPath);

                                    //entry exists
                                    checkIfEntryExists(currentPage, htblColNameValue, inputTable);

                                    Hashtable entry = currentPage.pageList.get(currentPage.pageList.size() - 1);
                                    currentPage.pageList.removeElementAt(currentPage.pageList.size() - 1);

                                    String tableMax = currentPage.pageList.get(currentPage.pageList.size() - 1).get(inputTable.clusteringKey).toString();
                                    inputTable.max.set(j, tableMax);

                                    currentPage.pageClusterings.removeElementAt(currentPage.pageClusterings.size() - 1);
                                    newPage.pageList.insertElementAt(entry, 0); //recent change from 0 to j
                                    newPage.pageClusterings.insertElementAt(entry.get(inputTable.clusteringKey).toString(), 0); //recent change

                                    inputTable.min.add(entry.get(inputTable.clusteringKey).toString());
                                    insertIntoPage(currentPage, htblColNameValue, inputPageClusteringKey, inputTable, j);
                                    inputTable.max.add(entry.get(inputTable.clusteringKey).toString());


                                    //                                newPage.pageList.add(htblColNameValue);
                                    //                                newPage.pageClusterings.add(newPage.pageId);

                                    serializePage(currentPage);
                                    serializePage(newPage);
                                    serializeTable(inputTable);
                                    return;
                                }
                                //end of else -Iam not below max rows-
                            }
                        } else {
                            currentPath = "src/main/resources/data/" + inputTable.tableName + j + ".bin";
                            currentPage = deserializePage(currentPath);

                            //entry exists
                            checkIfEntryExists(currentPage, htblColNameValue, inputTable);

                            if (currentPage.pageList.size() < maxRows) { // henaaaaaa
                                insertIntoPage(currentPage, htblColNameValue, inputPageClusteringKey, inputTable, j);
                                serializeTable(inputTable);
                                serializePage(currentPage);
                                return;
                            }// >= max
                            else {
                                // next page
                                String nextPath = "src/main/resources/data/" + inputTable.tableName + j + 1 + ".bin";
                                if (inputTable.pagesLocation.contains(nextPath)) {
                                    Page nextPage = deserializePage(nextPath);

                                    //entry exists
                                    checkIfEntryExists(nextPage, htblColNameValue, inputTable);

                                    if (nextPage.pageList.size() < maxRows) {
                                        Hashtable entry = currentPage.pageList.get(currentPage.pageList.size() - 1);
                                        currentPage.pageList.removeElementAt(currentPage.pageList.size() - 1);

                                        String tableMax = currentPage.pageList.get(currentPage.pageList.size() - 1).get(inputTable.clusteringKey).toString();

                                        inputTable.max.set(j, tableMax);
                                        currentPage.pageClusterings.removeElementAt(currentPage.pageClusterings.size() - 1);

                                        nextPage.pageList.insertElementAt(entry, 0);
                                        nextPage.pageClusterings.insertElementAt(entry.get(inputTable.clusteringKey).toString(), 0);

                                        inputTable.min.set(j + 1, entry.get(inputTable.clusteringKey).toString());
                                        insertIntoPage(currentPage, htblColNameValue, inputPageClusteringKey, inputTable, j);

                                        serializePage(currentPage);
                                        serializePage(nextPage);
                                        serializeTable(inputTable);
                                    } else {
                                        currentPath = "src/main/resources/data/" + strTableName + j + ".bin";
                                        currentPage = deserializePage(currentPath);

                                        //entry exists
                                        checkIfEntryExists(currentPage, htblColNameValue, inputTable);

                                        if (currentPage.overflowPage == null)
                                            currentPage.overflowPage = new Page(currentPage.pageId + "overflow");

                                        insertIntoPage(currentPage.overflowPage, htblColNameValue, inputPageClusteringKey, inputTable, j);

                                        serializePage(currentPage);
                                        serializePage(currentPage.overflowPage);
                                        serializeTable(inputTable);
                                        return;
                                    }
                                }//no next page
                                else {
                                    Page newPage = new Page(inputTable.tableName + inputTable.pagesLocation.size());
                                    inputTable.pagesLocation.add("src/main/resources/data" + "/" + newPage.pageId + ".bin");

                                    currentPath = "src/main/resources/data/" + inputTable.tableName + j + ".bin";
                                    currentPage = deserializePage(currentPath);

                                    //entry exists
                                    checkIfEntryExists(currentPage, htblColNameValue, inputTable);

                                    Hashtable entry = currentPage.pageList.get(currentPage.pageList.size() - 1);
                                    currentPage.pageList.removeElementAt(currentPage.pageList.size() - 1);

                                    String tableMax = currentPage.pageList.get(currentPage.pageList.size() - 1).get(inputTable.clusteringKey).toString();
                                    inputTable.max.set(j, tableMax);

                                    currentPage.pageClusterings.removeElementAt(currentPage.pageClusterings.size() - 1);
                                    newPage.pageList.insertElementAt(entry, 0); //recent change from 0 to j
                                    newPage.pageClusterings.insertElementAt(entry.get(inputTable.clusteringKey).toString(), 0); //recent change

                                    inputTable.min.add(entry.get(inputTable.clusteringKey).toString());
                                    insertIntoPage(currentPage, htblColNameValue, inputPageClusteringKey, inputTable, j);
                                    inputTable.max.add(entry.get(inputTable.clusteringKey).toString());


                                    //                                newPage.pageList.add(htblColNameValue);
                                    //                                newPage.pageClusterings.add(newPage.pageId);

                                    serializePage(currentPage);
                                    serializePage(newPage);
                                    serializeTable(inputTable);
                                    return;
                                }
                                //end of else -Iam not below max rows-
                            }
                        }

                    }
                } //loop end
            }


            if (inputTable != null && j >= inputTable.pagesLocation.size())
            {
                String location = inputTable.pagesLocation.get(inputTable.pagesLocation.size() - 1);
                Page finalPage = deserializePage(location);

                //entry exists
                checkIfEntryExists(finalPage, htblColNameValue, inputTable);

                if (finalPage.pageList.size() < maxRows) {

                    insertIntoPage(finalPage, htblColNameValue, inputPageClusteringKey, inputTable, inputTable.pagesLocation.size() - 1);
                    serializeTable(inputTable);
                } else {


                    Page newPage = new Page(strTableName + j);

//                    newPage.pageList.add(htblColNameValue);
//                    newPage.pageClusterings.add(inputPageClusteringKey);

                    insertIntoPage(newPage, htblColNameValue, inputPageClusteringKey, inputTable, j);

                    inputTable.min.add(inputPageClusteringKey);
                    inputTable.max.add(inputPageClusteringKey);

                    inputTable.pagesLocation.add("src/main/resources/data/" + newPage.pageId + ".bin");

//                    newPage.pageList.add(htblColNameValue);
//                    newPage.pageClusterings.add(newPage.pageId);

//                    finalPage.pageList.add(htblColNameValue);
//                    finalPage.pageClusterings.add(newPage.pageId);

                    serializePage(newPage);
                    serializeTable(inputTable);

                }

            }
//            newPage.pageList.add(j,htblColNameValue);
//            newPage.pageClusterings.add(j,newPage.pageId);

        }
//        serializeTable(inputTable);

        strTableName = null;
        htblColNameValue = null;
        inputTable = null;


    }

    // following method updates one row only
    // strClusteringKeyValue is the value to look for to find the row to update.
    // htblColNameValue holds the key and new value
    // htblColNameValue will not include clustering key as column name
    public void updateTable(String strTableName,
                            String strClusteringKeyValue,
                            Hashtable<String, Object> htblColNameValue) throws DBAppException {

        boolean columnExists = checkColumnAvailability(strTableName, htblColNameValue);
        if (!columnExists) throw new DBAppException(htblColNameValue.keySet() + " is not a column in table " + strTableName);
        checkDataTypes(strTableName, htblColNameValue);
        try {
            Octree index = null;
            File indexFile = new File("src/main/resources/data/" + strTableName + "_index.bin");
            if (indexFile.exists()) {
                index = deserializeOctree("src/main/resources/data/" + strTableName + "_index.bin");
            }

            tableNames = deserializeVector("src/main/resources/data/tableNames.bin");

//            Table currentTable = null;

            Table currentTable = deserializeTable("src/main/resources/data/" + strTableName + ".bin");

            if (htblColNameValue.contains(currentTable.clusteringKey))
                throw new DBAppException("can not update clustering key");

            if (tableNames.contains(strTableName)) {

                boolean pageFound = false;

                for (int i = 0; i < currentTable.max.size(); i++) {

                    if (index != null) {
                        double p = Double.parseDouble(strClusteringKeyValue);
                        List<Object> result = index.search(p, p, p, 0);
                        if (!result.isEmpty()) {
                            Page page = (Page) result.get(0);
                            p = Double.parseDouble(strClusteringKeyValue);
                            index.addData(p, p, p, page);
                            serializeOctree(index, "src/main/resources/data/" + strTableName + "_index.bin");
                        }


                    } else {

                    if (strClusteringKeyValue.toLowerCase().compareTo((currentTable.min.get(i)).toString()) >= 0 && strClusteringKeyValue.toLowerCase().compareTo((currentTable.max.get(i)).toString()) <= 0) {

                        Page page = deserializePage("src/main/resources/data/" + currentTable.tableName + i + ".bin");

                        if (page.pageClusterings.contains(strClusteringKeyValue)) {
                            int index2 = page.pageClusterings.indexOf(strClusteringKeyValue);
                            for (String key : htblColNameValue.keySet()) {
                                Object value = htblColNameValue.get(key);
                                page.pageList.get(index2).put(key, value);
//                                out.println("key " +key);

                            }
                            serializePage(page);
                            pageFound = true;
                            break;

                        } else if (!page.pageClusterings.contains(strClusteringKeyValue)) {
                            if (page.overflowPage != null) {
                                page.overflowPage = deserializePage("src/main/resources/data/" + currentTable.tableName + i + "overflow" + ".bin");
                                if (page.overflowPage.pageClusterings.contains(strClusteringKeyValue)) {
                                    String value = (String) htblColNameValue.keySet().toArray()[i];
                                    htblColNameValue.put(value, htblColNameValue.get(value));
                                    pageFound = true;
                                }
                            }
                        }
                        serializePage(page);
                    }
                } //else end
                }


                if (!pageFound) throw new DBAppException("Entry not found");

                serializeTable(currentTable);
            } else {
                throw new DBAppException("Table not found");
            }


        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }


    // following method could be used to delete one or more rows.
    // htblColNameValue holds the key and value. This will be used in search // to identify whi ch rows/tuples to delete.
    // htblColNameValue enteries are ANDED together
    public void deleteFromTable(String strTableName,
                                Hashtable<String, Object> htblColNameValue) throws DBAppException {
            try {
                tableNames = deserializeVector("src/main/resources/data/tableNames.bin");

                if (!tableNames.contains(strTableName)) {
                    throw new DBAppException("Table " + strTableName + " does not exist.");
                }

                Table table = deserializeTable("src/main/resources/data/" + strTableName + ".bin");
                List<String> clusteringKeys = Collections.singletonList(table.clusteringKey);
                List<String> minClusteringKeys = table.min;
                List<String> maxClusteringKeys = table.max;

                // Check if an Octree index exists for the table
                File octreeFile = new File("src/main/resources/data/" + strTableName + "_index.bin");
                if (octreeFile.exists()) {
                    Octree octree = deserializeOctree("src/main/resources/data/" + strTableName + "_index.bin");
                    double x = (double) htblColNameValue.get("x");
                    double y = (double) htblColNameValue.get("y");
                    double z = (double) htblColNameValue.get("z");
                    Object info = htblColNameValue.get("info");
                    List<Object> matchingObjects = octree.search(x, y, z, 0);

                    if (matchingObjects.contains(info)) {
                        for (int j = 0 ; j < table.pagesLocation.size() ; j++) {
                            String currentPath = "src/main/resources/data/" + strTableName + j + ".bin";
                            Page page = deserializePage(currentPath);

                            if (page != null && page.pageList.contains(info)) {
                                int rowIndex = page.pageList.indexOf(info);

                                for (int i = 0; i < clusteringKeys.size(); i++) {
                                    String clusteringKey = clusteringKeys.get(i);
                                    String clusteringKeyValue = page.pageList.get(rowIndex).get(clusteringKey).toString();
                                    if (clusteringKeyValue.equals(minClusteringKeys.get(i))) {
                                        minClusteringKeys.set(i, page.pageClusterings.get(0));
                                    }
                                    if (clusteringKeyValue.equals(maxClusteringKeys.get(i))) {
                                        maxClusteringKeys.set(i, page.pageClusterings.get(page.pageClusterings.size() - 1));
                                    }
                                }

                                page.pageClusterings.remove(rowIndex);
                                page.pageList.remove(rowIndex);

                                serializePage(page);
                            }
                        }

                        matchingObjects.remove(info);

                        Octree updatedOctree = new Octree(0, 0, 0, 100, 100, 100);
                        for (Object obj : matchingObjects) {
                            updatedOctree.addData(updatedOctree.getX(),updatedOctree.getY(), updatedOctree.getZ(), obj);
                        }
                        serializeOctree(updatedOctree, "src/main/resources/data/" + strTableName + "_index.bin");
                    }
                } else {

                // Iterate over all pages
                for (int j = 0; j < table.pagesLocation.size(); j++) {
                    String currentPath = "src/main/resources/data/" + strTableName + j + ".bin";
                    Page page = deserializePage(currentPath);

                    // Iterate over all rows in the page
                    if (page != null) {
                        for (int rowIndex = page.pageList.size() - 1; rowIndex >= 0; rowIndex--) {
                            Hashtable<String, Object> row = page.pageList.get(rowIndex);

                            // Check if the row matches the given condition
                            boolean matchesCondition = true;
                            for (String colName : htblColNameValue.keySet()) {
                                if (!row.containsKey(colName) || !row.get(colName).equals(htblColNameValue.get(colName))) {
                                    matchesCondition = false;
                                    break;
                                }
                            }

                            if (matchesCondition) {
                                // Update clustering keys
                                for (int i = 0; i < clusteringKeys.size(); i++) {
                                    String clusteringKey = clusteringKeys.get(i);
                                    String clusteringKeyValue = row.get(clusteringKey).toString();
                                    if (clusteringKeyValue.equals(minClusteringKeys.get(i))) {
                                        minClusteringKeys.set(i, page.pageClusterings.get(0));
                                    }
                                    if (clusteringKeyValue.equals(maxClusteringKeys.get(i))) {
                                        maxClusteringKeys.set(i, page.pageClusterings.get(page.pageClusterings.size() - 1));
                                    }
                                }

                                // Remove row from page
                                page.pageClusterings.remove(rowIndex);
                                page.pageList.remove(rowIndex);
                            }
                        }

                        // If page is empty, remove it from table's metadata
                        if (page.pageClusterings.isEmpty()) {
                            table.pagesLocation.remove(page.pageId);
                            Page deletedPage = deserializePage("src/main/resources/data/" + strTableName + page.pageId + ".bin");
                            deletedPage.pageList.clear();
                        } else {
                            // Serialize updated page
                            serializePage(page);
                        }
                    }

                }
            } //else end

                // Serialize updated table metadata
                serializeTable(table);
                serializeVector(tableNames);
            } catch (Exception e) {
                e.printStackTrace();
            }

        }



//        try {
//            int index = 0;
//            out.println("helooooo");
//            Page page = getPage(strTableName, htblColNameValue);
//            out.println("page id  "+page.pageId);
//            if (page != null) {
//                out.println("here");
//                Table table = deserializeTable("src/main/resources/data/" + strTableName + ".bin");
//                int rowIndex = getRow(page, htblColNameValue, strTableName);
//                out.println(rowIndex);
//
//                out.println();
//                out.println();
//
//                page = deserializePage("src/main/resources/data/" + strTableName + page.pageId + ".bin");
//
//                String clusteringKey = (page.pageList.get(rowIndex).get(table.clusteringKey)).toString();
//
//                page.pageClusterings.removeElementAt(rowIndex);
//                page.pageList.removeElementAt(rowIndex);
//
//                index = Character.getNumericValue(page.pageId.charAt(strTableName.length()));
//                if (clusteringKey == table.min.get(index)) {
//                    table.min.set(index, page.pageClusterings.get(0));
//                }
//
//                if (clusteringKey == table.max.get(index)) {
//                    table.max.removeElementAt(index);
//                    table.max.set(index, page.pageClusterings.get(page.pageClusterings.size() - 1));
//                }
//
//                serializeTable(table);
//            } else {
//                throw new DBAppException("page is empty");
//            }
//            page = deserializePage("src/main/resources/data/" + strTableName + page.pageId + ".bin");
//            if (page.pageClusterings.size() == 0) {
//                page.pageList.clear();
//                return;
//            }
//            page.pageList.removeElementAt(index);
//
//
//        } catch (Exception e) {
//            e.getMessage();
//        }



//
//        Vector tablePages = new Vector(); //Checks the data folder and gets all the pages associated with the table
//        Vector<Object> data_in_file = new Vector<>();  //To store the vector that will be deserialized
//        String metadata_file = "src/main/resources/metadata.csv";
//        Vector Objects_order = new Vector();
//        Boolean rr = false;
//        List<String> table_files2 = new ArrayList<String>();
//        File[] files = new File("src/main/resources/data/").listFiles();
//        for (File file : files) {
//            if (file.isFile()) {
//                table_files2.add(file.getName());
//            }
//        }
//        for (int ppp = 0; ppp < table_files2.size(); ppp++) {
//            String[] pp = table_files2.get(ppp).toString().split("#");
//            if (pp[0].equals(tableName)) {
//                tablePages.add(table_files2.get(ppp));
//            }
//        }
//        System.out.println("The table pages are: " + tablePages);
//        try (BufferedReader br = new BufferedReader(new FileReader(metadata_file))) {
//            String line;
//            br.readLine();
//            while ((line = br.readLine()) != null) {
//                String tableN = line.substring(0, line.indexOf(","));
//                String remainder = line.substring(line.indexOf(",") + 1, line.length());
//                String columnN = remainder.substring(0, remainder.indexOf(","));
//                String[] aa = line.split(", ");
//                if (tableN.equals(tableName)) {
//                    Objects_order.add(columnN.substring(1));
//                }
//            }
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        for (int j = 0; j < tablePages.size(); j++) {
//            //Deserializing
//            try {
//                FileInputStream fileIn = new FileInputStream("src/main/resources/data/" + ((tablePages.get(j)).toString()));
//                ObjectInputStream in = new ObjectInputStream(fileIn);
//                data_in_file = (Vector<Object>) in.readObject();
//                in.close();
//                fileIn.close();
//            } catch (IOException i) {
//                i.printStackTrace();
//                return;
//            } catch (ClassNotFoundException c) {
//                System.out.println("Not found");
//                c.printStackTrace();
//                return;
//            }
//            System.out.println("Original: " + data_in_file);
//
//            Enumeration enu = columnNameValue.keys();
//            ArrayList clustering_key_location = new ArrayList();
//            Hashtable aa = new Hashtable();
//            Vector azaa = new Vector();
//            while (enu.hasMoreElements()) {
//                String current_key = (String) enu.nextElement();
//                String key_value = columnNameValue.get(current_key).toString();
//                String formatedDate = "";
//                azaa.add(current_key);
//                for (int a = 0; a < Objects_order.size(); a++) {
//                    if (current_key.equals(Objects_order.get(a))) {
//                        if (((columnNameValue.get(current_key).getClass()).toString()).equals("class java.util.Date")) {
//                            aa.put(current_key, key_value);
//
//                            String dateStr = key_value;
//                            DateFormat formatter = new SimpleDateFormat("E MMM dd HH:mm:ss Z yyyy");
//                            Date date = null;
//                            try {
//                                date = (Date) formatter.parse(dateStr);
//                            } catch (ParseException e) {
//                                e.printStackTrace();
//                            }
//
//                            Calendar cal = Calendar.getInstance();
//                            cal.setTime(date);
//                            Integer az = cal.get(Calendar.YEAR);
//                            Integer b = (cal.get(Calendar.MONTH) + 1);
//                            Integer c = cal.get(Calendar.DATE);
//
//                            if (((b.toString()).length() == 1) && ((c.toString()).length() != 1)) {
//                                formatedDate = az + "-" + "0" + b + "-" + c;
//                            } else if (((b.toString()).length() != 1) && ((c.toString()).length() == 1)) {
//                                formatedDate = az + "-" + b + "-" + "0" + c;
//                            } else if (((b.toString()).length() == 1) && ((c.toString()).length() == 1)) {
//                                formatedDate = az + "-" + "0" + b + "-" + "0" + c;
//                            } else {
//                                formatedDate = az + "-" + b + "-" + c;
//                            }
//                            aa.put(current_key, formatedDate);
//                        } else {
//                            aa.put(current_key, key_value);
//                        }
//                    }
//                }
//            }
//
//            for (int az = 0; az < azaa.size(); az++) {
//                rr = false;
//                for (int azz = 0; azz < Objects_order.size(); azz++) {
//                    if (azaa.get(az).equals(Objects_order.get(azz))) {
//                        rr = true;
//                    }
//                }
//                if (rr == false) {
//                    System.out.println("Column " + azaa.get(az) + " doesn't exists in the table");
//                    throw new DBAppException();
//                }
//            }
//
//            Vector row = new Vector();
//            Hashtable vv = new Hashtable();
//            Vector data_in_file_final = new Vector();
//            for (int az = 0; az < data_in_file.size(); az++) {
//                row = (Vector) data_in_file.get(az);
//                for (int c = 0; c < Objects_order.size(); c++) {
//                    vv.put(Objects_order.get(c), row.get(c));
//                }
//
//
//                Enumeration enu2 = columnNameValue.keys();
//                Vector tt = new Vector();
//                while (enu2.hasMoreElements()) {
//                    String current_key = (String) enu2.nextElement();
//                    String key_value = columnNameValue.get(current_key).toString();
//                    if (aa.get(current_key).equals(vv.get(current_key))) {
//                        tt.add("true");
//                    } else {
//                        tt.add("false");
//                    }
//                }
//                Boolean tr = true;
//                for (int vf = 0; vf < tt.size(); vf++) {
//                    if (tt.get(vf).equals("false")) {
//                        tr = false;
//                    }
//                }
//
//                if (tr == false) {
//                    data_in_file_final.add(data_in_file.get(az));
//                }
//            }
//            System.out.println("Final: " + data_in_file_final);
//
//            if (data_in_file_final.size() != 0) {
//                //Serializing
//                try {
//                    FileOutputStream fileOut =
//                            new FileOutputStream("src/main/resources/data" + ((tablePages.get(j)).toString()));
//                    ObjectOutputStream out = new ObjectOutputStream(fileOut);
//                    out.writeObject(data_in_file_final);
//                    out.close();
//                    fileOut.close();
//                } catch (IOException i) {
//                    i.printStackTrace();
//                }
//            } else {
//                File myObj = new File("src/main/resources/data" + ((tablePages.get(j)).toString()));
//                if (myObj.delete()) {
//                    System.out.println("Deleted the page: " + myObj.getName());
//                } else {
//                    System.out.println("Failed to delete the file.");
//                }
//            }
//        }



    public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
                                    String[] strarrOperators) throws DBAppException {

        return null;
    }

    public Octree deserializeOctree(String filePath) throws IOException, ClassNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(filePath);
        ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);

        Octree octree = (Octree) objectInputStream.readObject();

        objectInputStream.close();
        fileInputStream.close();

        return octree;
    }

    public void serializeOctree(Octree octree, String filePath) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream(filePath);
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);

        objectOutputStream.writeObject(octree);

        objectOutputStream.close();
        fileOutputStream.close();
    }

    public static String getType (Object object) {
        if (object instanceof String)
            return "java.lang.String";
        else if (object instanceof Integer)
            return "java.lang.Integer";
        else if (object instanceof Double)
            return "java.lang.Double";
        else if (object instanceof Date)
            return "java.util.Date";
        else
            return "NONE";
    }
        public static void checkDataTypes (String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {

        String min;
        String max;

        Vector<String[]> data =new Vector<String[]>();
        String line = "";

        String splitBy = ",";
        int i =0;
        try
        {
            BufferedReader br = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));


            while ((line = br.readLine()) != null)
            {
                String[] array= line.split(splitBy);
                data.add(array);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

        for(int j =0 ;j<data.size();j++){
            if(data.get(j)[0].equals(tableName)) {


                min = data.get(j)[data.get(j).length - 2];

                String colName = data.get(j)[1];
                if (colNameValue.get(colName) != null) {

                    if(colNameValue.get(colName) instanceof  Date){


                        Date d = (Date)colNameValue.get(colName);
                        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                        String strDate = dateFormat.format(d);
                        Date date1= null;
                        try {


                            date1 = new SimpleDateFormat("yyyy-MM-dd").parse(min);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                        Date date2 = null;
                        try {
                            date2 = new SimpleDateFormat("yyy-MM-dd").parse(strDate);
                        } catch (ParseException e) {


                            throw new RuntimeException(e);
                        }
                        if (date2.compareTo(date1) < 0)
                            throw new DBAppException("value below minimum");
                        max = data.get(j)[data.get(j).length - 1];
                        Date datemax= null;
                        try {



                            datemax = new SimpleDateFormat("yyyy-MM-dd").parse(max);
                        } catch (ParseException e) {
                            throw new RuntimeException(e);
                        }
                        if (date2.compareTo(datemax) > 0)
                            throw new DBAppException("value above maximum");
                    }
                    else {
//                        if (colNameValue.get(colName).toString().compareTo(min) < 0) {
//                            throw new DBAppException("value below minimum");
//                        }
                        max = data.get(j)[data.get(j).length - 1];
//                        if (colNameValue.get(colName).toString().compareTo(max) > 1) {
//                            throw new DBAppException("value above maximum");
//                        }



                    }
                }
            }
        }
        String columnName="";
        Set<String> columns  = colNameValue.keySet();



        for(String k : columns) {
            columnName = k;
            String dataType = getType(colNameValue.get(k));

            try {
                if (dataType.equals("NONE"))
                    throw new DBAppException("invalid Data Type");
                String CSVType = "";


                for (int j = 0; j < data.size(); j++) {
                    String[] attributes = data.get(j);
                    if (attributes[0].equals(tableName) && attributes[1].equals(columnName))
                        CSVType = attributes[2];
                }



                if (!CSVType.equals(dataType))
                    throw new DBAppException("invalid Data Type");


            } catch (Exception e) {
                System.out.print(e.getMessage());
            }
        }
    }


    private int findInsertionPageIndex(Table table, String clusteringKeyValue) {
        List<String> pagesLocation = table.pagesLocation;
        List<String> maxValues = table.max;
        int pageIndex = -1;
        for (int i = 0; i < pagesLocation.size(); i++) {
            String maxValue = maxValues.get(i);
            if (clusteringKeyValue.compareTo(maxValue) <= 0) {
                pageIndex = i;
                break;
            }
        }
        if (pageIndex == -1) {
            pageIndex = pagesLocation.size() - 1;
        }
        return pageIndex;
    }

//    public Page getPage(String strtableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
//
//        Page page = null;
//
//        tableNames = deserializeVector("src/main/resources/data/tableNames.bin");
//        try {
//
//            if (!tableNames.contains(strtableName))
//                throw new DBAppException("table doesn't exist!");
//            else {
//
//                Table table = deserializeTable("src/main/resources/data/" + strtableName + ".bin");
//
//                if (htblColNameValue.get(table.clusteringKey) != null) {
////                    out.println("heloo 2");
//                    int var = Collections.binarySearch(table.max, htblColNameValue.get(table.clusteringKey).toString());
////                    var += 3;
////                    out.println(var);
//                    if (var >= 0) {
//                        page = deserializePage("src/main/resources/data/" + strtableName + var + ".bin");
////                        out.println(page.pageId);
//                    } else {
//                        page = deserializePage("src/main/resources/data/" + strtableName + (var - 1) + ".bin");
//                    }
//                } else {
//                    for (int i = 0; i < table.pagesLocation.size(); i++) {
//
//                        String pagePath = "src/main/resources/data/" + (table.tableName + i) + ".bin";
//                        page = deserializePage(pagePath);
//
//                        for (int j = 0; j < page.pageList.size(); j++) {
//
//                            String row = page.pageList.get(j).toString();
//                            String value = htblColNameValue.toString();
//
//                            if (row.contains(value)) {
//                                return page;
//                            }
//                        }
//                    }
//                }
//                serializeVector(tableNames);
//            }
//        } catch (Exception e) {
//            e.getMessage();
//        }
//
//        return page;
//    }

//    public static int getRow(Page page, Hashtable<String, Object> htblColNameValue, String clusteringKey) throws DBAppException {
//
//        int index = 0;
//
//        if (htblColNameValue.get(clusteringKey) == null) {
//            out.println("hena tani");
//            for (int i = 0; i < page.pageList.size(); i++) {
//                String row = page.pageList.elementAt(i).toString();
//                String value = htblColNameValue.toString();
////out.println(row);
////out.println(value);
////out.println(value);
//                if (row.equals(value)) {
//                    out.println(i);
//                    return i;
//                }
//
//            }
//            if (page.overflowPage.pageList.size() != 0 && page.overflowPage != null) {
//
//                for (int i = 0; i < page.overflowPage.pageList.size(); i++) {
//                    String row = page.overflowPage.pageList.get(i).toString();
//                    String value = htblColNameValue.toString();
//
//                    if (row.contains(value))
//                        return i;
//                }
//            }
//        } else {
//
//            out.println("hena aho");
//            index = Collections.binarySearch(page.pageClusterings, htblColNameValue.get(clusteringKey).toString());
//
//            if (index > 0) return index;
//
//            else if (page.overflowPage != null && page.overflowPage.pageList.size() != 0) {
//                int i2 = Collections.binarySearch(page.overflowPage.pageClusterings, htblColNameValue.get(clusteringKey).toString());
//
//                if (i2 > 0) return i2;
//
//
//            }
//
//            throw new DBAppException("row doesn't exist");
//
//        }
//
//        return index;
//
//        }

    public void insertIntoPage(Page page, Hashtable<String, Object> htblColNameValue, String inputPageClusteringKey, Table inputTable, int index) throws DBAppException {

        int searchKeyIndex = Collections.binarySearch(page.pageClusterings, htblColNameValue.get(inputTable.clusteringKey).toString());
        checkIfEntryExists(page, htblColNameValue, inputTable);

//        if (index == -1) {
//            page.pageList.insertElementAt(htblColNameValue, 0);
//            serializePage(page);
//            serializeTable(inputTable);
//            return;
//        }

        if (searchKeyIndex < 0) {
            int var = -1 - searchKeyIndex;
            if (var > 200) {
                throw new DBAppException("more than max rows");
            }
            try {
                page.pageList.insertElementAt(htblColNameValue, var);
            } catch (Exception e) {
                out.println(e.getMessage());
            }
            //insert clustering key in vector
            page.pageClusterings.insertElementAt(htblColNameValue.get(inputTable.clusteringKey.toString()).toString(), var);

            if (inputPageClusteringKey.toLowerCase().compareTo(inputTable.min.get(index).toString()) < 0) {
                inputTable.min.set(index, inputPageClusteringKey);
            }
            if (inputPageClusteringKey.toLowerCase().compareTo(inputTable.max.get(index).toString()) > 0)
                inputTable.max.set(index, inputPageClusteringKey);
        } else {
            out.println("input already exists");
        }
        serializePage(page);
        serializeTable(inputTable);
    }


//    public void insertIntoPage(Page page, Hashtable<String, Object> htblColNameValue, String inputPageClusteringKey, Table inputTable) throws DBAppException {
//
//        int searchKeyIndex = Collections.binarySearch(page.pageClusterings, htblColNameValue.get(inputTable.clusteringKey).toString());
//        checkIfEntryExists(page, htblColNameValue, inputTable);
//
//
//
//        if (searchKeyIndex < 0) {
//            int var = -1 - searchKeyIndex;
//            if (var > 200) {
//                throw new DBAppException("more than max rows");
//            }
//            try {
//                page.pageList.insertElementAt(htblColNameValue, var);
//            } catch (Exception e) {
//                out.println(e.getMessage());
//            }
//            //insert clustering key in vector
//            page.pageClusterings.insertElementAt(htblColNameValue.get(inputTable.clusteringKey.toString()).toString(), var);
//
//        } else {
//            out.println("input already exists");
//        }
//        serializePage(page);
//        serializeTable(inputTable);
//    }


//    public void insertIntoPage(Page page, Hashtable<String, Object> htblColNameValue, String inputPageClusteringKey, Table inputTable, int index) throws DBAppException {
//
//        int searchKeyIndex = Collections.binarySearch(page.pageClusterings, htblColNameValue.get(inputTable.clusteringKey).toString());
//
//        if (index == -1) {
//            page.pageList.insertElementAt(htblColNameValue, 0);
//            serializePage(page);
//            serializeTable(inputTable);
//            return;
//        }
//
//        if (searchKeyIndex < 0) {
//
//            int var = -1 - searchKeyIndex;
//
//            if (var != 0) var += 1;
//
//            if (var > 200) {
//                throw new DBAppException("more than max rows");
//            }
//            try {
//                page.pageList.insertElementAt(htblColNameValue, var);
//            } catch (Exception e) {
//                out.println(e.getMessage());
//            }
//            //insert clustering key in vector
//            page.pageClusterings.insertElementAt(htblColNameValue.get(inputTable.clusteringKey.toString()).toString(), var);
//
//            if (inputPageClusteringKey.toLowerCase().compareTo(inputTable.min.get(index).toString()) < 0) {
//                inputTable.min.set(index, inputPageClusteringKey);
//            }
//            if (inputPageClusteringKey.toLowerCase().compareTo(inputTable.max.get(index).toString()) > 0)
//                inputTable.max.set(index, inputPageClusteringKey);
//        } else {
//            out.println("input already exists");
//        }
//        serializePage(page);
//        serializeTable(inputTable);
//    }

//    public void checkIfEntryExists(Page page, Hashtable<String, Object> htblColNameValue, Table table) {
//        try {
//            if (page.pageClusterings.contains(htblColNameValue.get(table.clusteringKey).toString()))
//                throw new DBAppException("a value with this primary key exists");
//        } catch (Exception e) {
//            out.println(e.getMessage());
//        }
//
//    }
public void checkIfEntryExists(Page page, Hashtable<String, Object> htblColNameValue, Table table) {
    try {
        String clusteringKey = table.clusteringKey;
        for (Object entry : page.pageClusterings) {
            if (entry instanceof Hashtable) {
                Hashtable<String, Object> entryHashtable = (Hashtable<String, Object>) entry;
                if (entryHashtable.get(clusteringKey).equals(htblColNameValue.get(clusteringKey))) {
                    throw new DBAppException("a value with this primary key exists");
                }
            }
        }
    } catch (Exception e) {
        out.println(e.getMessage());
    }
}



//    public boolean checkColumnAvailability(String strTableName,
//                                          Hashtable<String, Object> htblColNameValue) {
//        String lineReader = "";
//        Vector<String[]> tablesInfo = new Vector<String[]>();
//
//        try {
//            BufferedReader metaData = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
//            while ((lineReader = metaData.readLine()) != null) {
//                tablesInfo.add(lineReader.split(","));
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        HashSet<String> columns = new HashSet<String>();
//        for (int i = 0; i < tablesInfo.size(); i++) {
//            if (tablesInfo.get(i)[0] == strTableName) columns.add(strTableName);
//            out.println(tablesInfo.get(i)[1]);
//        }
//
//
//        if (columns.containsAll(htblColNameValue.keySet())) return true;
//
//        return false;
//    }

    public boolean checkColumnAvailability(String strTableName, Hashtable<String, Object> htblColNameValue) {
        String lineReader = "";
        Vector<String[]> tablesInfo = new Vector<String[]>();

        try {
            BufferedReader metaData = new BufferedReader(new FileReader("src/main/resources/metadata.csv"));
            while ((lineReader = metaData.readLine()) != null) {
                tablesInfo.add(lineReader.split(","));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        HashSet<String> columns = new HashSet<String>();
        for (int i = 0; i < tablesInfo.size(); i++) {
            if (tablesInfo.get(i)[0].equals(strTableName)) {
                columns.add(tablesInfo.get(i)[1]);
            }
        }

        return columns.containsAll(htblColNameValue.keySet());
    }

    public int readConfig(String keyWord) throws IOException {
        int integer = 0;
        try {
            FileReader file = new FileReader("src/main/resources/DBApp.config");

            Properties p = new Properties();
            p.load(file);

            integer = Integer.parseInt(p.getProperty(keyWord));
        } catch (Exception e) {
            e.getMessage();

        }

        return integer;
    }

    public static String getTableAttributes(String strTableName,
                                            String strClusteringKeyColumn,
                                            Hashtable<String, String> htblColNameType,
                                            Hashtable<String, String> htblColNameMin,
                                            Hashtable<String, String> htblColNameMax) {
        String Attributes = "";
        String columnName = "";
        String columnType = "";
        boolean clusteringKey = false;
        String indexName = null;
        String indexType = null;
        String columnMin = "";
        String columnMax = "";
        Set<String> columns = htblColNameType.keySet();
        for (String i : columns) {
            columnName = i;
            columnType = htblColNameType.get(i);
            columnMin = htblColNameMin.get(i);
            columnMax = htblColNameMax.get(i);


            if (strClusteringKeyColumn.equals(i)) clusteringKey = true;

            Attributes += "\n" + strTableName + "," + columnName + "," + columnType + "," + clusteringKey + "," + indexName + ","
                    + indexType + "," + columnMin + "," + columnMax;
        }
        return Attributes;

//       return "error in getting table attributes";

    }

    public static Vector<String> deserializeVector(String filePath) {
        Vector<String> vector = new Vector<>();
        try {
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream input = new ObjectInputStream(file);
            vector = (Vector<String>) input.readObject();
            input.close();
            file.close();
        } catch (IOException e) {
            out.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            out.println(e.getMessage());
        }
        return vector;
    }

    public static void serializeVector(Vector<String> vector) {
        try {
            FileOutputStream fileOutput = new FileOutputStream("src/main/resources/data/tableNames.bin");
            ObjectOutputStream output = new ObjectOutputStream(fileOutput);

            output.writeObject(vector);
            output.close();
            fileOutput.close();
        } catch (IOException ex) {
            out.println(ex.getMessage());
        }

    }

    public static void serializeTable(Table table) {

        try {
            FileOutputStream file = new FileOutputStream("src/main/resources/data/" + table.tableName + ".bin");
            ObjectOutputStream output = new ObjectOutputStream(file);

            output.writeObject(table);
            output.close();
            file.close();
        } catch (IOException e) {
            out.println(e.getMessage());
        }

    }

    public static Table deserializeTable(String filePath) {
        Table table = null;
        try {
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream input = new ObjectInputStream(file);

            table = (Table) input.readObject();
            input.close();
            file.close();

        } catch (IOException | ClassNotFoundException e) {
            out.println(e.getMessage());
        }

        return table;
    }

    public static Page deserializePage(String filePath) {
        Page page = null;
        try {
            FileInputStream file = new FileInputStream(filePath);
            ObjectInputStream input = new ObjectInputStream(file);

            page = (Page) input.readObject();
            input.close();
            file.close();

        } catch (IOException | ClassNotFoundException e) {
            out.println(e.getMessage());
        }

        return page;
    }

    public static void serializePage(Page page) {

        try {
            FileOutputStream file = new FileOutputStream("src/main/resources/data/" + page.pageId + ".bin");
            ObjectOutputStream output = new ObjectOutputStream(file);

            output.writeObject(page);

            output.close();
            file.close();

        } catch (IOException e) {
            out.println(e.getMessage());
        }

    }

    public static void main(String[] args) throws DBAppException, IOException {
//
//        String strTableName = "Student";
//
//        DBApp dbApp = new DBApp();
//
//        Hashtable<String, String> min = new Hashtable<>();
//        Hashtable<String, String> max = new Hashtable<>();
//
//
//        Hashtable htblColNameType = new Hashtable();
//        htblColNameType.put("id", "java.lang.Integer");
//        htblColNameType.put("name", "java.lang.String");
//        htblColNameType.put("gpa", "java.lang.double");
//
//        dbApp.createTable(strTableName, "id", htblColNameType, min, max);
//
//
//
//        Hashtable htblColNameValue = new Hashtable();
//        htblColNameValue.put("id", new String("c"));
//        htblColNameValue.put("name", new String("mohamed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new String("b"));
//        htblColNameValue.put("name", new String("mohamed Noor"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new String("a"));
//        htblColNameValue.put("name", new String("Ahmed medhat"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new String("d"));
//        htblColNameValue.put("name", new String("gehan"));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new String("e"));
//        htblColNameValue.put("name", new String("gehan "));
//        htblColNameValue.put("gpa", new Double(0.95));
//        dbApp.insertIntoTable(strTableName, htblColNameValue);




//        Page page = deserializePage("src/main/resources/data/Student0.bin");
//        for (int i = 0; i < page.pageList.size(); i++) {
//            System.out.println(page.pageList.elementAt(i));
//
//        }
////
//
//        htblColNameValue.clear();
//        htblColNameValue.put("id", new String("e"));
//        htblColNameValue.put("name", new String("salaaa777 "));
//        htblColNameValue.put("gpa", new Double(1.5));
//        dbApp.deleteFromTable(strTableName, htblColNameValue);
//
//        for (int i = 0; i < page.pageList.size(); i++) {
//            System.out.println(page.pageList.elementAt(i));
//
//        }

    }
}

