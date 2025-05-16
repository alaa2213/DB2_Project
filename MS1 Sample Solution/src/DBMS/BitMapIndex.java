package DBMS;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class BitMapIndex implements Serializable {
    private Map<String, String> bitVectors;

    public BitMapIndex() {
        this.bitVectors = new HashMap<>();
    }

    public static void createBitMapIndex(String tableName, String colName) {
    	long Starttime=System.currentTimeMillis();
        Table table = FileManager.loadTable(tableName);
        if (table == null) return;

        BitMapIndex index = new BitMapIndex();
        int totalRecords = 0;
        Map<String, ArrayList<Integer>> valuePositions = new HashMap<>();

        // Scan all pages to collect positions for each unique value
        for (int i = 0; i < table.getPageCount(); i++) {
            Page page = FileManager.loadTablePage(tableName, i);
            if (page != null) {
                ArrayList<String[]> records = page.select();
                int recordPos = totalRecords;
                for (String[] record : records) {
                    String value = record[Arrays.asList(table.getcolName()).indexOf(colName)];
                    valuePositions.computeIfAbsent(value, k -> new ArrayList<>()).add(recordPos);
                    recordPos++;
                }
                totalRecords += records.size();
            }
        }

        // Generate bit-vectors
        int maxLength = totalRecords;
        for (Map.Entry<String, ArrayList<Integer>> entry : valuePositions.entrySet()) {
            StringBuilder bitVector = new StringBuilder();
            for (int j = 0; j < maxLength; j++) {
                bitVector.append(entry.getValue().contains(j) ? "1" : "0");
            }
            index.bitVectors.put(entry.getKey(), bitVector.toString());
        }

        // Store the index to disk
        File tableDirectory = new File(FileManager.directory, tableName);
        File indexFile = new File(tableDirectory, tableName + "_" + colName + ".db");
        try {
            FileOutputStream fout = new FileOutputStream(indexFile);
            ObjectOutputStream oos = new ObjectOutputStream(fout);
            oos.writeObject(index);
            oos.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        long Endtime=System.currentTimeMillis();
        String traceEntry = "Index created for column:" + colName;
        boolean exists = table.getTrace().stream().anyMatch(entry -> entry.startsWith(traceEntry));

        if (!exists) {
            table.getTrace().add(traceEntry + " execution time," + (Endtime - Starttime));
        }

       
        FileManager.storeTable(tableName, table);
    }

    public static String getValueBits(String tableName, String colName, String value) {
        File tableDirectory = new File(FileManager.directory, tableName);
        File indexFile = new File(tableDirectory, tableName + "_" + colName + ".db");

        if (!indexFile.exists()) {
            return null;
        }

        BitMapIndex index = null;
        try {
            FileInputStream fin = new FileInputStream(indexFile);
            ObjectInputStream ois = new ObjectInputStream(fin);
            index = (BitMapIndex) ois.readObject();
            ois.close();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        String bits = index.bitVectors.get(value);
        if (bits != null) return bits;

        // Generate zero-filled bit vector for unseen value
        int vectorLength = index.bitVectors.values().stream()
            .findAny()
            .map(String::length)
            .orElse(0);

        StringBuilder zeros = new StringBuilder();
        for (int i = 0; i < vectorLength; i++) {
            zeros.append('0');
        }
        return zeros.toString();

    }

}