package DBMS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DBApp
{
	static int dataPageSize = 2;


	public static void createTable(String tableName, String[] columnsNames)
	{
		Table t = new Table(tableName, columnsNames);
		FileManager.storeTable(tableName, t);
	}

	public static void insert(String tableName, String[] record)
	{
		Table t = FileManager.loadTable(tableName);
		t.insert(record);
		FileManager.storeTable(tableName, t);
	}

	public static ArrayList<String []> select(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select();
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String []> select(String tableName, int pageNumber, int recordNumber)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select(pageNumber, recordNumber);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static ArrayList<String []> select(String tableName, String[] cols, String[] vals)
	{
		Table t = FileManager.loadTable(tableName);
		ArrayList<String []> res = t.select(cols, vals);
		FileManager.storeTable(tableName, t);
		return res;
	}

	public static String getFullTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		String res = t.getFullTrace();
		return res;
	}

	public static String getLastTrace(String tableName)
	{
		Table t = FileManager.loadTable(tableName);
		String res = t.getLastTrace();
		return res;
	}
	public static int findOriginalPageIndex(Table table, String[] record) {
	    if (table == null || record == null) return -1;

	    
	    int recordIndex = 0;

	    for (int i = 0; i < table.getPageCount(); i++) {
	        Page p = table.getPage(i);
	        if (p != null) {
	            ArrayList<String[]> records = p.select();
	            for (String[] r : records) {
	                if (Arrays.equals(r, record)) {
	                    return i; 
	                }
	                recordIndex++;
	            }
	        }
	    }
	    return -1;
	}


	public static ArrayList<String[]> validateRecords(String tableName) {
	    ArrayList<String[]> missingRecords = new ArrayList<>();

	   
	    Table table = FileManager.loadTable(tableName);
	    if (table == null) return missingRecords;
    
	   
	    String path = FileManager.class.getResource("FileManager.class").toString();
	    String tableDirPath = path.substring(6, path.length() - 17) + File.separator + "Tables" + File.separator + tableName;
	    File tableDir = new File(tableDirPath);

	  
	    Set<String> existingPages = new HashSet<>();
	    File[] files = tableDir.listFiles();
	    if (files != null) {
	        for (File f : files) {
	            String name = f.getName();
	            if (name.endsWith(".db") && name.matches("\\d+\\.db")) {
	                existingPages.add(name);
	            }
	        }
	    }

	    
	    for (int i = 0; i < table.getPageCount(); i++) {
	        String pageFilename = i + ".db";
	        if (!existingPages.contains(pageFilename)) {
	           
	        	
	            Page missingPage = table.getPage(i); 
	            if (missingPage != null) {
	                missingRecords.addAll(missingPage.select());
	            }
	        }
	    }
        table.getTrace().add("Validating records: "+missingRecords.size()+" records missing.");
        FileManager.storeTable(tableName, table);
	    return missingRecords;
	}
	 public static void recoverRecords(String tableName,ArrayList<String[]> missing) {
		
		 Table table = FileManager.loadTable(tableName);
		
		 Set<Integer> recoveredPages = new HashSet<>();
		
			 for(int j=0;j<missing.size();j++) {
				 int pageIndex = findOriginalPageIndex(table, missing.get(j));
			        if (pageIndex == -1) continue; 
                   Page p=table.getPage(pageIndex);
                   if(p==null) {
                	   table.addPage(p);
                	  
                   }
                   p.insert(missing.get(j));
                   FileManager.storeTablePage(tableName, pageIndex, p);
			        recoveredPages.add(pageIndex);
			       
				
			 }
			
			    table.getTrace().add("Recovering " + missing.size() + " records in pages: " + recoveredPages+".");
			    FileManager.storeTable(tableName, table);
		 }
		 
	 

	
	public static void main(String []args) throws IOException 
	 { 
	  FileManager.reset(); 
	  String[] cols = {"id","name","major","semester","gpa"}; 
	  createTable("student", cols); 
	  String[] r1 = {"1", "stud1", "CS", "5", "0.9"}; 
	  insert("student", r1); 
	   
	  String[] r2 = {"2", "stud2", "BI", "7", "1.2"}; 
	  insert("student", r2); 
	   
	  String[] r3 = {"3", "stud3", "CS", "2", "2.4"}; 
	  insert("student", r3); 
	   
	  String[] r4 = {"4", "stud4", "CS", "9", "1.2"}; 
	  insert("student", r4); 
	   
	  String[] r5 = {"5", "stud5", "BI", "4", "3.5"}; 
	  insert("student", r5); 
	   
	  //////// This is the code used to delete pages from the table 
	  System.out.println("File Manager trace before deleting pages: "+FileManager.trace());
	  String path = 
	FileManager.class.getResource("FileManager.class").toString(); 
	     File directory = new File(path.substring(6,path.length()-17) + 
	File.separator 
	       + "Tables//student" + File.separator); 
	     File[] contents = directory.listFiles(); 
	     int[] pageDel = {0,2}; 
	for(int i=0;i<pageDel.length;i++) 
	{ 
	contents[pageDel[i]].delete(); 
	} 
	////////End of deleting pages code 
	System.out.println("File Manager trace after deleting pages: "+FileManager.trace()); 
	ArrayList<String[]> tr = validateRecords("student"); 
	System.out.println("Missing records count: "+tr.size()); 
	recoverRecords("student", tr); 
	System.out.println("--------------------------------"); 
	System.out.println("Recovering the missing records."); 
	tr = validateRecords("student"); 
	System.out.println("Missing record count: "+tr.size()); 
	System.out.println("File Manager trace after recovering missing records: "+FileManager.trace()); 
	System.out.println("--------------------------------"); 
	System.out.println("Full trace of the table: "); 
	System.out.println(getFullTrace("student")); 
} 

}
