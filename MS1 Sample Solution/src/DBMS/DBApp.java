package DBMS;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DBApp
{
	static int dataPageSize = 2;
	static Map<String, Set<String>> indexedColumns = new HashMap<>();


	public static void createTable(String tableName, String[] columnsNames)
	{
		Table t = new Table(tableName, columnsNames);
		FileManager.storeTable(tableName, t);
		indexedColumns.putIfAbsent(tableName, new HashSet<>());
	}

	public static void insert(String tableName, String[] record)
	{
		Table t = FileManager.loadTable(tableName);
		t.insert(record);
		FileManager.storeTable(tableName, t);
		Set<String> columns = indexedColumns.getOrDefault(tableName, new HashSet<>());
	    for (String colName : columns) {
	        BitMapIndex.createBitMapIndex(tableName, colName);
	    }
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
	 
	 public static void createBitMapIndex(String tableName, String colName) {
		    BitMapIndex.createBitMapIndex(tableName, colName);
		    indexedColumns.get(tableName).add(colName);
		}
	 
	 public static String getValueBits(String tableName, String colName, String value) {
		    return BitMapIndex.getValueBits(tableName, colName, value);
		}
	 private static ArrayList<String[]> linearFilter(ArrayList<String[]> records, String[] cols, String[] vals, Table table) {
		    ArrayList<String[]> filtered = new ArrayList<>();
		    for (String[] record : records) {
		        boolean match = true;
		        for (int i = 0; i < cols.length; i++) {
		            int colIndex = Arrays.asList(table.getcolName()).indexOf(cols[i]);
		            if (!record[colIndex].equals(vals[i])) {
		                match = false;
		                break;
		            }
		        }
		        if (match) {
		            filtered.add(record);
		        }
		    }
		    return filtered;
		}

	 private static ArrayList<String[]> intersectResults(HashMap<String, ArrayList<String[]>> indexedResults, String[] cols) {
		    if (cols.length == 0) return new ArrayList<>();

		    Set<List<String>> resultSet = new HashSet<>();
		    boolean first = true;

		    for (String col : cols) {
		        ArrayList<String[]> list = indexedResults.get(col);
		        if (list == null) continue;

		        Set<List<String>> currentSet = new HashSet<>();
		        for (String[] r : list) {
		            currentSet.add(Arrays.asList(r));
		        }

		        if (first) {
		            resultSet = currentSet;
		            first = false;
		        } else {
		            resultSet.retainAll(currentSet); // AND operation
		        }
		    }

		    ArrayList<String[]> result = new ArrayList<>();
		    for (List<String> rowList : resultSet) {
		        result.add(rowList.toArray(new String[0]));
		    }
		    return result;
		}
	 public static ArrayList<String[]> selectIndex(String tableName, String[] cols, String[] vals) {
		    long startTime = System.currentTimeMillis();

		    Table table = FileManager.loadTable(tableName);
		    if (table == null) return new ArrayList<>();

		    ArrayList<Integer> indexedCols = new ArrayList<>();
		    ArrayList<Integer> nonIndexedCols = new ArrayList<>();

		    // Classify columns
		    for (int i = 0; i < cols.length; i++) {
		        File indexFile = new File(FileManager.directory + "/" + tableName,
		                tableName + "_" + cols[i] + ".db");
		        if (indexFile.exists()) {
		            indexedCols.add(i);
		        } else {
		            nonIndexedCols.add(i);
		        }
		    }

		    String combinedBits = null;
		    int indexedSelectionCount = 0;
		    ArrayList<String[]> selectedRecords = new ArrayList<>();

		    // Step 1: Apply bitmap AND for all indexed conditions
		    if (!indexedCols.isEmpty()) {
		        ArrayList<String> bitVectors = new ArrayList<>();

		        for (int i : indexedCols) {
		            String bits = BitMapIndex.getValueBits(tableName, cols[i], vals[i]);
		            if (bits == null || bits.isEmpty()) {
		                combinedBits = null;
		                break;
		            }
		            bitVectors.add(bits);
		        }

		        if (!bitVectors.isEmpty()) {
		            int length = bitVectors.get(0).length();
		            StringBuilder result = new StringBuilder();
		            for (int j = 0; j < length; j++) {
		                boolean isOne = true;
		                for (String bv : bitVectors) {
		                    if (bv.charAt(j) != '1') {
		                        isOne = false;
		                        break;
		                    }
		                }
		                result.append(isOne ? '1' : '0');
		            }
		            combinedBits = result.toString();
		        }

		        // Step 2: Collect records based on bitmap result
		        if (combinedBits != null) {
		            int recordIndex = 0;
		            for (int i = 0; i < table.getPageCount(); i++) {
		                Page page = FileManager.loadTablePage(tableName, i);
		                ArrayList<String[]> records = page.select();
		                for (String[] record : records) {
		                    if (recordIndex < combinedBits.length() && combinedBits.charAt(recordIndex) == '1') {
		                        selectedRecords.add(record);
		                    }
		                    recordIndex++;
		                }
		            }
		            indexedSelectionCount = selectedRecords.size();
		        }
		    }

		    ArrayList<String[]> finalResult = new ArrayList<>();

		    // Step 3: Apply linear filtering for non-indexed columns
		    if (!indexedCols.isEmpty()) {
		        for (String[] record : selectedRecords) {
		            boolean match = true;
		            for (int i : nonIndexedCols) {
		                int colIndex = Arrays.asList(table.getcolName()).indexOf(cols[i]);
		                if (!record[colIndex].equals(vals[i])) {
		                    match = false;
		                    break;
		                }
		            }
		            if (match) {
		                finalResult.add(record);
		            }
		        }
		    } else {
		        // No indexed columns, full scan
		        finalResult = table.select(cols, vals);
		    }

		    // Step 4: Trace output
		    StringBuilder trace = new StringBuilder();
		    trace.append("Select index condition:")
		         .append(Arrays.toString(cols)).append("->").append(Arrays.toString(vals)).append(", ");

		    ArrayList<String> indexedColNames = new ArrayList<>();
		    ArrayList<String> nonIndexedColNames = new ArrayList<>();
		    for (int i : indexedCols) indexedColNames.add(cols[i]);
		    for (int i : nonIndexedCols) nonIndexedColNames.add(cols[i]);
	    
	    Collections.sort(indexedColNames);
		    trace.append("Indexed columns: ").append(indexedColNames);
		    if (!indexedCols.isEmpty()) {
		    	trace.append(", Indexed selection count: ").append(indexedSelectionCount);
		    }
		    if (!nonIndexedColNames.isEmpty()) {
		        trace.append(", Non-indexed: ").append(nonIndexedColNames);
		    }

		    trace.append(", Final count: ").append(finalResult.size());
		    trace.append(", execution time (mil):").append(System.currentTimeMillis() - startTime);
		  
			
		    if (!table.getTrace().contains(trace.toString())) {
		        table.getTrace().add(trace.toString());
		        FileManager.storeTable(tableName, table);
		    }

		    return finalResult;
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
	  
	  createBitMapIndex("student", "gpa"); 
	    createBitMapIndex("student", "major");
	    
	    System.out.println("Bitmap of the value of CS from the major index: " + getValueBits("student", "major", "CS")); 
	    System.out.println("Bitmap of the value of 1.2 from the gpa index: " + getValueBits("student", "gpa", "1.2"));
	   
	  String[] r4 = {"4", "stud4", "CS", "9", "1.2"}; 
	  insert("student", r4); 
	   
	  String[] r5 = {"5", "stud5", "BI", "4", "3.5"}; 
	  insert("student", r5); 
	  System.out.println("After new insertions:");  
	    System.out.println("Bitmap of the value of CS from the major index: " + getValueBits("student", "major", "CS")); 
	    System.out.println("Bitmap of the value of 1.2 from the gpa index: " + getValueBits("student", "gpa", "1.2"));
	   
	  //////// This is the code used to delete pages from the table 
//	  System.out.println("File Manager trace before deleting pages: "+FileManager.trace());
//	  String path = 
//	FileManager.class.getResource("FileManager.class").toString(); 
//	     File directory = new File(path.substring(6,path.length()-17) + 
//	File.separator 
//	       + "Tables//student" + File.separator); 
//	     File[] contents = directory.listFiles(); 
//	     int[] pageDel = {0,2}; 
//	for(int i=0;i<pageDel.length;i++) 
//	{ 
//	contents[pageDel[i]].delete(); 
//	} 
	////////End of deleting pages code 
//	System.out.println("File Manager trace after deleting pages: "+FileManager.trace()); 
//	ArrayList<String[]> tr = validateRecords("student"); 
//	System.out.println("Missing records count: "+tr.size()); 
//	recoverRecords("student", tr); 
//	System.out.println("--------------------------------"); 
//	System.out.println("Recovering the missing records."); 
//	tr = validateRecords("student"); 
//	System.out.println("Missing record count: "+tr.size()); 
//	System.out.println("File Manager trace after recovering missing records: "+FileManager.trace()); 
//	System.out.println("--------------------------------"); 
//	System.out.println("Full trace of the table: "); 
//	System.out.println(getFullTrace("student")); 
	System.out.println("Output of selection using index when all columns of the select conditions are indexed:"); 
			  ArrayList<String[]> result1 = selectIndex("student", new String[] 
			{"major","gpa"}, new String[] {"CS","1.2"}); 
			        for (String[] array : result1) { 
			            for (String str : array) { 
			                System.out.print(str + " "); 
			            } 
			            System.out.println(); 
			        } 
			  System.out.println("Last trace of the table: "+getLastTrace("student")); 
			        System.out.println("--------------------------------"); 
			         
			  System.out.println("Output of selection using index when only one column of the columns of the select conditions are indexed:"); 
			  ArrayList<String[]> result2 = selectIndex("student", new String[] 
			{"major","semester"}, new String[] {"CS","5"}); 
			        for (String[] array : result2) { 
			            for (String str : array) { 
			                System.out.print(str + " "); 
			            } 
			            System.out.println(); 
			        } 
			  System.out.println("Last trace of the table: "+getLastTrace("student")); 
			        System.out.println("--------------------------------");
			        System.out.println("Output of selection using index when some of the columns of the select conditions are indexed:"); 
			        		ArrayList<String[]> result3 = selectIndex("student", new String[] 
			        		{"major","semester","gpa" }, new String[] {"CS","5", "0.9"}); 
			        		for (String[] array : result3) { 
			        		for (String str : array) { 
			        		System.out.print(str + " "); 
			        		} 
			        		System.out.println(); 
			        		} 
			        		System.out.println("Last trace of the table: "+getLastTrace("student")); 
			        		System.out.println("--------------------------------"); 
			        		System.out.println("Full Trace of the table:"); 
			        		System.out.println(getFullTrace("student")); 
			        		System.out.println("--------------------------------"); 
			        		System.out.println("The trace of the Tables Folder:"); 
			        		System.out.println(FileManager.trace()); 
			        		} 
} 


