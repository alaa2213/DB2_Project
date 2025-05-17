package DBMS;

import java.io.Serializable;
import java.util.ArrayList;

public class Page implements Serializable
{

	private ArrayList<String []> records;
	
	
	public Page() 
	{
		super();
		this.records = new ArrayList<String[]>();
	}
	
	public boolean insert(String []record)
	{
		if(records.size()<DBApp.dataPageSize)
		{
			this.records.add(record);
			return true;
		}
		return false;
	}
	
	public ArrayList<String[]> select() {
	    // Return a new copy of the records to prevent external modification
	    return new ArrayList<>(records != null ? records : new ArrayList<>());
	}

	public ArrayList<String[]> select(String[] cond) {
	    ArrayList<String[]> res = new ArrayList<>();
	    
	    // Null/empty checks
	    if (cond == null || cond.length == 0 || records == null || records.isEmpty()) {
	        return res;
	    }

	    for (String[] record : records) {
	        // Skip null records
	        if (record == null) {
	            continue;
	        }

	        boolean match = true;
	        for (int j = 0; j < cond.length; j++) {
	            // Skip null conditions but check non-null ones
	            if (cond[j] != null) {
	                // Check array bounds and compare values
	                if (j >= record.length || !cond[j].equals(record[j])) {
	                    match = false;
	                    break;
	                }
	            }
	        }
	        
	        if (match) {
	            res.add(record.clone()); // Add a copy of the record
	        }
	    }
	    
	    return res;
	}

	public ArrayList<String[]> select(int index) {
	    ArrayList<String[]> res = new ArrayList<>();
	    
	    // Bounds checking
	    if (records == null || index < 0 || index >= records.size()) {
	        return res;
	    }
	    
	    String[] record = records.get(index);
	    if (record != null) {
	        res.add(record.clone()); // Add a copy of the record
	    }
	    
	    return res;
	}

}
