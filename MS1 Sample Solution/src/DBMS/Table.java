package DBMS;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Table implements Serializable {
    private static final int MAX_CACHE_SIZE = 100;
    private String name;
    private String[] columnsNames;
    private int pageCount;
    private int recordsCount;
    private ArrayList<String> trace;
    private ArrayList<Page> pages;
    private transient Map<Integer, Page> pageCache;
    
    public Table(String name, String[] columnsNames) {
        super();
        this.name = name;
        this.columnsNames = columnsNames;
        this.trace = new ArrayList<String>();
        this.pages = new ArrayList<Page>();
        this.pageCache = new HashMap<Integer, Page>();
        this.trace.add("Table created name:" + name + ", columnsNames:" + Arrays.toString(columnsNames));
    }

    private synchronized void initCache() {
        if (pageCache == null) {
            pageCache = new HashMap<Integer, Page>();
        }
    }

    private synchronized void addToCache(int index, Page page) {
        initCache();
        if (pageCache.size() >= MAX_CACHE_SIZE) {
            pageCache.remove(pageCache.keySet().iterator().next());
        }
        pageCache.put(index, page);
    }

    @Override
    public String toString() {
        return "Table [name=" + name + ", columnsNames=" + Arrays.toString(columnsNames) + ", pageCount=" + pageCount + ", recordsCount=" + recordsCount + "]";
    }

    public synchronized void addPage(Page p) {
        initCache();
        pages.add(p);
        pageCache.put(pages.size() - 1, p);
    }

    public String[] getcolName() {
        return this.columnsNames;
    }

    public ArrayList<String> getTrace() {
        return this.trace;
    }

    public synchronized Page getPage(int i) {
        initCache();
        if (i >= 0 && i < pageCount) {
            Page p = pageCache.get(i);
            if (p == null) {
                p = FileManager.loadTablePage(this.name, i);
                if (p != null) {
                    addToCache(i, p);
                }
            }
            return p;
        }
        return null;
    }

    public synchronized void insert(String[] record) {
        long startTime = System.currentTimeMillis();
        initCache();
        
        Page current = (pageCount > 0) ? pageCache.get(pageCount-1) : null;
        if (current == null && pageCount > 0) {
            current = FileManager.loadTablePage(this.name, pageCount-1);
            addToCache(pageCount-1, current);
        }

        if (current == null || !current.insert(record)) {
            current = new Page();
            current.insert(record);
            pageCount++;
            addPage(current);
        }

        FileManager.storeTablePage(this.name, pageCount-1, current);
        recordsCount++;
        long stopTime = System.currentTimeMillis();
        this.trace.add("Inserted:"+ Arrays.toString(record)+", at page number:"+(pageCount-1)+", execution time (mil):"+(stopTime - startTime));
    }

    public void addAt(int index, Page p) {
        initCache();
        while (pages.size() <= index) {
            pages.add(null);
        }
        pages.set(index, p);
        addToCache(index, p);
    }

    public String[] fixCond(String[] cols, String[] vals) {
        String[] res = new String[columnsNames.length];
        for(int i=0;i<res.length;i++) {
            for(int j=0;j<cols.length;j++) {
                if(columnsNames[i].equals(cols[j])) {
                    res[i]=vals[j];
                }
            }
        }
        return res;
    }

    public int getPageCount() {
        return this.pageCount;
    }

    public ArrayList<String[]> select(String[] cols, String[] vals) {
        String[] cond = fixCond(cols, vals);
        String tracer ="Select condition:"+Arrays.toString(cols)+"->"+Arrays.toString(vals);
        ArrayList<ArrayList<Integer>> pagesResCount = new ArrayList<ArrayList<Integer>>();
        ArrayList<String[]> res = new ArrayList<String[]>();
        long startTime = System.currentTimeMillis();
        for(int i=0;i<pageCount;i++) {
            Page p = getPage(i);
            ArrayList<String[]> pRes = p.select(cond);
            if(pRes.size()>0) {
                ArrayList<Integer> pr = new ArrayList<Integer>();
                pr.add(i);
                pr.add(pRes.size());
                pagesResCount.add(pr);
                res.addAll(pRes);
            }
        }
        long stopTime = System.currentTimeMillis();
        tracer +=", Records per page:" + pagesResCount+", records:"+res.size()+", execution time (mil):"+(stopTime - startTime);
        this.trace.add(tracer);
        return res;
    }

    public ArrayList<String[]> select(int pageNumber, int recordNumber) {
        String tracer ="Select pointer page:"+pageNumber+", record:"+recordNumber;
        ArrayList<String[]> res = new ArrayList<String[]>();
        long startTime = System.currentTimeMillis();
        Page p = getPage(pageNumber);
        ArrayList<String[]> pRes = p.select(recordNumber);
        if(pRes.size()>0) {
            res.addAll(pRes);
        }
        long stopTime = System.currentTimeMillis();
        tracer+=", total output count:"+res.size()+", execution time (mil):"+(stopTime - startTime);
        this.trace.add(tracer);
        return res;
    }

    public ArrayList<String[]> select() {
        ArrayList<String[]> res = new ArrayList<>();
        long startTime = System.currentTimeMillis();
        for(int i = 0; i < pageCount; i++) {
            Page p = getPage(i);
            if(p != null) {
                ArrayList<String[]> pageRecords = p.select();
                if(pageRecords != null) {
                    res.addAll(pageRecords);
                }
            }
        }
        long stopTime = System.currentTimeMillis();
        this.trace.add("Select all pages:" + pageCount + ", records:" + recordsCount + ", execution time (mil):" + (stopTime - startTime));
        return res;
    }

    public String getFullTrace() {
        StringBuilder sb = new StringBuilder();
        
        // Append all trace entries
        for (String entry : trace) {
            sb.append(entry).append("\n");
        }
        
        // Append basic table info
        sb.append("Pages Count: ").append(pageCount)
          .append(", Records Count: ").append(recordsCount);
        
        // Get all table columns and indexed columns
        String[] allColumns = this.getcolName();
        Set<String> indexedCols = DBApp.indexedColumns.getOrDefault(name, Collections.emptySet());
        
        // Calculate non-indexed columns
        Set<String> nonIndexedCols = new HashSet<>();
        for (String col : allColumns) {
            if (!indexedCols.contains(col)) {
                nonIndexedCols.add(col);
            }
        }
        
        // Append column information
      
			if(!indexedCols.isEmpty()) {
        	 List<String> sortedIndexed = new ArrayList<>(indexedCols);
     	    Collections.sort(sortedIndexed);
            sb.append(", Indexed Columns: ").append(sortedIndexed);
			}
			else {
				indexedCols = new HashSet<>();
				 DBApp.indexedColumns.put(name, indexedCols); 
				sb.append(", Indexed Columns: ").append(indexedCols);
				
			}
        
        
        return sb.toString();
    }

    public String getLastTrace() {
        return this.trace.get(this.trace.size()-1);
    }

    private Object readResolve() {
        if (trace == null) trace = new ArrayList<>();
        if (pages == null) pages = new ArrayList<>();
        if (pageCache == null) pageCache = new HashMap<>();
        return this;
    }
}