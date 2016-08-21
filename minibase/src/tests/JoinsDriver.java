package tests;

import global.*;


import heap.Heapfile;
import heap.Tuple;
import iterator.*;
import tests.IEJoinHeap_v2.IEJoinResultHeap;
import tests.IEJoin_v2.IEJoinResult;
import java.util.Iterator;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

class JoinsDriver implements GlobalConst {
	public static JoinsDriver jd;
	public boolean OK = true;
	public boolean FAIL = false;
	
    public String[] selClause =null;
    public String[] relName =null;
    public String[] prdctOne =null;
    public String[] prdctTwo =null;
    public int noOfSel= 0;
    public int noOfRel = 0;
    public int noOfWhrOne;
    public int noOfWhrTwo;
    public String opxPrdctOne =null;
    public String opxPrdctTwo =null;
    public String connClause = null;
    public int opx1 = 0 ;
    public int opx2 = 0;
    public int numrInstance = 0;
    public int numrInstance_attrs = 0;
    public int numsInstance = 0;
    public int numsInstance_attrs = 0;
    public int col1_Prdt1= 0 ;
    public int col2_Prdt1= 0 ;
    public int proj1 = 0;
    public int proj2 = 0;
    public int col1_Prdt2= 0 ;
    public int col2_Prdt2= 0;
    public int noOfPredicate = 0;
    private static String queryFileName;
    private static String schemaFileName;
    private String projectItem ="";
    String schemaRelName = "";
    HashMap<String, List<String>> predicateLst1 = new HashMap<String, List<String>>();
    HashMap<Integer, String> predicateLst = new HashMap<Integer, String>();
    HashMap<String, List<String>> schemaLst = new HashMap<String, List<String>>();
    HashMap<String, String> relAlias = new HashMap<String, String>();

    public void queryReading(){
        String fileName = queryFileName;

        // This will reference one line at a time
        String line = null;
        int count = 1;
        int prdctCnt = 0;

        try {
        	
        	//Reading Schema
        	String schemaLine ="";
        	FileReader schemaFileReader = new FileReader(schemaFileName);
            BufferedReader schemaReader = new BufferedReader(schemaFileReader);
            while((schemaLine = schemaReader.readLine()) != null) {
            	schemaRelName = ((schemaLine.split(":"))[0]).replaceAll("\\s+","");
            	schemaLst.put(schemaRelName, (Arrays.asList((schemaLine.split(":"))[1].replaceAll("\\s+","").split(","))));
            }
        	
            // FileReader reads text files in the default encoding.
            FileReader fileReader = new FileReader(fileName);

            // Always wrap FileReader in BufferedReader.
            BufferedReader bufferedReader =
            		new BufferedReader(fileReader);
            System.out.println("*** Execution for following Query  :");
            while((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                if((line == null) || (line.equalsIgnoreCase("")))
                	continue;
                if (count == 1)
                {
                    selClause = line.split(",");
                    noOfSel = selClause.length;
                    System.out.println((selClause[0].split("\\s+"))[1]);
                    projectItem = (selClause[0].split("\\s+"))[1];
                    
                }
                else if (count == 2)
                {
                    relName = line.split(",");
                    noOfRel = relName.length;
                    relName[0] = relName[0].substring(relName[0].indexOf(" "));
                    for(int i=0;i<relName.length;i++)
                    {
                    	relName[i] = relName[i].trim();
                    	relAlias.put( (relName[i].split("\\s+"))[1],(relName[i].split("\\s+"))[0]);
                    }
                }
                else if (count == 3)
                {
                	//WHERE r.salary > s.salary AND r.tax < s.tax
                    prdctOne = line.split("AND");
                    prdctOne[0] = prdctOne[0].substring(prdctOne[0].indexOf(" ")+1);
                    noOfWhrOne = prdctOne.length;
                    for(int i=0;i<noOfWhrOne;i++)
                    {          	
                    	prdctOne[i] = prdctOne[i].trim();
                    	predicateLst.put(prdctCnt, prdctOne[i]);     
                    	List<String> tempList = new ArrayList<String>(Arrays.asList(prdctOne[i].split("\\s+")));  
                    	String alias1 = relAlias.get((prdctOne[i].split("\\s+"))[0].substring(0,1));
                    	String alias2 = relAlias.get((prdctOne[i].split("\\s+"))[2].substring(0,1));
                    	String key = alias1 + alias2 + "-" + Integer.toString(prdctCnt);
                    	int r1 = schemaLst.get(alias1).indexOf(tempList.get(0).substring(2)) + 1;
                    	int r2 = schemaLst.get(alias2).indexOf(tempList.get(2).substring(2)) + 1;
                    	
                    	if(tempList.get(1).equals("<"))
                    		tempList.set(1,"1");
                    	if(tempList.get(1).equals(">"))
                    		tempList.set(1,"4");
                    	
                    	tempList.set(0,Integer.toString(r1));
                    	tempList.set(2,Integer.toString(r2));
                    	predicateLst1.put(key, tempList);
                        prdctCnt++;                        	
                    }
                }
                else if (count >= 4)
                {
                	line = line.substring(line.indexOf(" ")+1);
                	prdctOne = line.split("AND");
                	noOfWhrOne = prdctOne.length;
                	for(int i=0;i<noOfWhrOne;i++)
                    {
                		prdctOne[i] = prdctOne[i].trim();
                    	predicateLst.put(prdctCnt, prdctOne[i]);     
                    	List<String> tempList = new ArrayList<String>(Arrays.asList(prdctOne[i].split("\\s+")));  
                    	String alias1 = relAlias.get((prdctOne[i].split("\\s+"))[0].substring(0,1));
                    	String alias2 = relAlias.get((prdctOne[i].split("\\s+"))[2].substring(0,1));
                    	String key = alias1 + alias2 + "-" + Integer.toString(prdctCnt);
                    	int r1 = schemaLst.get(alias1).indexOf((tempList.get(0).substring(2)).replaceAll(";", "")) + 1;
                    	int r2 = schemaLst.get(alias2).indexOf((tempList.get(2).substring(2)).replaceAll(";", "")) + 1;
                    	tempList.set(0,Integer.toString(r1));
                    	tempList.set(2,Integer.toString(r2));
                    	if(tempList.get(1).equals("<"))
                    		tempList.set(1,"1");
                    	if(tempList.get(1).equals(">"))
                    		tempList.set(1,"4");
                    	predicateLst1.put(key, tempList);
                        prdctCnt++;                        	
                    }
                }
                else
                {
                    System.out.println("\n\n **** Error: Passed Query is invalid. ");
                    Runtime.getRuntime().exit(1);
                }
                count++;
            }

            noOfPredicate = prdctCnt-1;
            
            if(noOfPredicate < 0)
            {
                System.out.println("\n\n **** Error: Invalid Number of Predicate. ");
                Runtime.getRuntime().exit(1);
            }

            bufferedReader.close();
            schemaReader.close();
            
        }
        catch(FileNotFoundException ex) {
            System.out.println(
                    "Unable to open file '" +
                            fileName + "'");
        }
        catch(IOException ex) {
            System.out.println(
                    "Error reading file '"
                            + fileName + "'");
        }
    }

  
    /** Constructor
     */
    public JoinsDriver() {

       queryReading();
    }
    
    private static HashMap<Integer, Integer> sortByComparator(HashMap<Integer, Integer> unsortMap) {

		// Convert Map to List
		List<Map.Entry<Integer, Integer>> list = 
			new LinkedList<Map.Entry<Integer, Integer>>(unsortMap.entrySet());

		// Sort list with comparator, to compare the Map values
		Collections.sort(list, new Comparator<Map.Entry<Integer, Integer>>() {
			public int compare(Map.Entry<Integer, Integer> o1,
                                           Map.Entry<Integer, Integer> o2) {
				return (o1.getValue()).compareTo(o2.getValue());
			}
		});

		// Convert sorted map back to a Map
		HashMap<Integer, Integer> sortedMap = new LinkedHashMap<Integer, Integer>();
		for (Iterator<Map.Entry<Integer, Integer>> it = list.iterator(); it.hasNext();) {
			Map.Entry<Integer, Integer> entry = it.next();
			sortedMap.put(entry.getKey(), entry.getValue());
		}
		return sortedMap;
	}
    
    public static void runTest1() 
    {
    	IEJoin_v2 fullRunIEJoin = new IEJoin_v2();
        System.out.print ("\n********************** Sampling starts here !!! **********************\n\n");        
		Map<String, List<String>> sortPrdctMap = new TreeMap<String, List<String>>(jd.predicateLst1);
		HashMap<Integer, Integer> sampleListMap = new HashMap<Integer, Integer>();
		int pcnt = 30;
		// Logic For Sampling !!!!!!
		for (int i=0;i<(int)(sortPrdctMap.size()/2);i++)
		{
			IEJoinResult result  = fullRunIEJoin.IEJoin_Phase4(sortPrdctMap,2*i,jd,pcnt);
			sampleListMap.put(i,result.resultlist.size());
		}
		HashMap<Integer, Integer> sortedsampleListMap = sortByComparator(sampleListMap);
		System.out.print ("\n********************** Sampling ends here !!! **********************\n\n"); 
		String key1,key2;
		Map<String, List<String>> sortPrdctMap1 = new LinkedHashMap<String, List<String>>();
		ArrayList<String> keyList = new ArrayList<>();
		HashMap<Integer,String> keyMap = new HashMap<Integer,String>();
		for (int i=0;i<sortedsampleListMap.size();i++)
		{
			key1 =  (sortPrdctMap.keySet().toArray()[2*((int)sortedsampleListMap.keySet().toArray()[i])]).toString();
			keyList.add(key1);
			keyMap.put(i, key1);
		}
		// Logic For Sampling ENDS !!!!!!
		
		// Logic For Selectivity  Starts !!!!!!
		System.out.print ("\n********************** Selectivity starts here !!! **********************\n\n"); 
		char swp = 'N';
		int g=1;
		for (int i=0;i<keyMap.size();i++)
		{
			if(i != 0)
			{
				String rel1 = keyMap.get(i).substring(0,2);
				String rel2 = keyMap.get(i).substring(2,4);
				int o =0;
				
				while(o < i)
				{
					if((keyMap.get(o).contains(rel1) == true) || (keyMap.get(o).contains(rel2) == true))
					{
						o++;
						g =1;
						//swp = 'Y';
						break;
					}
					if(o==i-1)
					{
						String tmp = keyMap.get(i);
						if(swp == 'Y')
						{
							
							if((i+g) < keyMap.size())
							{
								keyMap.put(i,keyMap.get(i+g));
								keyMap.put(i+g, tmp);
								g++;
							}
						}
						else
						{
							keyMap.put(i, keyMap.get(i+1));
							keyMap.put(i+1, tmp);
							
						}
						swp = 'Y';
						i--;
					}
					o++;
				}
			}
		}
		
		for (int i=0;i<keyMap.size();i++)
		{
			int idx1 = Arrays.asList(sortPrdctMap.keySet().toArray()).indexOf(keyMap.get(i));
			key1 = sortPrdctMap.keySet().toArray()[idx1].toString();
			key2 = sortPrdctMap.keySet().toArray()[idx1+1].toString();
			sortPrdctMap1.put(key1, sortPrdctMap.get(key1));
			sortPrdctMap1.put(key2, sortPrdctMap.get(key2));
		}
		System.out.println(" Query Plan (based on sampling): \n");
		for (int p=0;p<sortPrdctMap1.size();p++)
		{
			System.out.println(sortPrdctMap1.keySet().toArray()[p].toString()+ " - " + sortPrdctMap1.get((sortPrdctMap1.keySet().toArray()[p].toString())));
		}
		// Logic For Selectivity  Ends !!!!!!  ---*********** sortPrdctMap1 ***************
		System.out.print ("\n********************** Sampling ends here !!! **********************\n\n"); 
		int prdctNo =0;
		int offset = 0;
		IEJoinResult result  = fullRunIEJoin.IEJoin_Phase4(sortPrdctMap1,prdctNo,jd,0);
		
		for(int k=0;k<(int)((sortPrdctMap1.size()/2)-1);k++)
		{
			if(result.numberOfCol > 0)
				result  = fullRunIEJoin.IEJoin_Phase41(sortPrdctMap1,(prdctNo + 2*(k+1)),jd,result,calcOffset(sortPrdctMap1,prdctNo+2*k,result.relColl));
		}
		System.out.println("**** Total Record fetched: "+result.resultlist.size());
    }


    public static void runTest2() 
    {
    	IEJoinHeap_v2 fullRunIEJoin = new IEJoinHeap_v2();
        System.out.print ("\n********************** Sampling starts here !!! **********************\n\n");        
		Map<String, List<String>> sortPrdctMap = new TreeMap<String, List<String>>(jd.predicateLst1);
		HashMap<Integer, Integer> sampleListMap = new HashMap<Integer, Integer>();
		int pcnt = 10;
		// Logic For Sampling !!!!!!
		for (int i=0;i<(int)(sortPrdctMap.size()/2);i++)
		{
			IEJoinResultHeap result  = fullRunIEJoin.IEJoin_Phase4(sortPrdctMap,2*i,jd,pcnt);
			sampleListMap.put(i,result.resultlist.size());
		}
		HashMap<Integer, Integer> sortedsampleListMap = sortByComparator(sampleListMap);
		System.out.print ("\n********************** Sampling ends here !!! **********************\n\n"); 
		String key1,key2;
		Map<String, List<String>> sortPrdctMap1 = new LinkedHashMap<String, List<String>>();
		ArrayList<String> keyList = new ArrayList<>();
		HashMap<Integer,String> keyMap = new HashMap<Integer,String>();
		for (int i=0;i<sortedsampleListMap.size();i++)
		{
			key1 =  (sortPrdctMap.keySet().toArray()[2*((int)sortedsampleListMap.keySet().toArray()[i])]).toString();
			keyList.add(key1);
			keyMap.put(i, key1);
		}
		// Logic For Sampling ENDS !!!!!!
		
		// Logic For Selectivity  Starts !!!!!!
		System.out.print ("\n********************** Selectivity starts here !!! **********************\n\n"); 
		char swp = 'N';
		int g=1;
		for (int i=0;i<keyMap.size();i++)
		{
			if(i != 0)
			{
				String rel1 = keyMap.get(i).substring(0,2);
				String rel2 = keyMap.get(i).substring(2,4);
				int o =0;
				
				while(o < i)
				{
					if((keyMap.get(o).contains(rel1) == true) || (keyMap.get(o).contains(rel2) == true))
					{
						o++;
						g =1;
						//swp = 'Y';
						break;
					}
					if(o==i-1)
					{
						String tmp = keyMap.get(i);
						if(swp == 'Y')
						{
							
							if((i+g) < keyMap.size())
							{
								keyMap.put(i,keyMap.get(i+g));
								keyMap.put(i+g, tmp);
								g++;
							}
						}
						else
						{
							keyMap.put(i, keyMap.get(i+1));
							keyMap.put(i+1, tmp);
							
						}
						swp = 'Y';
						i--;
					}
					o++;
				}
			}
		}
		
		for (int i=0;i<keyMap.size();i++)
		{
			int idx1 = Arrays.asList(sortPrdctMap.keySet().toArray()).indexOf(keyMap.get(i));
			key1 = sortPrdctMap.keySet().toArray()[idx1].toString();
			key2 = sortPrdctMap.keySet().toArray()[idx1+1].toString();
			sortPrdctMap1.put(key1, sortPrdctMap.get(key1));
			sortPrdctMap1.put(key2, sortPrdctMap.get(key2));
		}
		System.out.println(" Query Plan (based on sampling): \n");
		for (int p=0;p<sortPrdctMap1.size();p++)
		{
			System.out.println(sortPrdctMap1.keySet().toArray()[p].toString()+ " - " + sortPrdctMap1.get((sortPrdctMap1.keySet().toArray()[p].toString())));
		}
		// Logic For Selectivity  Ends !!!!!!  ---*********** sortPrdctMap1 ***************
		System.out.print ("\n********************** Sampling ends here !!! **********************\n\n"); 
		int prdctNo =0;
		int offset = 0;
		IEJoinResultHeap result  = fullRunIEJoin.IEJoin_Phase4(sortPrdctMap1,prdctNo,jd,0);
		
		for(int k=0;k<(int)((sortPrdctMap1.size()/2)-1);k++)
		{
			if(result.numberOfCol > 0)
				result  = fullRunIEJoin.IEJoin_Phase41(sortPrdctMap1,(prdctNo + 2*(k+1)),jd,result,calcOffset(sortPrdctMap1,prdctNo+2*k,result.relColl));
		}
		System.out.println("**** Total Record fetched: "+result.resultlist.size());
    }


    
    public static int calcOffset(Map<String, List<String>> sortPrdctMap, int prdctNo, String relColl) 
    {
    	String frstRel = sortPrdctMap.keySet().toArray()[prdctNo+2].toString().substring(0,2);
    	String secondRel = sortPrdctMap.keySet().toArray()[prdctNo+2].toString().substring(2,4);
    	String keyRel = "";
    	keyRel = relColl.contains(frstRel)?frstRel:secondRel;
    	int pos = relColl.indexOf(keyRel);
    	if(pos == 0)
    		return 0;
    	int sizeOff = 0;
    	for (int j=0;j<pos/2;j++)
    	{
    		String currRel = relColl.substring(2*j, 2*(j+1));
			sizeOff = sizeOff + jd.schemaLst.get(currRel).size();
    	}
		return sizeOff;
    }

    /**
     * Runs a join query based on the input
     * @param argv {data_file_name, query_file_name, (1a|1b|2a|2b|2c|2d)}
     * @throws IOException 
     */
    public static void main(String argv[]) throws IOException{
    	System.out.println("Please provide data Schema File name, Query file name and Implementation Type [1 for Array Based and 2 for Heapfile Based]");
        if(argv.length<2){
            System.out.println("Please provide data Schema File name, Query file name and Implementation Type [1 for Array Based and 2 for Heapfile Based]");
            return;
        }
        queryFileName = argv[1];
        schemaFileName = argv[0];
        jd = new JoinsDriver();

        String queryType = argv[2];
        
        if(queryType.equals("1"))
        {
        	runTest1();
        }
        else if(queryType.equals("2"))
        {
        	runTest2();
        }


    }
}