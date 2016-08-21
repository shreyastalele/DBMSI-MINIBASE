package tests;

import btree.BTreeFile;
import bufmgr.PageNotReadException;
import global.*;
import heap.*;
import iterator.CondExpr;
import iterator.FileScan;
import iterator.FileScanException;
import iterator.FldSpec;
import iterator.InvalidRelation;
import iterator.JoinsException;
import iterator.PredEvalException;
import iterator.RelSpec;
import iterator.Sort;
import iterator.TupleUtilsException;
import iterator.UnknowAttrType;
import iterator.WrongPermat;

import javax.swing.plaf.synth.SynthEditorPaneUI;

import java.awt.geom.RoundRectangle2D;
import java.io.*;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.*;

class IEJoinHeap_v2 extends TestDriver implements GlobalConst {
	private static final int NUM_PAGES = 30;

	private Heapfile inputHeapFile;
	private AttrType[] inputRAttrTypes;
	private AttrType[] inputSAttrTypes;
	private static final String doublePredOutput = "double_pred_output.txt";
	private int sizeLeft ;
	private int sizeRight;
	private long recCount = 0;
	private int predicateNo = 0;
	ArrayList<String> ROrg = new ArrayList<String>();
	ArrayList<String> SOrg = new ArrayList<String>();
	String filename1,filename2;

	public IEJoinResultHeap IEJoin_Phase4(Map<String, List<String>> sortPrdctMap,int  prdctNo, JoinsDriver jd,int samplePcnt){
		Heapfile heapS = null;
		Heapfile heapR = null;
		dbpath = System.getProperty("user.name")+".minibase_3-db";
		logpath = System.getProperty("user.name")+".minibase_3-log";
		SystemDefs sysdef = new SystemDefs( dbpath, 30000, NUMBUF, "Clock" );
		String firstKey = (sortPrdctMap.keySet().toArray()[prdctNo]).toString();
		String secondKey = (sortPrdctMap.keySet().toArray()[prdctNo+1]).toString();
		String LeftRel = (sortPrdctMap.keySet().toArray()[prdctNo]).toString().substring(0,2);
		String RightRel =  (sortPrdctMap.keySet().toArray()[prdctNo]).toString().substring(2,4);
		int LeftRowCount = 0;
		int LeftMaxRecs= 0;
		int RightRowCount= 0;
		int RightMaxRecs= 0;

		filename1 = "file1.in";
		filename2 = "file2.in";
		try
		{
			if(samplePcnt > 0 )
			{
				LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(LeftRel+".csv")));
				lnr.skip(Long.MAX_VALUE);
				LeftRowCount = lnr.getLineNumber() + 1;
				LeftMaxRecs = (LeftRowCount * samplePcnt)/100 ;
			}
			sizeLeft = jd.schemaLst.get(LeftRel).size();
			FileReader fr = new FileReader(LeftRel+".csv");
	        BufferedReader br = new BufferedReader(fr);
            heapR = new Heapfile(filename1);
            Tuple t = new Tuple();
            inputRAttrTypes= new AttrType[sizeLeft];
            for(int i=0;i<inputRAttrTypes.length;i++){
            	inputRAttrTypes[i] = new AttrType(AttrType.attrInteger);
            }
	        t.setHdr((short) sizeLeft, inputRAttrTypes, null);
	        ROrg = new ArrayList<String>();
	        if(samplePcnt > 0 ){
	        	int recNo = 0;
	        	while(recNo<LeftMaxRecs){
		            String line = br.readLine();
		            if(line!=null){
		                String[] fields = line.split(",");
		                if(fields.length < sizeLeft){
		                    continue;
		                }
		                for(int i=0; i<fields.length; i++){
		                    t.setIntFld(i+1,Integer.parseInt(fields[i]));
		                }
		                ROrg.add(line);
		                heapR.insertRecord(t.getTupleByteArray());
		                recNo++;
		            }else{
		                break;
		            }
		        }
	        }
	        else
	        {
	        	while(true){
		            String line = br.readLine();
		            if(line!=null){
		                String[] fields = line.split(",");
		                if(fields.length < sizeLeft){
		                    continue;
		                }
		                for(int i=0; i<fields.length; i++){
		                    t.setIntFld(i+1,Integer.parseInt(fields[i]));
		                }
		                ROrg.add(line);
		                heapR.insertRecord(t.getTupleByteArray());
		            }else{
		                break;
		            }
		        }
	        }
	        br.close();
	        fr.close();
	//        System.out.println("**Left Relation Size: " + heapR.getRecCnt());
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
        	if(samplePcnt > 0 )
			{
				LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(RightRel+".csv")));
				lnr.skip(Long.MAX_VALUE);
				RightRowCount = lnr.getLineNumber() + 1;
				RightMaxRecs = (LeftRowCount * samplePcnt)/100 ;
			}
        	sizeRight = jd.schemaLst.get(RightRel).size(); //4
        	FileReader fr = new FileReader(RightRel+".csv");
			BufferedReader br = new BufferedReader(fr);
            heapS = new Heapfile(filename2);
            Tuple t = new Tuple();
            inputSAttrTypes= new AttrType[sizeRight];
            for(int i=0;i<inputSAttrTypes.length;i++){
            	inputSAttrTypes[i] = new AttrType(AttrType.attrInteger);
            }
	        t.setHdr((short) sizeRight, inputSAttrTypes, null);
	        SOrg = new ArrayList<String>();
	        if(samplePcnt > 0 ){
	        	int recNo = 0;
		        while(recNo<RightMaxRecs){
		            String line = br.readLine();
		            if(line!=null){
		                String[] fields = line.split(",");
		                if(fields.length < sizeRight){
		                    continue;
		                }
		                for(int i=0; i<fields.length; i++){
		                    t.setIntFld(i+1,Integer.parseInt(fields[i]));
		                }
		                SOrg.add(line);
		                heapS.insertRecord(t.getTupleByteArray());
		                recNo++;
		            }else{
		                break;
		            }
		        }
	        }
	        else
	        {
	        	while(true){
		            String line = br.readLine();
		            if(line!=null){
		                String[] fields = line.split(",");
		                if(fields.length < sizeRight){
		                    continue;
		                }
		                for(int i=0; i<fields.length; i++){
		                    t.setIntFld(i+1,Integer.parseInt(fields[i]));
		                }
		                SOrg.add(line);
		                heapS.insertRecord(t.getTupleByteArray());
		            }else{
		                break;
		            }
		        }
	        }
	        br.close();
	        fr.close();
//        System.out.println("Right Relation Size: " + heapS.getRecCnt());
        } catch (Exception e) {
            e.printStackTrace();
        }
        int leftp1 =  Integer.parseInt(sortPrdctMap.get(firstKey).get(0));
        int rightp1 =  Integer.parseInt(sortPrdctMap.get(firstKey).get(2));
        int leftp2 =  Integer.parseInt(sortPrdctMap.get(secondKey).get(0));
        int rightp2 =  Integer.parseInt(sortPrdctMap.get(secondKey).get(2));
        int leftOp =  Integer.parseInt(sortPrdctMap.get(firstKey).get(1));
        int rightOp =  Integer.parseInt(sortPrdctMap.get(secondKey).get(1));
        IEJoinResultHeap result = ieJoin(new int[]{1,1},new int[]{leftp1,rightp1,leftp2,rightp2}, new int[]{leftOp,rightOp}, heapR, heapS);
        result.relColl = LeftRel + RightRel;
        return result;
        //        ieJoin(new int[]{1,1},new int[]{2,3,2,3}, new int[]{2,2}, heapR, heapS);
        //2,3,4,5 - R_2,S_3,R_4,S_5   1 : < 2: <= 3: >= 4:>
}

	public IEJoinResultHeap IEJoin_Phase41(Map<String, List<String>> sortPrdctMap,int  prdctNo, JoinsDriver jd,IEJoinResultHeap input,int offset){
		Heapfile heapS = null;
		Heapfile heapR = null;
		dbpath = System.getProperty("user.name")+".minibase_3-db";
		logpath = System.getProperty("user.name")+".minibase_3-log";
		SystemDefs sysdef = new SystemDefs( dbpath, 30000, NUMBUF, "Clock" );
		String firstKey = (sortPrdctMap.keySet().toArray()[prdctNo]).toString();
		String secondKey = (sortPrdctMap.keySet().toArray()[prdctNo+1]).toString();
		String RightRel =  (sortPrdctMap.keySet().toArray()[prdctNo]).toString().substring(2,4);
		char switchRel = 'N';
		if(input.relColl.contains(RightRel))
		{
			RightRel = (sortPrdctMap.keySet().toArray()[prdctNo]).toString().substring(0,2);
			switchRel = 'Y';
		}

		filename1=input.fileName;

		try{
			heapR = input.heapfile;

			sizeLeft = input.numberOfCol;
			ROrg = new ArrayList<String>();
			ROrg = input.resultlist;
			heapR = new Heapfile(filename1);
	        Tuple t = new Tuple();
	        inputRAttrTypes= new AttrType[sizeLeft];
	        for(int i=0;i<inputRAttrTypes.length;i++)
	        	inputRAttrTypes[i] = new AttrType(AttrType.attrInteger);
	        t.setHdr((short) sizeLeft, inputRAttrTypes, null);
			for(int k =0;k<ROrg.size();k++)
			{
				String[] rArray = ROrg.get(k).split(",");
				for(int i=0; i<sizeLeft; i++){
	                t.setIntFld(i+1,Integer.parseInt(rArray[i]));
	            }
				heapR.insertRecord(t.getTupleByteArray());
			}
		}catch (Exception e)
		{
			e.printStackTrace();
		}

		try {
            //Load the table S from file to the memory
        	//sizeRight = jd.schemaLst.get(jd.predicateLst1.get(1).get(4)).size(); //4
        	sizeRight = jd.schemaLst.get(RightRel).size(); //4
        	//sizeRight = 4;
        	FileReader fr = new FileReader(RightRel+".csv");
			BufferedReader br = new BufferedReader(fr);
            heapS = new Heapfile(filename2);
            Tuple t = new Tuple();
            inputSAttrTypes= new AttrType[sizeRight];
            for(int i=0;i<inputSAttrTypes.length;i++){
            	inputSAttrTypes[i] = new AttrType(AttrType.attrInteger);
        }
        t.setHdr((short) sizeRight, inputSAttrTypes, null);
        SOrg = new ArrayList<String>();
        while(true){
            String line = br.readLine();
            if(line!=null){
                String[] fields = line.split(",");
                if(fields.length < sizeRight){
                    continue;
                }
                for(int i=0; i<fields.length; i++){
                    t.setIntFld(i+1,Integer.parseInt(fields[i]));
                }
                SOrg.add(line);
                heapS.insertRecord(t.getTupleByteArray());
            }else{
                break;
            }
        }
        br.close();
        fr.close();
//        System.out.println("Right Relation Size: " + heapS.getRecCnt());
        } catch (Exception e) {
            e.printStackTrace();
        }
		int leftp1 ;
		int rightp1;
		int leftp2 ;
		int rightp2;
		int leftOp ;
		int rightOp;

		if(switchRel == 'N')
		{
	        leftp1 =  Integer.parseInt(sortPrdctMap.get(firstKey).get(0));
	        rightp1 =  Integer.parseInt(sortPrdctMap.get(firstKey).get(2));
	        leftp2 =  Integer.parseInt(sortPrdctMap.get(secondKey).get(0));
	        rightp2 =  Integer.parseInt(sortPrdctMap.get(secondKey).get(2));
	        leftOp =  Integer.parseInt(sortPrdctMap.get(firstKey).get(1));
	        rightOp =  Integer.parseInt(sortPrdctMap.get(secondKey).get(1));
		}
		else
		{
			rightp1 =  Integer.parseInt(sortPrdctMap.get(firstKey).get(0));	//leftp1
	        leftp1 =  Integer.parseInt(sortPrdctMap.get(firstKey).get(2)); //rightp1
	        rightp2 =  Integer.parseInt(sortPrdctMap.get(secondKey).get(0)); //leftp2
	        leftp2 =  Integer.parseInt(sortPrdctMap.get(secondKey).get(2));//rightp2
	        leftOp =  Integer.parseInt(sortPrdctMap.get(firstKey).get(1)); //leftOp
	        rightOp =  Integer.parseInt(sortPrdctMap.get(secondKey).get(1)); //rightOp
	        leftOp = (leftOp == 1)?4:1;
	        rightOp = (rightOp == 1)?4:1;
		}
        IEJoinResultHeap result = ieJoin(new int[]{1,1},new int[]{leftp1+offset,rightp1,leftp2+offset,rightp2}, new int[]{leftOp,rightOp}, heapR, heapS);
        result.relColl = input.relColl + RightRel;
        return result;
        //        ieJoin(new int[]{1,1},new int[]{2,3,2,3}, new int[]{2,2}, heapR, heapS);
        //2,3,4,5 - R_2,S_3,R_4,S_5   1 : < 2: <= 3: >= 4:>
}

private IEJoinResultHeap ieJoin(int[] outputColumns, int[] predicateColumns, int[] operator, Heapfile heapR, Heapfile heapS){
	IEJoinResultHeap result = new IEJoinResultHeap();
	try {

		FldSpec [] Sprojection = new FldSpec[sizeRight];
		for(int i=0;i<sizeRight;i++){
			Sprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
		}
		FldSpec [] Rprojection = new FldSpec[sizeLeft];
		for(int i=0;i<sizeLeft;i++){
			Rprojection[i] = new FldSpec(new RelSpec(RelSpec.outer), i+1);
		}
		FileScan fscanR = new FileScan(filename1, inputRAttrTypes, null, (short)inputRAttrTypes.length,
					Rprojection.length, Rprojection , null);
		FileScan fscanS = new FileScan(filename2, inputSAttrTypes, null, (short)inputSAttrTypes.length,
					Sprojection.length, Sprojection, null);
		ArrayList<Integer> l1 = new ArrayList<Integer>();
		ArrayList<Integer> l2 = new ArrayList<Integer>();
		ArrayList<Integer> l1_ = new ArrayList<Integer>();
		ArrayList<Integer> l2_ = new ArrayList<Integer>();
		Tuple rec12 = null;
		try {
			rec12 = fscanR.get_next();
		} catch (Exception e2) {
			e2.printStackTrace();
		}
		while(rec12!= null) {
		try {
			l1.add(rec12.getIntFld(predicateColumns[0]));
			l2.add(rec12.getIntFld(predicateColumns[2]));
			rec12 = fscanR.get_next();
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		}
		fscanR.close();

		rec12 = null;
		try {
			rec12 = fscanS.get_next();
		} catch (Exception e2) {
			e2.printStackTrace();
		}
		while(rec12!= null) {
		try {
				l1_.add(rec12.getIntFld(predicateColumns[1]));
				l2_.add(rec12.getIntFld(predicateColumns[3]));
				rec12 = fscanS.get_next();
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		fscanS.close();
		ArrayList<Integer> P = new ArrayList<Integer>();
		ArrayList<Integer> P_ = new ArrayList<Integer>();
		ArrayList<Integer> o1 = new ArrayList<Integer>();
		ArrayList<Integer> o2 = new ArrayList<Integer>();
		ArrayList<Integer> left = new ArrayList<Integer>();
		ArrayList<Integer> right = new ArrayList<Integer>();

		int decide = 0;
		P = GetP(l1,l2,operator,left,right,decide);
		decide =1;
		P_ = GetP(l1_,l2_,operator,left,right,decide);
		GetO(l1,l2,l1_,l2_,operator,o1,o2);
		long startTime = System.nanoTime();
		result =  IeJoin(o1,o2,P,P_,l1,l2,l1_,l2_,left,right);
		long endTime = System.nanoTime();
		long elapsedTime = (endTime - startTime)/1000000;
//		System.out.println("Execution Completed ");
//		System.out.println("Time elapsed : " + elapsedTime + " ms");

	} catch (Exception e) {
		e.printStackTrace();
	}
	return result;
}

	public class IEJoinResultHeap {
    Heapfile heapfile;
    int numberOfCol ;
    String fileName;
    ArrayList<String> resultlist;
    String relColl;
}
	
	
	ArrayList<Integer> GetP(ArrayList<Integer> l1, ArrayList<Integer> l2,int []inpOpr,ArrayList<Integer> left, ArrayList<Integer> right,int decide)
	{
			ArrayList<Integer> temp = new ArrayList<Integer>();
	    ArrayList<Integer> order= new ArrayList<Integer>();
	    ArrayList<ArrayList<Integer>> maps = new ArrayList<ArrayList<Integer>>();
	    ArrayList<Integer> tmList1= new ArrayList<Integer>();
	    ArrayList<Integer> scratchList1= new ArrayList<Integer>();
	    ArrayList<Integer> resultSet= new ArrayList<Integer>();
	    ArrayList<Integer> tmList2= new ArrayList<Integer>();
	    ArrayList<Integer> scratchList= new ArrayList<Integer>();
	    ArrayList<Integer> l1Index= new ArrayList<Integer>();
	    ArrayList<Integer> l2Index= new ArrayList<Integer>();
	    ArrayList<Integer> nullList= new ArrayList<Integer>();
	    ArrayList<Integer> permArrayList = new ArrayList<Integer>();
		int t1 = 0;int t2 = 0;int countL = 1;int lIndexVal;int arVal;
		int opr1 = inpOpr[0];int orderpos = 0;
	    int last;
	    int op2 = inpOpr[1],b,t;
		order.clear();
		maps.clear();
		tmList1.clear();
		scratchList1.clear();
		resultSet.clear();
		tmList2.clear();
		scratchList.clear();
		temp.clear();
	    temp.clear();
	    int rev = 0;
	    temp.addAll(l1);
	    Collections.sort(l1);
	    if(opr1 ==3 || opr1==4){
	    	Collections.reverse(l1);
	    	rev = 1;
	    }
	    if(rev == 1){
	    	Collections.reverse(l1);
	    }
	    for(int i=0;i<temp.size();i++){
	    	scratchList.add(Collections.binarySearch(l1, temp.get(i))+1);
	    }
	    if(rev == 1){
	    	Collections.reverse(l1);
	    }
	    if(rev == 1){
	    	b=0;
	    	if(scratchList.size()%2==0){
	    		b = scratchList.size()/2;
	    		for(int i =0;i<scratchList.size();i++){
	    			if(scratchList.get(i)>b)
	    				scratchList.set(i, b-scratchList.get(i)+b+1);
	    			else
	    				scratchList.set(i, b+b-scratchList.get(i)+1);
	    		}
	    	}
	    	else{
	    		b = (scratchList.size()/2)+1;
	    		for(int i =0;i<scratchList.size();i++){
	    			if(scratchList.get(i)>b)
	    				scratchList.set(i, b-scratchList.get(i)+b);
	    			else
	    				scratchList.set(i, b+b-scratchList.get(i));
	    		}
	    	}
	    	rev = 0;
	    }
	    for(int i=0;i<scratchList.size();i++){
	    	l1Index.add(0);
	    }
	    for(int i=0;i<scratchList.size();i++){
	    	t = scratchList.get(i)-1;
	    	if(l1Index.get(t) == 0)
	    	l1Index.set(t, i+1);
	    	else
	    		l1Index.set(t, -1);
	    }
	    scratchList.clear();
	    tmList1.clear();
	    tmList2.clear();
	    for(int i=0;i<l1Index.size();i++){
	    	if(l1Index.get(i)==0){
	    		tmList1.add(i);
	    		orderpos ++;
	    	}
	    	if(l1Index.get(i)==-1){
	    		scratchList1.add(l1.get(i));
	    		tmList2.add(i);
	    		order.add(orderpos);
	    		orderpos ++;
	    	}
	    }
	    orderpos=0;
	    nullList.clear();
	    for(int i=0;i<scratchList1.size();i++){
	    	maps.add(new ArrayList<Integer>());
	    }
	    for(int i=0;i<scratchList1.size();i++){
	    	last=0;
	    	b = temp.subList(last, temp.size()).indexOf(scratchList1.get(i));
	    	while(b != -1){
	    		last = last + b;
	    		maps.get(i).add(last);
	    		last ++;
	    		b = temp.subList(last, temp.size()).indexOf(scratchList1.get(i));
	    	}
	    }
	    while(t2<tmList2.size()){
	    	countL = 1;
	    	last=1;
	    	b = tmList2.get(t2);
	    	l1Index.set(b, 0);
	    	tmList1.add(order.get(orderpos), b);
	    	orderpos++;
	    	lIndexVal = 0;
	    	t = l1.get(b);
	    	arVal = temp.subList(lIndexVal, temp.size()).indexOf(t);
				arVal = maps.get(t2).get(last-1);
				countL = arVal;
				last++;
				l1Index.set(tmList1.get(t1),lIndexVal + arVal + 1);
				t1++;
	    	while(last <= maps.get(t2).size()){
	    		lIndexVal =  lIndexVal+ arVal + 1;
	    		arVal = maps.get(t2).get(last-1) - countL - 1;
	    		countL = maps.get(t2).get(last-1);
	    		last++;
	    		l1Index.set(tmList1.get(t1),lIndexVal + arVal + 1);
	    		t1++;
	    	}
	    	t2++;
	   }
	    t1 = 0;t2 =0;countL=1;
	    order.clear();
	    maps.clear();
	    tmList1.clear();
	    scratchList1.clear();
	    resultSet.clear();
	    tmList2.clear();
	    scratchList.clear();
	    temp.clear();
	    temp.clear();
	    rev = 0;
	    temp.addAll(l2);
	    Collections.sort(l2);
	    if(op2==2 || op2==1){
	    	Collections.reverse(l2);
	    	rev = 1;
	    }
	    if(rev == 1){
	    	Collections.reverse(l2);
	    }
	    for(int i=0;i<temp.size();i++){
	    	scratchList.add(Collections.binarySearch(l2, temp.get(i))+1);
	    }
	    if(rev == 1){
	    	Collections.reverse(l2);
	    }
	    if(rev == 1){
	    	b=0;
	    	if(scratchList.size()%2==0){
	    		b = scratchList.size()/2;
	    		for(int i =0;i<scratchList.size();i++){
	    			if(scratchList.get(i)>b)
	    				scratchList.set(i, b-scratchList.get(i)+b+1);
	    			else
	    				scratchList.set(i, b+b-scratchList.get(i)+1);
	    		}
	    	}
	    	else{
	    		b = (scratchList.size()/2)+1;
	    		for(int i =0;i<scratchList.size();i++){
	    			if(scratchList.get(i)>b)
	    				scratchList.set(i, b-scratchList.get(i)+b);
	    			else
	    				scratchList.set(i, b+b-scratchList.get(i));
	    		}
	    	}
	    	rev = 0;
	    }
	     for(int i=0;i<scratchList.size();i++){
	    	l2Index.add(0);
	    }
	    for(int i=0;i<scratchList.size();i++){
	    	t = scratchList.get(i)-1;
	    	if(l2Index.get(t) == 0)
	    	l2Index.set(t, i+1);
	    	else
	    		l2Index.set(t, -1);
	    }
	    scratchList.clear();
	    tmList1.clear();
	    tmList2.clear();
	    orderpos = 0;
	    for(int i=0;i<l2Index.size();i++){
	    	if(l2Index.get(i)==0){
	    		tmList1.add(i);
	    		orderpos ++;
	    	}
	    	if(l2Index.get(i)==-1){
	    		scratchList1.add(l2.get(i));
	    		tmList2.add(i);
	    		order.add(orderpos);
	    		orderpos ++;
	    	}
	    }
	    orderpos=0;
 	    nullList.clear();
	    for(int i=0;i<scratchList1.size();i++){
	    	maps.add(new ArrayList<Integer>());
	    }
	    for(int i=0;i<scratchList1.size();i++){
	    	last=0;
	    	b = temp.subList(last, temp.size()).indexOf(scratchList1.get(i));
	    	while(b != -1){
	    		last = last + b;
	    		maps.get(i).add(last);
	    		last ++;
	    		b = temp.subList(last, temp.size()).indexOf(scratchList1.get(i));
	    	}
	    }
	    while(t2<tmList2.size()){
		    	countL = 1;
		    	last=1;
		    	b = tmList2.get(t2);
		    	l2Index.set(b, 0);
		    	tmList1.add(order.get(orderpos), b);
		    	orderpos++;
		    	lIndexVal = 0;
		    	t = l2.get(b);
		    	arVal = temp.subList(lIndexVal, temp.size()).indexOf(t);
					arVal = maps.get(t2).get(last-1);
					countL = arVal;
					last++;
					l2Index.set(tmList1.get(t1),lIndexVal + arVal + 1);
					t1++;
		    	while(last <= maps.get(t2).size()){

		    		lIndexVal =  lIndexVal+ arVal + 1;
		    		arVal = maps.get(t2).get(last-1) - countL - 1;
		    		countL = maps.get(t2).get(last-1);
		    		last++;
		    		l2Index.set(tmList1.get(t1),lIndexVal + arVal + 1);
		    		t1++;
		    	}
		    	t2++;
		  }
    	temp.clear();
    	scratchList.clear();
			temp.clear();
			tmList1.clear();
			temp.addAll(l1Index);
			Collections.sort(temp);
			for(int i=0;i<l1Index.size();i++){
				scratchList.add(Collections.binarySearch(temp, l1Index.get(i))+1);
			}
			for(int i=0;i<l1Index.size();i++){
				tmList1.add(0);
			}
			for(int i=0;i<scratchList.size();i++){
				tmList1.set(scratchList.get(i)-1, i+1);
			}
			for(int i=0;i<l2Index.size();i++){
				permArrayList.add(tmList1.get(l2Index.get(i)-1));
			}
      if(decide ==0)
      	for(int i = 0;i<l2Index.size();i++)
      		left.add(l2Index.get(i));
      else
      	for(int i = 0;i<l1Index.size();i++)
      		right.add(l1Index.get(i));
	    return permArrayList;
	}

	void GetO(ArrayList<Integer> l1, ArrayList<Integer> l2,ArrayList<Integer> l1__, ArrayList<Integer> l2__,int operator[],ArrayList<Integer> offsetList1, ArrayList<Integer> offsetList2)

	{
		int lineIndx;
		int op1 = operator[0];
		int op2 = operator[1];
		if(op1==2 || op1==3){
			if(op1==2){
				for(int i=0;i<l1.size();i++){
	            	lineIndx = 0;
	            	while(lineIndx<l1__.size() &&l1__.get(lineIndx)<l1.get(i))
	            		lineIndx++;
	            	offsetList1.add(lineIndx+1);
	            }
	   	 	}
	   	 	if(op1==3){
	   	 		for(int i=0;i<l1.size();i++){
	            	lineIndx = 0;
	            	while(lineIndx<l1__.size() &&l1__.get(lineIndx)>l1.get(i))
	            		lineIndx++;
	            	offsetList1.add(lineIndx+1);
	            }
	   	 	}

		}
		if(op1==1 || op1==4){
			if(op1==1){
       		 for(int i=0;i<l1.size();i++){
                	lineIndx = 0;
                	while(lineIndx<l1__.size() &&l1__.get(lineIndx)<=l1.get(i))
                		lineIndx++;
                	offsetList1.add(lineIndx+1);
                }
			}
			if(op1==4){
				for(int i=0;i<l1.size();i++){
					lineIndx = 0;
					while(lineIndx<l1__.size() &&l1__.get(lineIndx)>=l1.get(i))
						lineIndx++;
					offsetList1.add(lineIndx+1);
				}
			}
		}
		if(op2==1 || op2==4){
			if(op2==1){
				for(int i=0;i<l2.size();i++){
					lineIndx = 0;
					while(lineIndx<l2__.size() &&l2__.get(lineIndx)>l2.get(i))
						lineIndx++;
					offsetList2.add(lineIndx+1);
				}
			}
			if(op2==4){
				for(int i=0;i<l2.size();i++){
					lineIndx = 0;
					while(lineIndx<l2__.size() &&l2__.get(lineIndx)<l2.get(i))
						lineIndx++;
					offsetList2.add(lineIndx+1);
				}
			}
		}
		if(op2==2 || op2==3){
			if(op2==2){
				for(int i=0;i<l2.size();i++){
				 	lineIndx = 0;
				   	while(lineIndx<l2__.size() &&l2__.get(lineIndx)>=l2.get(i))
				   		lineIndx++;
				   	offsetList2.add(lineIndx+1);
				}
			}
			if(op2==3){
	      		 for(int i=0;i<l2.size();i++){
	               	lineIndx = 0;
	               	while(lineIndx<l2__.size() &&l2__.get(lineIndx)<=l2.get(i))
	               		lineIndx++;
	               	offsetList2.add(lineIndx+1);
	               }
			}
		}
   }

	IEJoinResultHeap IeJoin(ArrayList<Integer> offset1,ArrayList<Integer> offset2,ArrayList<Integer> permList,ArrayList<Integer> permList_,ArrayList<Integer> l1,ArrayList<Integer> l2,ArrayList<Integer> l1__,ArrayList<Integer> l2__,ArrayList<Integer> left1,ArrayList<Integer> right1)
	{
		IEJoinResultHeap result = new IEJoinResultHeap();
		try {
			SecureRandom random = new SecureRandom();
			result.fileName = new BigInteger(130, random).toString(32);
			result.heapfile = new Heapfile(result.fileName);
			result.resultlist = new ArrayList<String>();
			ArrayList<Integer> base_ = new ArrayList<Integer>();
			for(int i=0;i<l1__.size();i++){
				base_.add(-1);
			}
	        int indxStart = base_.size()+1;
	        int indxEnd = -1;
	        int presVal = 0;
	        int indicLeft = 0,offVal2;
			for(int i=0;i<l2.size();i++){
				offVal2 = offset2.get(i);
				if(offVal2>=2){
	    		for(int j=0;j<=offVal2-2;j++){
	    			presVal = permList_.get(j);
	    			if(base_.get(presVal-1)==-1)
	    			{
	    			if(presVal<indxStart){
	    				base_.set(presVal-1, indxStart);
	    				indxStart = presVal;
	    			}
	    			if(presVal>indxEnd){
	    				if(indxEnd>-1){
	    					base_.set(indxEnd-1, presVal);
	    					indxEnd = presVal;
	    					base_.set(indxEnd-1, base_.size()+1);
	    				}
	    				else{
	    					indxEnd = presVal;
	    				}
	    			}
	    			else if(indxStart<presVal && presVal<indxEnd){
	    				for(int z=presVal-1;z>=indxStart;z--){
	    					if(base_.get(z-1)!=-1){
	    						indicLeft = z;
	    						break;
	    					}
	    				}
	    				base_.set(presVal-1, base_.get(indicLeft-1));
	    				base_.set(indicLeft-1, presVal);
	    				}
	    			}
	    		}
	    	}
	    	int offVal1 = offset1.get(permList.get(i)-1),base;
	    	if(offVal1<indxStart)
	    		base = indxStart;
	    	else
	    		base = offVal1;
	    	if(base<indxEnd){
	    		if(base_.get(base-1)==-1){
	    		for(int z=base+1;z<=indxEnd;z++){
	    			if(base_.get(z-1)!= -1)
	    			{
	    				base=z;
	    				break;
	    			}
	    		}
	    		}
	    	}
	    	while(base<=indxEnd){
	    		recCount++;
	    		//System.out.println("Ans:\t\t"+left1.get(i)+" "+right1.get(b-1));
	    		Tuple t = new Tuple();
	            inputRAttrTypes= new AttrType[sizeLeft+sizeRight];
	            for(int l=0;l<inputRAttrTypes.length;l++){
	            	inputRAttrTypes[l] = new AttrType(AttrType.attrInteger);
	            }
	            t.setHdr((short)(sizeLeft+sizeRight), inputRAttrTypes, null);
	           String left2 = ROrg.get(left1.get(i)-1);
	           String right2 = SOrg.get(right1.get(base-1)-1);
	           String[] fields1 = left2.split(",");
	           String[] fields2 = right2.split(",");
	           int a;
	            // Set the fields of the tuple
	            for( a=0; a<sizeLeft; a++){
	                t.setIntFld(a+1,Integer.parseInt(fields1[a]));
	            }
	            int r = 0;
	            for(; a<sizeRight; a++){
	                t.setIntFld(a+1,Integer.parseInt(fields2[r]));
	                r++;
	            }
	            result.heapfile.insertRecord(t.getTupleByteArray());
	            result.numberOfCol = sizeLeft+sizeRight;
	            result.resultlist.add(left2.concat(",").concat(right2));
	    		base = base_.get(base-1);
	    	}
	    }

		int count = result.heapfile.getRecCnt();
//		System.out.println("----------------------------------------------------"+count);
			}  catch (Exception e) {
				e.printStackTrace();
			}

	return result;
	}

}

