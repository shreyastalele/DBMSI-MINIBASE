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

class IEJoin_v2 extends TestDriver implements GlobalConst {
	private static final int NUM_PAGES = 30;
	private static final String doublePredOutput = "double_pred_output.txt";
	private int sizeLeft ;
	private int sizeRight;
	private long recCount = 0;
	private int predicateNo = 0;
	ArrayList<String> ROrg_1 = new ArrayList<String>();
	ArrayList<String> SOrg_1 = new ArrayList<String>();
	ArrayList<String> ROrg = new ArrayList<String>();
	ArrayList<String> SOrg = new ArrayList<String>();
	String filename1,filename2;

	public IEJoinResult IEJoin_Phase4(Map<String, List<String>> sortPrdctMap,int  prdctNo, JoinsDriver jd,int samplePcnt){
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
		int leftp1 =  Integer.parseInt(sortPrdctMap.get(firstKey).get(0));
	    int rightp1 =  Integer.parseInt(sortPrdctMap.get(firstKey).get(2));
	    int leftp2 =  Integer.parseInt(sortPrdctMap.get(secondKey).get(0));
	    int rightp2 =  Integer.parseInt(sortPrdctMap.get(secondKey).get(2));
	    int leftOp =  Integer.parseInt(sortPrdctMap.get(firstKey).get(1));
	    int rightOp =  Integer.parseInt(sortPrdctMap.get(secondKey).get(1));
		try
		{
			if(samplePcnt > 0 )
			{
				LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(LeftRel+".csv")));
				lnr.skip(Long.MAX_VALUE);
				LeftRowCount = lnr.getLineNumber() + 1;
				LeftMaxRecs = (LeftRowCount * samplePcnt)/100 ;
				ROrg = new ArrayList<String>();
			}
			
			sizeLeft = jd.schemaLst.get(LeftRel).size();
			FileReader fr = new FileReader(LeftRel+".csv");
	        BufferedReader br = new BufferedReader(fr);
	        ROrg_1 = new ArrayList<String>();
	        if(samplePcnt > 0 ){
	        	int recNo = 0;
	        	while(recNo<LeftRowCount){
		            String line = br.readLine();
		            if(line!=null){
		                String[] fields = line.split(",");
		                if(fields.length < sizeLeft){
		                    continue;
		                }
	                	ROrg.add(line);
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
		                ROrg_1.add(line);
		            }else{
		                break;
		            }
		        }
	        }

	        if(samplePcnt > 0 )
	        {
	        	Collections.shuffle(ROrg);
	        	for(int y=0;y<LeftMaxRecs;y++)
	        	{
	        		ROrg_1.add(ROrg.get(y));
	        	}
			}
	        br.close();
	        fr.close();
	        
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
        	if(samplePcnt > 0 )
			{
				LineNumberReader  lnr = new LineNumberReader(new FileReader(new File(RightRel+".csv")));
				lnr.skip(Long.MAX_VALUE);
				RightRowCount = lnr.getLineNumber() + 1;
				RightMaxRecs = (RightRowCount * samplePcnt)/100 ;
				SOrg = new ArrayList<String>();
			}
        	sizeRight = jd.schemaLst.get(RightRel).size(); //4
        	FileReader fr = new FileReader(RightRel+".csv");
        	BufferedReader br = new BufferedReader(fr);
	        SOrg_1 = new ArrayList<String>();
	        if(samplePcnt > 0 ){
	        	int recNo = 0;
		        while(recNo<RightRowCount){
		            String line = br.readLine();
		            if(line!=null){
		                String[] fields = line.split(",");
		                if(fields.length < sizeRight){
		                    continue;
		                }
	                	SOrg.add(line);
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
		                SOrg_1.add(line);
		            }else{
		                break;
		            }
		        }
	        }
	        if(samplePcnt > 0 )
	        {
	        	Collections.shuffle(SOrg);
	        	for(int y=0;y<RightMaxRecs;y++)
	        	{
	        		SOrg_1.add(SOrg.get(y));
	        	}
			}	        
	        br.close();
	        fr.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        IEJoinResult result = ieJoin(new int[]{1,1},new int[]{leftp1,rightp1,leftp2,rightp2}, new int[]{leftOp,rightOp});
        result.relColl = LeftRel + RightRel;
        return result;
}

	public IEJoinResult IEJoin_Phase41(Map<String, List<String>> sortPrdctMap,int  prdctNo, JoinsDriver jd,IEJoinResult input,int offset){
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
		try{
			sizeLeft = input.numberOfCol;
			ROrg_1 = new ArrayList<String>();
			ROrg_1 = input.resultlist;
		}catch (Exception e)
		{
			e.printStackTrace();
		}

		try {
        	sizeRight = jd.schemaLst.get(RightRel).size(); //4
        	FileReader fr = new FileReader(RightRel+".csv");
        	BufferedReader br = new BufferedReader(fr);
        SOrg_1 = new ArrayList<String>();
        while(true){
            String line = br.readLine();
            if(line!=null){
                String[] fields = line.split(",");
                if(fields.length < sizeRight){
                    continue;
                }
                SOrg_1.add(line);
            }else{
                break;
            }
        }
        br.close();
        fr.close();
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

        IEJoinResult result = ieJoin(new int[]{1,1},new int[]{leftp1+offset,rightp1,leftp2+offset,rightp2}, new int[]{leftOp,rightOp});
        result.relColl = input.relColl + RightRel;
        return result;
}

	private IEJoinResult ieJoin(int[] outputColumns, int[] predicateColumns, int[] operator){
	IEJoinResult result = new IEJoinResult();
	try {

		ArrayList<Integer> l1 = new ArrayList<Integer>();
		ArrayList<Integer> l2 = new ArrayList<Integer>();
		ArrayList<Integer> l1_ = new ArrayList<Integer>();
		ArrayList<Integer> l2_ = new ArrayList<Integer>();

		for(String line : ROrg_1)
			{
				String[] fields = line.split(",");
				l1.add(Integer.parseInt(fields[predicateColumns[0]-1]));
				l2.add(Integer.parseInt(fields[predicateColumns[2]-1]));
			}

		for(String line : SOrg_1)
			{
				String[] fields = line.split(",");
				l1_.add(Integer.parseInt(fields[predicateColumns[1]-1]));
				l2_.add(Integer.parseInt(fields[predicateColumns[3]-1]));
			}

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
		result =  IeJoin(o1,o2,P,P_,l1,l2,l1_,l2_,left,right);

	} catch (Exception e) {
		e.printStackTrace();
	}
	return result;
}

	public class IEJoinResult {
    int numberOfCol ;
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

	IEJoinResult IeJoin(ArrayList<Integer> offsetList1,ArrayList<Integer> offsetList2,ArrayList<Integer> permList,ArrayList<Integer> permList__,ArrayList<Integer> l1,ArrayList<Integer> l2,ArrayList<Integer> l1__,ArrayList<Integer> l2__,ArrayList<Integer> left1,ArrayList<Integer> right1)
	{
		IEJoinResult result = new IEJoinResult();
		try {
			result.resultlist = new ArrayList<String>();
			ArrayList<Integer> base_ = new ArrayList<Integer>();
			for(int i=0;i<l1__.size();i++){
				base_.add(-1);
			}
	        int indxStart = base_.size()+1;
	        int indxEnd = -1;
	        int presVal = 0;
	        int left = 0,off2;
			for(int i=0;i<l2.size();i++){
				off2 = offsetList2.get(i);
				if(off2>=2){
	    		for(int j=0;j<=off2-2;j++){
	    			presVal = permList__.get(j);
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
	    						left = z;
	    						break;
	    					}
	    				}
	    				base_.set(presVal-1, base_.get(left-1));
	    				base_.set(left-1, presVal);
	    				}
	    			}
	    		}
	    	}
	    	int offsetVal1 = offsetList1.get(permList.get(i)-1),base;
	    	if(offsetVal1<indxStart)
	    		base = indxStart;
	    	else
	    		base = offsetVal1;
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
	         String left2 = ROrg_1.get(left1.get(i)-1);
	         String right2 = SOrg_1.get(right1.get(base-1)-1);
	         result.numberOfCol = sizeLeft+sizeRight;
	         result.resultlist.add(left2.concat(",").concat(right2));
	  		   base = base_.get(base-1);
	    	}
	    }
			}  catch (Exception e) {
				e.printStackTrace();
			}

	return result;
	}

}

