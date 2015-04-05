package edu.pitt.sis.infsci2711.social;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class sortFriend {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		HashMap<String,Integer> map = new HashMap<String,Integer>();
        ValueComparator bvc =  new ValueComparator(map);
        TreeMap<String,Integer> sorted_map = new TreeMap<String,Integer>(bvc);

        BufferedReader input = new BufferedReader(new InputStreamReader(new FileInputStream("output/part-r-00000")));
 	   
 	   String str = input.readLine();
 	   while(str != null){
 		   String[] temp = str.split("\t");
 		   String pair = temp[0];
 		   Integer num = Integer.parseInt(temp[1]);
 		   map.put(pair, num);
 		   str = input.readLine();
 	   }

        System.out.println("unsorted map: "+map);

        sorted_map.putAll(map);

        System.out.println("results: "+sorted_map);
        
        Iterator it = sorted_map.entrySet().iterator();
        BufferedWriter output = new BufferedWriter(new OutputStreamWriter (new FileOutputStream("output/result.txt")));
        while(it.hasNext()){
        	Map.Entry<String, Integer> pair = (Map.Entry<String, Integer>)it.next();
        	output.write(pair.getKey() + "\t" +pair.getValue());
        	output.write("\n");
        	it.remove();
        }
        output.close();
    }
}

class ValueComparator implements Comparator<String> {

    Map<String, Integer> base;
    public ValueComparator(Map<String, Integer> base) {
        this.base = base;
    }

    public int compare(String a, String b) {
        if (base.get(a) >= base.get(b)) {
            return -1;
        } else {
            return 1;
        } // returning 0 would merge keys
    }
}
