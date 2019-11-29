package project1;

import java.util.ArrayList;
import java.util.Scanner;
import java.util.List;

public class test {
	public static String aaa;
	public static List<String> allFile = new ArrayList<String>();;
	
	public static void testa() {
		System.out.println(aaa);
		
	}
	public static void addfile() {
		System.out.println(allFile.size());
		allFile.add("---------");
		System.out.println(allFile.size());
	}

	public static void main(String[] args) {
		aaa = "100";
//		testa();
//		addfile();
//		for(int m = 0; m < allFile.size(); m++) {
//			System.out.println(allFile.get(m));
//		}
		System.out.println(aaa.substring(0));

	}

}
