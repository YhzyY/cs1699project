package project1;

import java.util.Scanner;

public class test {
	public static String aaa;
	
	public static void testa() {
		System.out.println(aaa);
		
	}

	public static void main(String[] args) {
		aaa = "100";
//		testa();

//List list = [48525	the, 33083	and, 26621	to, 23597	of, 16201	a, 14645	he, 14300	in, 13115	that, 12768	his, 12639	was];
		Scanner myObj = new Scanner(System.in); 
		while (true) {
		    System.out.println("Type \"search\" to search a term; Type \"TopN\" to find topN term; type \"exit\" to exit the system");
		    String command = myObj.nextLine();  // Read user input
		    if(command.contentEquals("search")) {
		    	System.out.println("the word you want to search:");
		    	String searchTerm = myObj.nextLine();
//		    	System.out.println(termMap.get(searchTerm));
		    	System.out.println(searchTerm);
		    }else if(command.contentEquals("topN")) {
		    	System.out.println("the value of N:");
		    	int N = Integer.parseInt(myObj.nextLine());
//		    	System.out.println(topN.subList(0, N));
		    	System.out.println(N);
		    }else if(command.contentEquals("exit")) {
		    	break;
		    }else {
		    	System.out.println("else");
		    	continue;
		    }
		}
		myObj.close();

	}

}
