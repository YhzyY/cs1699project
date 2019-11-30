package project1;
import java.util.logging.Logger;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.List;

public class test {
	
	public static String aaa;
	public static List<String> allFile = new ArrayList<String>();
	static String strClassName = test.class.getName();  
	static Logger logger = Logger.getLogger(strClassName); 
//	private static final Logger logger = Logger.getLogger(test.class);

	
	public static void testa() {
		System.out.println(aaa);
		
	}
	public static void addfile() {
		logger.info("this is the info");
		System.out.println(allFile.size());
		allFile.add("---------");
		System.out.println(allFile.size());
	}

	public static void main(String[] args) throws IOException {
//		
//		logger.warning("this is the aaa" + aaa);
//		aaa = "100";
//		testa();
//		addfile();
//		for(int m = 0; m < allFile.size(); m++) {
//			System.out.println(allFile.get(m));
//		}
//		System.out.println(aaa.substring(0));
		long startTime = System.currentTimeMillis(); //程序开始记录时间
        File f = new File("/Users/ziyi/document/cs1699/project/logoftest.txt");
        FileOutputStream fop = new FileOutputStream(f);
        OutputStreamWriter writer = new OutputStreamWriter(fop, "UTF-8");
        
		
		//。。。 。。。
		long endTime   = System.currentTimeMillis(); //程序结束记录时间
		long TotalTime = endTime - startTime; 
		writer.append("English\r\n");
//		writer.append("\r\n");
		writer.append(TotalTime+"\r\n");
		writer.close();
		fop.close();
		
		String term = " ".split("\\s+")[0];
		System.out.println(term);
	}

}
