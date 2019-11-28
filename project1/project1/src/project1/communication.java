package project1;

import com.google.api.gax.paging.Page;
import com.google.auth.Credentials;
import com.google.auth.ServiceAccountSigner;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.BatchResult;
import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.Acl;
import com.google.cloud.storage.Acl.Role;
import com.google.cloud.storage.Acl.User;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.CopyWriter;
import com.google.cloud.storage.HmacKey;
import com.google.cloud.storage.HmacKey.HmacKeyMetadata;
import com.google.cloud.storage.HmacKey.HmacKeyState;
import com.google.cloud.storage.HttpMethod;
import com.google.cloud.storage.ServiceAccount;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BucketField;
import com.google.cloud.storage.Storage.BucketGetOption;
import com.google.cloud.storage.Storage.BucketListOption;
import com.google.cloud.storage.Storage.BucketSourceOption;
import com.google.cloud.storage.Storage.ComposeRequest;
import com.google.cloud.storage.Storage.CopyRequest;
import com.google.cloud.storage.Storage.ListHmacKeysOption;
import com.google.cloud.storage.Storage.SignUrlOption;
import com.google.cloud.storage.StorageBatch;
import com.google.cloud.storage.StorageBatchResult;
import com.google.cloud.storage.StorageClass;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class communication {
	
	public static Storage storage;
	public static String bucketName = "dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2";
//	public static String topNFile = "output2/output2.txt";
	public static String indicesFile = "output/part-r-00000";
	public static String topNFile = "output2/part-r-00000";
	
	
	public void StorageSnippets(Storage storage) {
		    this.storage = storage;
	
	}
	public static Page<Bucket> authListBuckets() throws FileNotFoundException, IOException {
		Page<Bucket> buckets = storage.list();
		for (Bucket bucket : buckets.iterateAll()) {
			System.out.println(bucket.toString());
			System.out.println(bucket.getLocation());
			System.out.println(bucket.get("output2/part-r-00000"));
			if((bucket.getLocation()).toString() == "US-WEST2".toString()) {
				bucketName = bucket.getName();
				System.out.println("!!");
			}
	    }
	    // [END auth_cloud_implicit]
	    return buckets;
	}
	
	  public static List readerFromStrings(String bucketName, String blobName) throws IOException {
		  List<String> content = new ArrayList();
		  String tempString;
		  String[] tempArr;
		    // [START readerFromStrings]
		  try (ReadChannel reader = storage.reader(bucketName, blobName)) {
			  ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
		      while (reader.read(bytes) > 0) {
		        bytes.flip();
		        tempString = new String(bytes.array(),StandardCharsets.UTF_8);
		        tempArr = tempString.split("\n");
		        content.addAll(Arrays.asList(tempArr));
		        bytes.clear();
		      }
		      return (content);
		  }
	  }
	  public static Blob getBlobFromId(String bucketName, String blobName) {
		    // [START getBlobFromId]
		    BlobId blobId = BlobId.of(bucketName, blobName);
		    Blob blob = storage.get(blobId);
		    // [END getBlobFromId]
		    return blob;
	  }
	  
	  public static Map<String, String> termMap(List termList){
		Map<String, String> map = new HashMap<String, String>();
		String[] tempPair = {"" , ""};
		int cut;
		String term;
//		System.out.println(termList.size());
		for(int i = 0; i< termList.size(); i++) {
//			System.out.println(termList.get(i).toString());
			cut = termList.get(i).toString().indexOf("files/");
			if(cut == -1) continue;
			tempPair[0] = termList.get(i).toString().substring(0, cut);
			term = tempPair[0].split("\\s+")[0];
			tempPair[1] = termList.get(i).toString().substring(cut);
			if((term != null) & (term != "")) {
				map.put(term, tempPair[1]);
			}
		}
		return map;
	  }

	public static void main(String[] args) throws FileNotFoundException, IOException {
		// If you don't specify credentials when constructing the client, the
		// client library will look for credentials in the environment.
		Credentials credentials = GoogleCredentials.fromStream(new FileInputStream("/Users/ziyi/document/gcloud/MyFirstProject-03009f1d294c.json"));
		storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
//		Page<Bucket> buckets = authListBuckets();
//		System.out.println(buckets);
//		Blob mybolb = getBlobFromId(bucketName,topNFile);
		List topN = readerFromStrings(bucketName, topNFile);
		List termList = readerFromStrings(bucketName, indicesFile);
		Map<String, String> termMap = termMap(termList);
//		System.out.println(termMap.get("violence"));
//	    Bucket bucket = storage.create(BucketInfo.of("bucketname"));
//	    System.out.printf("Bucket %s created.%n", bucket.getName());
		
		// Create a Scanner object
		Scanner myObj = new Scanner(System.in); 
		while (true) {
		    System.out.println("Type \"search\" to search a term; Type \"TopN\" to find topN term; type \"exit\" to exit the system");
		    String command = myObj.nextLine();  // Read user input
		    if(command.contentEquals("search")) {
		    	System.out.println("the word you want to search:");
		    	String searchTerm = myObj.nextLine();
		    	System.out.println(termMap.get(searchTerm));
		    }else if(command.contentEquals("topN")) {
		    	System.out.println("the value of N:");
		    	int N = Integer.parseInt(myObj.nextLine());
		    	if((N <= 0 ) || (N > topN.size())) {
		    		System.out.println("N should be between 1 and "+ topN.size());
		    		continue;
		    	}
//		    	System.out.println(topN.subList(0, N));
		    	 for(int n = 0;n < N;n++){   
		    	       System.out.println(topN.get(n));
		    	   }   
		    }else if(command.contentEquals("exit")) {
		    	System.out.println("exit the system");
		    	break;
		    }else {
		    	continue;
		    }
		}
		myObj.close();

	}

}
