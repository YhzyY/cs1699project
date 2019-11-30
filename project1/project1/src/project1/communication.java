package project1;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.extensions.appengine.auth.oauth2.AppIdentityCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.gax.paging.Page;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.Dataproc.Projects.Regions.Jobs.Submit;
import com.google.api.services.dataproc.DataprocRequest;
import com.google.api.services.dataproc.DataprocScopes;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.SparkJob;
import com.google.api.services.dataproc.model.SubmitJobRequest;

import static java.nio.charset.StandardCharsets.UTF_8;
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
import com.google.common.collect.ImmutableList;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
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
	public static String indicesFile = "output3/part-r-00000";
	public static String topNFile = "output4/part-r-00000";
	public static List<String> allFile = new ArrayList<String>();
	
	public static String indiciesJar = "gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/JAR/jar16.jar";
	public static String topNJar = "gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/TOPJAR/top13.jar";
	public static String filesFolder = "gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/files";
	public static String indiciesFolder = "gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/output3";
	public static String topNFolder = "gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/output4";
	
	
	public void StorageSnippets(Storage storage) {
		    this.storage = storage;
	
	}
//	public static Page<Bucket> authListBuckets() throws FileNotFoundException, IOException {
//		Page<Bucket> buckets = storage.list();
//		for (Bucket bucket : buckets.iterateAll()) {
//			System.out.println(bucket.toString());
//			System.out.println(bucket.getLocation());
//			System.out.println(bucket.get("output2/part-r-00000"));
//			if((bucket.getLocation()).toString() == "US-WEST2".toString()) {
//				bucketName = bucket.getName();
//				System.out.println("!!");
//			}
//	    }
//	    // [END auth_cloud_implicit]
//	    return buckets;
//	}
//	
	public static Blob getBlobFromId(String bucketName, String blobName) {
		    // [START getBlobFromId]
		    BlobId blobId = BlobId.of(bucketName, blobName);
		    Blob blob = storage.get(blobId);
		    // [END getBlobFromId]
		    return blob;
	 }
	  
	  public static boolean deleteBlob(String bucketName, String blobName) {
		    // [START deleteBlob]
		    BlobId blobId = BlobId.of(bucketName, blobName);
		    boolean deleted = storage.delete(blobId);
		    if (deleted) {
		    	// the blob was deleted
		    	System.out.println("delete "+ blobName + " successfully");
		    } else {
		    	// the blob was not found
		    	System.out.println(blobName + " not found");
		    }
		    // [END deleteBlob]
		    return deleted;
		  }
	  
	private static void getFile(File file) {
		File[] fs = file.listFiles();
		for(File f:fs){
			if((f.toString().substring(f.toString().length()-9)).equalsIgnoreCase(".DS_Store") ) {
				continue;
			}
			if(f.isDirectory()) {
				getFile(f);
			}
			else if(f.isFile()) {
				allFile.add(f.toString());
			}else {
				System.out.println("neither a folder nor a file");
				continue;
			}
		}
//		System.out.println(allFile.size());
	}
	
//	 read content from target file
    public static String readFile(String filePath) throws IOException {
        StringBuffer sb = new StringBuffer();
        InputStream is = new FileInputStream(filePath);
        String line;
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        line = reader.readLine();
        while (line != null) {
        	sb.append(line);
        	sb.append(" ");
            line = reader.readLine();
        }
        reader.close();
        is.close();
        return sb.toString();
    }
	  
//	 write content into the bucket
	public static void writer(String bucketName, String blobName, String fileContent) throws IOException {
		    // [START writer]
		    BlobId blobId = BlobId.of(bucketName, blobName);
		    byte[] content = fileContent.getBytes(UTF_8);
		    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
		    try (WriteChannel writer = storage.writer(blobInfo)) {
		      try {
		        writer.write(ByteBuffer.wrap(content, 0, content.length));
		      } catch (Exception ex) {
		        // handle exception
		      }
		    }
		    // [END writer]
	}
	
	
//	 read content from the bucket
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

	  
	public static Map<String, String> termMap(List termList){
		Map<String, String> map = new HashMap<String, String>();
		String[] tempPair = {"" , ""};
		int cut;
		String term;
//		System.out.println(termList.size());
		for(int i = 0; i< termList.size(); i++) {
			try {
//				System.out.println(termList.get(i).toString());
				cut = termList.get(i).toString().indexOf("files/");
				if(cut == -1) continue;
				tempPair[0] = termList.get(i).toString().substring(0, cut);
				term = tempPair[0].split("\\s+")[0];
				tempPair[1] = termList.get(i).toString().substring(cut);
				if((term != null) & (term != "")) {
					map.put(term, tempPair[1]);
				}			
			}
			catch(Exception e){
				continue;
			}
			
		}
		return map;
	  }

	public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {
		// If you don't specify credentials when constructing the client, the
		// client library will look for credentials in the environment.
		Credentials credentials = GoogleCredentials.fromStream(new FileInputStream("/Users/ziyi/document/gcloud/MyFirstProject-03009f1d294c.json"));
		storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        File f = new File("/Users/ziyi/document/cs1699/project/log.txt");
        FileOutputStream fop = new FileOutputStream(f);
        OutputStreamWriter writer = new OutputStreamWriter(fop, "UTF-8");
        
		long startTime;
		long endTime;
		long TotalTime;
		
//		Page<Bucket> buckets = authListBuckets();
//		System.out.println(buckets);
//	    Bucket bucket = storage.create(BucketInfo.of("bucketname"));
//	    System.out.printf("Bucket %s created.%n", bucket.getName());

//		upload files to the GCP bucket
		writer.append("uploading files...\r\n");
		startTime = System.currentTimeMillis();
		
		String path = "/Users/ziyi/document/cs1699/project/data/shakespeare";
		File file = new File(path);
		getFile(file);
		String fileContent, fileName, bolbName;
		int fileNameIndex;
		for(int m = 0; m < allFile.size(); m++) {
			System.out.println(allFile.get(m));
			fileNameIndex = allFile.get(m).lastIndexOf('/')+1;
			if(fileNameIndex == -1) fileNameIndex = 0;
			fileName = allFile.get(m).substring(fileNameIndex);
			bolbName = "files/"+fileName;
			fileContent = readFile(allFile.get(m));
			
			writer(bucketName, bolbName, fileContent);
		}
		
		endTime  = System.currentTimeMillis();
		TotalTime = endTime - startTime; 
		writer.append("The time used to upload files is " + TotalTime +" ms \r\n");
		writer.append("\r\n");
		

//		construct inverted indicies
		writer.append("Constructing inverted indicies...\r\n");
		startTime = System.currentTimeMillis();
			
		GoogleCredential credential = GoogleCredential.fromStream(new FileInputStream("/Users/ziyi/document/gcloud/MyFirstProject-03009f1d294c.json"))
			    .createScoped(Collections.singleton(DataprocScopes.CLOUD_PLATFORM));
		String projectId = "inlaid-subset-259218";
		Dataproc dataproc = new Dataproc.Builder(new NetHttpTransport(), new JacksonFactory(), credential)
			    .setApplicationName("PITTCS1699")
			    .build();	
		dataproc.projects().regions().jobs().submit(
			    projectId, "us-west2", new SubmitJobRequest()
			    .setJob(new Job()
			    		.setPlacement(new JobPlacement()
			    				.setClusterName("cs1699project"))
			    		.setHadoopJob(new HadoopJob()
			    				.setMainJarFileUri(indiciesJar)
			            		.setArgs(ImmutableList.of(filesFolder,indiciesFolder))
			            		)))
		.execute();
		
		dataproc.projects().regions().jobs().submit(
			    projectId, "us-west2", new SubmitJobRequest()
			    .setJob(new Job()
			    		.setPlacement(new JobPlacement()
			    				.setClusterName("cs1699project"))
			    		.setHadoopJob(new HadoopJob()
			    				.setMainJarFileUri(topNJar)
			            		.setArgs(ImmutableList.of(filesFolder,topNFolder))
			            		)))
		.execute();
		
		
//		check whether the jobs are finished
		Blob indicesbolb = getBlobFromId(bucketName,indicesFile);
		Blob topNbolb = getBlobFromId(bucketName,topNFile);
 		while((indicesbolb == null) | (topNbolb == null)){
// 			TimeUnit.MINUTES.sleep(1);
 			TimeUnit.SECONDS.sleep(30);
 			indicesbolb = getBlobFromId(bucketName,indicesFile);
			topNbolb = getBlobFromId(bucketName,topNFile);
			System.out.println(topNbolb + "  " + indicesbolb);
		}
 		System.out.println("jobs finished");
 		
		
//		analyze output
		List topN = readerFromStrings(bucketName, topNFile);
		List termList = readerFromStrings(bucketName, indicesFile);
		Map<String, String> termMap = termMap(termList);	
		
		endTime   = System.currentTimeMillis(); //程序结束记录时间
		TotalTime = endTime - startTime; 
		writer.append("The time used to construct inverted indicies is " + TotalTime +" ms \r\n");
		writer.append("\r\n");
		
// 		Create a Scanner object
		Scanner myObj = new Scanner(System.in); 
//		get command from user
		while (true) {
		    System.out.println("Type \"search\" to search a term; Type \"TopN\" to find topN term; type \"exit\" to exit the system");
		    String command = myObj.nextLine();  // Read user input
		    if(command.contentEquals("search")) {
		    	System.out.println("the word you want to search:");
		    	String searchTerm = myObj.nextLine();
				writer.append("Searching word \"" + searchTerm + "\"...\r\n");
				startTime = System.currentTimeMillis();
		    	System.out.println(termMap.get(searchTerm));
				endTime   = System.currentTimeMillis();
				TotalTime = endTime - startTime; 
				writer.append("The time used to search \"" + searchTerm + "\" is " + TotalTime +" ms \r\n");
				writer.append("\r\n");
		    }else if(command.contentEquals("topN")) {
		    	System.out.println("the value of N:");
		    	int N = Integer.parseInt(myObj.nextLine());
		    	writer.append("getting top" + N + " ...\r\n");
		    	startTime = System.currentTimeMillis();
		    	if((N <= 0 ) || (N > topN.size())) {
		    		System.out.println("N should be between 1 and "+ topN.size());
		    		writer.append("N out of range : " + N + " ...\r\n");
		    		writer.append("\r\n");
		    		continue;
		    	}
//		    	System.out.println(topN.subList(0, N));
		    	 for(int n = 0;n < N;n++){   
		    	       System.out.println(topN.get(n));
		    	   }   
		 		endTime   = System.currentTimeMillis(); //程序结束记录时间
				TotalTime = endTime - startTime; 
				writer.append("The time used to find top"+ N + " is " + TotalTime +" ms \r\n");
				writer.append("\r\n");
		    }else if(command.contentEquals("exit")) {
		    	System.out.println("exit the system");
		    	writer.append("exit");
		    	writer.append("\r\n");
		    	break;
		    }else {
		    	continue;
		    }
		}
		
		myObj.close();
		writer.close();
		fop.close();

	}


}
