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
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
//	public static String topNFile = "output2/output2.txt";
	public static String indicesFile = "output/part-r-00000";
	public static String topNFile = "output2/part-r-00000";
	public static List<String> allFile = new ArrayList<String>();
	
	
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
	
	public static Blob getBlobFromId(String bucketName, String blobName) {
		    // [START getBlobFromId]
		    BlobId blobId = BlobId.of(bucketName, blobName);
		    Blob blob = storage.get(blobId);
		    // [END getBlobFromId]
		    return blob;
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
	
	
	
//    public Submit submit(java.lang.String projectId, java.lang.String region, com.google.api.services.dataproc.model.SubmitJobRequest content) throws java.io.IOException {
//        Submit result = new Submit(projectId, region, content);
//        initialize(result);
//        return result;
//      }
//    public class Submit extends DataprocRequest<com.google.api.services.dataproc.model.Job> {
//    	private static final String REST_PATH = "v1/projects/{projectId}/regions/{region}/jobs:submit";
//
//        protected Submit(java.lang.String projectId, java.lang.String region, com.google.api.services.dataproc.model.SubmitJobRequest content) {
//            super(Dataproc.this, "POST", REST_PATH, content, com.google.api.services.dataproc.model.Job.class);
//            this.projectId = com.google.api.client.util.Preconditions.checkNotNull(projectId, "Required parameter projectId must be specified.");
//            this.region = com.google.api.client.util.Preconditions.checkNotNull(region, "Required parameter region must be specified.");
//          }
//        /** Required. The ID of the Google Cloud Platform project that the job belongs to. */
//        @com.google.api.client.util.Key
//        private java.lang.String projectId;
//        
//        /** Required. The ID of the Google Cloud Platform project that the job belongs to. */
//        public Submit setProjectId(java.lang.String projectId) {
//          this.projectId = projectId;
//          return this;
//        }
//        /** Required. The Cloud Dataproc region in which to handle the request. */
//        @com.google.api.client.util.Key
//        private java.lang.String region;
//    	
//    }
	
	
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

//		System.out.println(termMap.get("violence"));
//	    Bucket bucket = storage.create(BucketInfo.of("bucketname"));
//	    System.out.printf("Bucket %s created.%n", bucket.getName());
	
////		upload files to the GCP bucket
//		String path = "/Users/ziyi/document/cs1699/project/data/Hugo";
//		File file = new File(path);
//		getFile(file);
//		String fileContent, fileName, bolbName;
//		int fileNameIndex;
//		for(int m = 0; m < allFile.size(); m++) {
//			System.out.println(allFile.get(m));
//			fileNameIndex = allFile.get(m).lastIndexOf('/')+1;
//			if(fileNameIndex == -1) fileNameIndex = 0;
//			fileName = allFile.get(m).substring(fileNameIndex);
//			bolbName = "files/"+fileName;
//			fileContent = readFile(allFile.get(m));
//			
//			writer(bucketName, bolbName, fileContent);
//		}
		
		
		
//		construct inverted indicies
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
			    				.setMainJarFileUri("gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/JAR/jar16.jar")
			            		.setArgs(ImmutableList.of("gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/files","gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/output3"))
			            		)))
		.execute();
		
		dataproc.projects().regions().jobs().submit(
			    projectId, "us-west2", new SubmitJobRequest()
			    .setJob(new Job()
			    		.setPlacement(new JobPlacement()
			    				.setClusterName("cs1699project"))
			    		.setHadoopJob(new HadoopJob()
			    				.setMainJarFileUri("gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/TOPJAR/top13.jar")
			            		.setArgs(ImmutableList.of("gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/files","gs://dataproc-effe9cb0-28c9-4f0d-94d4-13f79e57af23-us-west2/output4"))
			            		)))
		.execute();
		
		
////		analyze output
//		List topN = readerFromStrings(bucketName, topNFile);
//		List termList = readerFromStrings(bucketName, indicesFile);
//		Map<String, String> termMap = termMap(termList);	
		
//// 		Create a Scanner object
//		Scanner myObj = new Scanner(System.in); 
////		get command from user
//		while (true) {
//		    System.out.println("Type \"search\" to search a term; Type \"TopN\" to find topN term; type \"exit\" to exit the system");
//		    String command = myObj.nextLine();  // Read user input
//		    if(command.contentEquals("search")) {
//		    	System.out.println("the word you want to search:");
//		    	String searchTerm = myObj.nextLine();
//		    	System.out.println(termMap.get(searchTerm));
//		    }else if(command.contentEquals("topN")) {
//		    	System.out.println("the value of N:");
//		    	int N = Integer.parseInt(myObj.nextLine());
//		    	if((N <= 0 ) || (N > topN.size())) {
//		    		System.out.println("N should be between 1 and "+ topN.size());
//		    		continue;
//		    	}
////		    	System.out.println(topN.subList(0, N));
//		    	 for(int n = 0;n < N;n++){   
//		    	       System.out.println(topN.get(n));
//		    	   }   
//		    }else if(command.contentEquals("exit")) {
//		    	System.out.println("exit the system");
//		    	break;
//		    }else {
//		    	continue;
//		    }
//		}
//		myObj.close();

	}


}
