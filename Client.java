package com.cn.p2p;


import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicIntegerArray;


public class Client
{ 
	private static Socket socket = null;
	private static AtomicIntegerArray  chunksList;
	private static String parentFileName = "";
	private static final Integer CLIENT_ID = 1;
	private static final String LISTENING_PORT = "LISTENING_PORT";
	private static final String UPLOAD_PORT = "UPLOAD_PORT";
	private static final String DOWNLOAD_PORT = "DOWNLOAD_PORT";
	private static final String SERVER_MESSAGE = "Completed!";
	private static final String MERGE_REPOSITORY = "MergeRepository";
	private static final String CLIENT_SERVER_MSG = "Server running at ";
	private static final String UPLOAD_MSG = "Uploading to peer at port ";
	private static final String DOWNLOAD_MSG = "Downloading from peer at port ";
	private static final String CONFIGURATION_FILE = "Config.txt";
	private static final String CHUNK_REPOSITORY = "ChunkRepository";
	private static ArrayList<String> serverChunkList = new ArrayList<String>();

	@SuppressWarnings("deprecation")
	public static void main(String srgs[])throws Exception
	{
		getChunksFromServer();
		Map<String, Integer> networkConfig = readConfigFile();
		ServerSocket serverSocket = new ServerSocket(networkConfig.get(LISTENING_PORT));

		System.out.println(CLIENT_SERVER_MSG + networkConfig.get(LISTENING_PORT));
		System.out.println("Client No : "+ CLIENT_ID +" at port :" + networkConfig.get(LISTENING_PORT));
		System.out.println(UPLOAD_MSG +networkConfig.get(UPLOAD_PORT));
		System.out.println(DOWNLOAD_MSG +networkConfig.get(DOWNLOAD_PORT));

		Downloader downloader = new Downloader(networkConfig.get(DOWNLOAD_PORT));
		downloader.start();

		Uploader uploader = new Uploader(serverSocket.accept());
		uploader.start();
	}   

	public static Map<String, Integer> readConfigFile(){

		Scanner scanner = null;
		String[] portConfiguration =  new String[6];
		Map<String, Integer> networkConfiguration = new HashMap<String, Integer>();
		int i =0, uploadPeerId = 0, downloadPeerId = 0;

		try{
			scanner = new Scanner(new File(CONFIGURATION_FILE));
		}catch(FileNotFoundException e){
			System.out.println("Config file not found, exiting");
			System.exit(0);				
		}catch(Exception e){
			e.printStackTrace();
			System.exit(0);
		}
		while(scanner.hasNext()){
			String configuration = scanner.nextLine();
			portConfiguration[i] = configuration.split("\\s+")[1];
			if(CLIENT_ID == i){
				uploadPeerId = Integer.valueOf(configuration.split("\\s+")[2]) - 1;
				downloadPeerId = Integer.valueOf(configuration.split("\\s+")[3]) - 1;
			}
			i++;
		}
		scanner.close();

		networkConfiguration.put(LISTENING_PORT, Integer.valueOf(portConfiguration[CLIENT_ID]));
		networkConfiguration.put(UPLOAD_PORT, Integer.valueOf(portConfiguration[uploadPeerId]));
		networkConfiguration.put(DOWNLOAD_PORT, Integer.valueOf(portConfiguration[downloadPeerId]));

		return networkConfiguration;
	}

	public static void getChunksFromServer()
	{
		try
		{ 
			socket = new Socket("localhost",8000);
			ObjectInputStream inputStream = new ObjectInputStream(socket.getInputStream());
			Object chunkCount = inputStream.readObject();
			Object file = inputStream.readObject();

			parentFileName = (String)file;

			// Inititalizing Atomic Integer Array and setting elements initially to zero
			chunksList = new AtomicIntegerArray((int)chunkCount);
			for(int i = 0; i <(int)chunkCount ; i++)
			{
				chunksList.set(i,0);
			}

			Boolean proceed = true;
			int bytesRead;
			while(proceed)
			{

				InputStream in = socket.getInputStream();
				DataInputStream dataStream = new DataInputStream(in); 

				String fString = dataStream.readUTF();

				String fileName = CHUNK_REPOSITORY + "\\" + fString;
				if(!fString.equalsIgnoreCase(SERVER_MESSAGE))
				{	
					// Retrieving the chunk number from the file name

					serverChunkList.add(fString);
					int chunkNumber = 0;
					String[] fileNameSplit = fString.split("\\.");
					chunkNumber = Integer.valueOf(fileNameSplit[fileNameSplit.length - 1]);
					chunksList.set(chunkNumber, 1);

					OutputStream outputStream = new FileOutputStream(fileName);   
					Long streamSize = dataStream.readLong();   
					byte[] bufferSize = new byte[1024];   
					while (streamSize > 0 && (bytesRead = dataStream.read(bufferSize, 0, (int)Math.min(bufferSize.length, streamSize))) != -1)   
					{   
						outputStream.write(bufferSize, 0, bytesRead);   
						streamSize -= bytesRead;   
					}
					outputStream.close();
				}
				else
					proceed = false;
				System.out.println("Transferring chunks from AppServer to Client!");
			}

			// Printing Initial Chunks received from the server into a summary file

			PrintWriter out = new PrintWriter("ServerChunk.txt");
			for(int i = 0; i < serverChunkList.size(); i++)
				out.println(serverChunkList.get(i));
			out.close();


			ObjectInputStream obInputStream = new ObjectInputStream(socket.getInputStream());
			Object object = obInputStream.readObject();
			System.out.println(String.valueOf(object));
			System.out.println("Chunk Transfer from AppServer Completed");
			socket.close();


		}catch(Exception e)
		{
			e.printStackTrace();
			System.exit(0);
		}
	}

	private static class Downloader extends Thread {
		private Socket connection;
		private ObjectInputStream in;	
		private ObjectOutputStream out;  
		private Integer downloadPeerPort;

		public Downloader(int port) throws UnknownHostException, IOException, InterruptedException {
			System.out.println("Client "  + CLIENT_ID + " is connected to DOWNLOAD Client on Port " + port);
			connection=new Socket("localhost",port);
			downloadPeerPort = port;
		}

		public void run() 
		{
			try
			{
				while(true)
				{
					out = new ObjectOutputStream(connection.getOutputStream());
					ArrayList arr = new ArrayList();

					for (int u=0;u<chunksList.length();u++)
					{
						int arg1 = chunksList.get(u);
						arr.add(arg1);
					}
					out.writeObject(arr);
					System.out.println("Requesting for Chunk list from peer at port " + downloadPeerPort);
					ArrayList receivedBatch = new ArrayList();

					Boolean proceed = true;
					while(proceed)
					{
						int bytesRead;

						InputStream in = connection.getInputStream();
						DataInputStream clientData = new DataInputStream(in); 

						String fString = clientData.readUTF();
						if(!fString.equalsIgnoreCase("Done"))
						{

							int chunkNumber = 0;
							String[] fileNameSplit = fString.split("\\.");
							chunkNumber = Integer.valueOf(fileNameSplit[fileNameSplit.length - 1]);

							String fileName = CHUNK_REPOSITORY + "\\"+fString;
							System.out.println("Received chunk " + chunkNumber +" file " +fString);
							receivedBatch.add(chunkNumber);

							OutputStream output = new FileOutputStream(fileName);   
							long size = clientData.readLong();   
							byte[] buffer = new byte[1024]; 

							while ((bytesRead = clientData.read(buffer, 0, (int)Math.min(buffer.length, size))) != -1 && size > 0)   
							{   
								output.write(buffer, 0, bytesRead);   
								size = size - bytesRead;   
							}
							output.close();
						}
						else{
							proceed = false;
						}							

						for (int i= 0; i < receivedBatch.size(); i++)
						{
							chunksList.set((int)receivedBatch.get(i), 1);
						}						
					}
				}
			}
			catch(Exception e)
			{
				e.printStackTrace();
				System.exit(0);
			}
		}
	}

	private static class Uploader extends Thread {

		private Socket connection;
		public Uploader(Socket connection){
			this.connection = connection;

		}
		public void run() {
			try{
				while(true)
				{
					ObjectInputStream inputStream = new ObjectInputStream(connection.getInputStream());
					Object object = inputStream.readObject();
					ArrayList arrayList = (ArrayList) object;
					Boolean proceed = true;
					int i = 0;


					while(proceed)
					{
						int dispatchChunk = chunksList.length() + 1;
						for ( ; i < arrayList.size(); i++) {
							if(((int)chunksList.get(i)-(int)arrayList.get(i)) == 1)
							{
								dispatchChunk = i;
								break;
							}
						}
						i= dispatchChunk;
						
						OutputStream opstream = connection.getOutputStream();
						DataOutputStream dataOutStream = new DataOutputStream(opstream);   

						if(i < arrayList.size())
						{
							System.out.println("Sending Chunk :" + dispatchChunk);
							String chunkName = CHUNK_REPOSITORY + "\\" + parentFileName +"."+i; 
							File myFile = new File(chunkName);
							byte[] mybytearray = new byte[(int) myFile.length()];

							FileInputStream fis = new FileInputStream(myFile);
							BufferedInputStream bis = new BufferedInputStream(fis);

							DataInputStream dis = new DataInputStream(bis);   
							dis.readFully(mybytearray, 0, mybytearray.length);

							System.out.println(myFile.getName());
							dataOutStream.writeUTF(myFile.getName());   
							dataOutStream.writeLong(mybytearray.length);   
							dataOutStream.write(mybytearray, 0, mybytearray.length);   
							dataOutStream.flush();

							i++;
						}
						else
						{
							dataOutStream.writeUTF("Done");
							proceed = false;
						}
					}

					int chunksRecCount = 0;
					for (int j = 0;j < chunksList.length(); j++)
					{
						chunksRecCount += chunksList.get(j);
					}

					if(chunksRecCount == chunksList.length())
					{
						System.out.println("All the chunks are received by Client");
						Thread.sleep(5000);
						break;

					}
				}

				System.out.println("-----------Merging the Chunks----------");
				String baseFileName = CHUNK_REPOSITORY + "\\"+parentFileName;
				mergeChunks(baseFileName,parentFileName);
				System.out.println("File download is completed");
			}
			catch(Exception e) {
				e.printStackTrace();
			}
		}

		public static void mergeChunks(String baseFilename, String parentFileName) throws IOException
		{
			int count = retrieveCount(baseFilename);
			System.out.println("Total chunks count "+ count);

			String mergedFileName = MERGE_REPOSITORY + "\\"+ parentFileName; 
			BufferedOutputStream out = new BufferedOutputStream(new FileOutputStream(mergedFileName));
			for (int i = 0; i < count; i++)
			{
				BufferedInputStream in = new BufferedInputStream(new FileInputStream(baseFilename + "." + i));

				int bytes;
				while ( (bytes = in.read()) != -1 )
					out.write(bytes);

				in.close();
			}
			out.close();
		}

		private static Integer retrieveCount(String totalName) throws IOException
		{			
			File Directory = new File(totalName).getAbsoluteFile().getParentFile();
			final String fileName = new File(totalName).getName();
			String[] filteredFiles = Directory.list(new FilenameFilter()
			{
				public boolean accept(File Directory, String name)
				{
					return acceptCriterion(name, fileName);
				}
			});
			return filteredFiles.length;
		}

		private static Boolean acceptCriterion(String totalName, String fileName){

			return totalName.startsWith(fileName) && totalName.substring(fileName.length()).matches("^\\.\\d+$");
		}

	}

}

