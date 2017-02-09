package com.cn.p2p;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

public class AppServer {

	private static StringBuilder fileName = new StringBuilder();
	private static final String INPUT_MESSAGE = "Please enter fileName i.e. to be shared....";
	private static final String SPLIT_INITIATE = "Splitting the shared file to chunks....";
	private static final String SPLIT_COMPLETE = "File has been split successfully...";
	private static final String SERVER_LISTENING = "Server is listening on port ";
	private static final AppServerUtils appUtils = AppServerUtils.getInstance();
	private static final Integer serverPort = 8000;
	private static AtomicInteger peerConnectionCount = new AtomicInteger(0);
	private static Integer chunkCount = null;
	private static final Integer PEER_COUNT = 5;
	private static final String CHUNK_LOCATION = "ChunkRepository\\";
	private static final String PERIOD = ".";

	public static void main(String args[]) throws Exception{

		Scanner scanner = null;
		Integer client = 1;

		try{
			scanner = new Scanner(System.in);

			System.out.println(INPUT_MESSAGE);
			fileName.append(scanner.nextLine());

			System.out.println(SPLIT_INITIATE);
			chunkCount = appUtils.splitFile(fileName);
			System.out.println(SPLIT_COMPLETE);

			ServerSocket socket = new ServerSocket(serverPort);
			System.out.println(SERVER_LISTENING + serverPort);

			try{
				while(true)
					new Handler(socket.accept(),client++).start();
			}finally{
				socket.close();
			}

		}finally{
			if(scanner != null)
				scanner.close();
		}
	}

	private static class Handler extends Thread {

		private Socket Connection;
		private ObjectOutputStream outputStream = null;
		private Integer clientNumber = null;
		private Integer sharePerPeer = chunkCount/PEER_COUNT;


		public Handler(Socket socket, int client){
			peerConnectionCount.incrementAndGet();
			this.Connection = socket;
			this.clientNumber = client;

			try{
				outputStream = new ObjectOutputStream(Connection.getOutputStream());
				outputStream.writeObject(chunkCount);
				outputStream.writeObject(fileName.toString());
			}catch(IOException e){
				e.printStackTrace();
			}

		}

		public void run(){

			try{
				int chunkStartIndex = 0, chunkEndIndex = 0;
				chunkStartIndex = (clientNumber - 1) * sharePerPeer;
				chunkEndIndex = returnChunkEndIndex();

				for(; chunkStartIndex < chunkEndIndex; chunkStartIndex++){

					String fileChunkName = CHUNK_LOCATION + fileName + PERIOD + chunkStartIndex;
					File chunkFile = new File(fileChunkName);
					byte[] byteArray = new byte[(int)chunkFile.length()];
					FileInputStream fis = new FileInputStream(chunkFile);
					BufferedInputStream bis = new BufferedInputStream(fis);

					DataInputStream dataInStream = new DataInputStream(bis);   
					dataInStream.readFully(byteArray, 0, byteArray.length);

					OutputStream outStream = Connection.getOutputStream();

					DataOutputStream dataOutStream = new DataOutputStream(outStream);   
					dataOutStream.writeUTF(chunkFile.getName());   
					dataOutStream.writeLong(byteArray.length);   
					dataOutStream.write(byteArray, 0, byteArray.length);   
					dataOutStream.flush();
				}

				OutputStream outStream = Connection.getOutputStream();
				DataOutputStream  dataOutStream = new DataOutputStream(outStream);
				dataOutStream.writeUTF("Completed!");
				
				while(true)
				{
					if(peerConnectionCount.get()== PEER_COUNT)
					{
						ObjectOutputStream out = new ObjectOutputStream(Connection.getOutputStream());
				        out.writeObject("ServerTransferCompleted");
						break;
					}
				}

				Connection.close();

			}catch(IOException e){
				e.printStackTrace();
			}
		}

		private Integer returnChunkEndIndex(){

			Integer chunkEndIndex = null;
			chunkEndIndex = clientNumber <= (PEER_COUNT - 1) ?  (clientNumber * sharePerPeer) : (chunkCount);
			return chunkEndIndex;
		}
	}

}
