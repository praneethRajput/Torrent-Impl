package com.cn.p2p;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class AppServerUtils {

	private static AppServerUtils appUtils = null;
	private static final String FILE_LOCATION = "FileRepository\\";
	private static final String CHUNK_LOCATION = "ChunkRepository\\";
	private static final String PERIOD = ".";
	private static final Integer chunkSize = 100*1024;

	public static AppServerUtils getInstance(){
		if(appUtils == null)
			appUtils = new AppServerUtils();

		return appUtils;
	}

	public Integer splitFile(StringBuilder fileName) throws FileNotFoundException, IOException{

		Long fileSize = null;
		Double totalChunks = null;
		Integer chunkCounter = null;
		StringBuilder destinationFileName = new StringBuilder(CHUNK_LOCATION);
		StringBuilder sourceFileName = new StringBuilder(FILE_LOCATION);

		destinationFileName.append(fileName).append(PERIOD);
		sourceFileName.append(fileName);
		File file = new File(sourceFileName.toString());
		fileSize = file.length();
		totalChunks = Math.ceil((double)(fileSize)/(double)(chunkSize));

		BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(sourceFileName.toString()));

		for(chunkCounter = 0; chunkCounter < fileSize/ chunkSize; chunkCounter++){

			StringBuilder chunkName = new StringBuilder(destinationFileName);
			BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(chunkName.append(chunkCounter).toString()));
			for(int currentByte = 0; currentByte < chunkSize; currentByte++)
				outputStream.write(inputStream.read());
			outputStream.close();

		}

		if(splitRemainder(fileSize, chunkCounter)){

			StringBuilder chunkName = new StringBuilder(destinationFileName);
			BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(chunkName.append(chunkCounter).toString()));
			int b;
			while ((b = inputStream.read()) != -1)			
				outputStream.write(b);
			
			outputStream.close();
		}

		inputStream.close();

		return totalChunks.intValue();

	}


	private static Boolean splitRemainder(Long fileSize, Integer chunkCounter){
		return fileSize != (chunkSize * (chunkCounter - 1));
	}
}