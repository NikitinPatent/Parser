package parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URL;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.Vector;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class parser {
	
	private static Writer writer;
	public static void main(String args[]){
		try {
			writer = new BufferedWriter(new OutputStreamWriter(
			          new FileOutputStream("log_file.log",true), "utf-8"));
		} catch (IOException ex) {
		}
		
		String input = "";
		for (int i=0;i<args.length;i++){
			input += args[i] + " ";
		}
		String search_string = "none";
		String path_to_file = "none";
		Boolean is_from_file = false;
		Boolean is_found = false;
		for (int i=0;i<args.length;i++){
			if( input.contains("-f") && !is_found ){
				is_from_file = true;
				path_to_file = args[i+1];
				is_found = true;
			}
			else if( input.contains("-s") && !is_found ){
				search_string = args[i+1];
				
				search_string = input.substring(input.indexOf("-s")+3, input.length());
				is_found = true;
			}
		}
		
		if( search_string == "none"  && is_from_file){
			search_string = getSearchString(path_to_file);
		}
	
		try{
			Runtime.getRuntime().exec("/bin/bash -c python main.py");
		}
		catch (Exception ex){}
		writeToLog("PARSING GOOGLE SCHOLAR");
		Vector<String> scholar = parseGoogleScholarFromFile();
		//Vector<String> scholar = parseGoogleScholar(search_string.replace(" ", "+"));
		
		writeToLog("WRITING SCHOLAR DATA TO DATABASE");
		writeToDB(scholar);

		//writeToLog("PARSING ELIBRARY");
		//Vector<String> elibrary = parseElibrary();
		
		//writeToLog("WRITING ELIBRARY DATA TO DATABASE");
		//writeToDB(elibrary);
		
		try {writer.close();} catch (Exception ex) {}
		
		//Vector<String> scholar = parseGoogleScholarFromFileTest();
		//writeToDB(scholar);
		
		//try {writer.close();} catch (Exception ex) {}
	}
	
	public static void writeToDB( Vector<String> data ){
		
		String[] configData = getConfigData().split(" ");
		
		java.sql.Connection connection = null;
        String url = "jdbc:postgresql://127.0.0.1:5432/"+configData[0];
        String name = configData[1];
        String password = configData[2];
        try {
            Class.forName("org.postgresql.Driver");
            writeToLog("Successfully connected driver");
            connection = DriverManager.getConnection(url, name, password);
            writeToLog("Successfully connected to "+url+" user name is "+name+", password is "+password);
            
            Statement statement = null;
            statement = connection.createStatement();
            
            ResultSet result1 = statement.executeQuery(
                    "SELECT id FROM parsing_data");
            int max = 0;
            while (result1.next()) {
            	if( result1.getInt("id") > max ){
            		max = result1.getInt("id");
            	}
            }
            
            for(int i=0; i < data.size(); i++){
            	
            	String[] parts = data.get(i).split(";");
            	
            	try{
	            	String query = "INSERT INTO parsing_data(id, date_of_added, header, fio, year_of_edition, publishing, link, language, from_data, path_to_pdf, path_to_txt)"
	                        + " values("+max+", '"+parts[0]+"', '"+parts[1]+"', '"+parts[2]+"', '"+parts[3]+"', '"+parts[4]+"', '"+parts[5]+"', '"+parts[6]+"', '"+parts[7]+"', '"+parts[8]+"', '"+parts[9]+"')";
	            	statement.executeUpdate(query);
	            	
	            	writeToLog("Successfully executed query "+query);
	            	max++;
            	} catch (Exception ex){
            		writeToLog("ERROR! "+ex.getMessage());
            		}
            }
            
        } catch (Exception ex) {
        	writeToLog("ERROR! "+ex.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                	writeToLog("ERROR! "+ex.getMessage());
                }
            }
        }
	}
	
	public static Vector<String> parseGoogleScholar(String search){
		Vector<String> scholarData = new Vector<String>();
		boolean isEnd = false;
		Random rand = new Random();
			
		try{
			int start = 0;
			
			int id = getFirstId();
			while(!isEnd){
				/*try{
					Thread.sleep(rand.nextInt((100000 - 40000) + 1) + 40000);
				}
				catch(InterruptedException ie){writeToLog("ERROR! "+ie.getMessage());}*/
		        
				Document doc=Jsoup.connect("http://scholar.google.ru/scholar?as_vis=1&start="+start+"&q="+search+"&num=20").userAgent("Mozilla/5.0 Chrome/26.0.1410.64 Safari/537.31").get();
				
				writeToLog("Successfully connected to http://scholar.google.ru/scholar?start="+start+"&q="+search+"&num=20");
				
				Elements div_gs_ri = doc.select("div.gs_ri"); //get div by class
				Elements next = doc.select("div#gs_n"); //get div by id
				
				//System.out.println(doc);
						
				String tmp = next.select("b").get(2).toString();
						
				if(tmp.contains("hidden")){
					isEnd = true;
				}
						
				for (int i=0; i< div_gs_ri.size(); i++){
					Element item = div_gs_ri.get(i);
					String link;
					DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
					Date date = new Date();
							
					String header = item.select("h3").first().text();
					String nameAndYear = item.select("div.gs_a").first().text();
							
					if(!header.contains("[ЦИТИРОВАНИЕ]")){
						link = item.select("a").first().attr("href");
					}
					else{
						link = "none";
					}
					
					String year = nameAndYear.replaceAll("[^0-9]", "");
					if(year.length() > 4){
						year = year.substring(Math.max(year.length() - 4, 0));
					}
					if(year.length() < 1){
						year = "none";
					}
					
					String[] parts = nameAndYear.split("-");
					String publishing="";
					
					for(int j=1; j < parts.length; j++ ){
						publishing += parts[j];
						
					}
					
					publishing = publishing.replaceAll("[0-9]", "");
					
					String language = detectLanguage(header);
					
					String[] configData = getConfigData().split(" ");
					
					if(header.contains("[PDF]")){
						boolean isSuccess = downloadPdf(link,configData[3]+"/"+Integer.toString(id)+".pdf");
						if(isSuccess){
							scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar"+";"+Integer.toString(id)+".pdf;"+Integer.toString(id)+".txt");
							writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar"+";"+Integer.toString(id)+".pdf;"+Integer.toString(id)+".txt");
		
							try{
								String toExec = "java -jar pdfbox-app-1.8.9.jar ExtractText "+configData[3]+"/"+Integer.toString(id)+".pdf "+configData[4]+"/"+Integer.toString(id)+".txt";
								Runtime.getRuntime().exec(toExec);
							}
							catch (Exception ex){
								writeToLog("ERROR! "+ex.getMessage());
							}
						}
						else{
							scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
							writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
						}
					}
					else{
						scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
						writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
					}
					id++;
				}
				
				start += 20;
			}
		}
		catch (IOException e) {
			writeToLog("Unable to connect to Google Scholar");
		}
				
		return scholarData;
	}
	public static Vector<String> parseGoogleScholarFromFile(){

		Vector<String> scholarData = new Vector<String>();
		boolean isEnd = false;
		
		String search="сварка+взрывом";
		
		try{
			int start = 0;
			
			int id = getFirstId();
			while(!isEnd){
				File in = new File("./docs2/input"+start+".html");
				Document doc = Jsoup.parse(in, "UTF-8");
				
				writeToLog("Get documents ./docs2/input"+start+".html");
				
				Elements div_gs_ri = doc.select("div.gs_ri"); //get div by class
				Elements next = doc.select("div#gs_n"); //get div by id
				
				String tmp = next.select("b").get(2).toString();
				
				if(tmp.contains("hidden")){
					isEnd = true;
				}
				
				for (int i=0; i< div_gs_ri.size(); i++){
					Element item = div_gs_ri.get(i);
					String link;
					DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
					Date date = new Date();
					
					String header = item.select("h3").first().text();
					String nameAndYear = item.select("div.gs_a").first().text();
					
					if(!header.contains("[ЦИТИРОВАНИЕ]")){
						link = item.select("a").first().attr("href");
					}
					else{
						link = "none";
					}
					
					String year = nameAndYear.replaceAll("[^0-9]", "");
					if(year.length() > 4){
						year = year.substring(Math.max(year.length() - 4, 0));
					}
					if(year.length() < 1){
						year = "none";
					}
					
					String[] parts = nameAndYear.split("-");
					String publishing="";
					
					for(int j=1; j < parts.length; j++ ){
						publishing += parts[j];
						
					}
					
					publishing = publishing.replaceAll("[0-9]", "");
					
					String language = detectLanguage(header);
					
					String[] configData = getConfigData().split(" ");
					
					if(header.contains("[PDF]")){
						boolean isSuccess = downloadPdf(link,configData[3]+"/"+Integer.toString(id)+".pdf");
						if(isSuccess){
							scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar"+";"+Integer.toString(id)+".pdf;"+Integer.toString(id)+".txt");
							writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar"+";"+Integer.toString(id)+".pdf;"+Integer.toString(id)+".txt");
						
							try{
								String toExec = "java -jar pdfbox-app-1.8.9.jar ExtractText "+configData[3]+"/"+Integer.toString(id)+".pdf "+configData[4]+"/"+Integer.toString(id)+".txt";
								Runtime.getRuntime().exec(toExec);
							}
							catch (Exception ex){
								writeToLog("ERROR! "+ex.getMessage());
							}
						}
						else{
							scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
							writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
						}
					}
					else{
						scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
						writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
					}
					id++;
				}
				
				start += 20;
			}
		}
		catch (IOException e) {
			writeToLog("ERROR! "+e.getMessage());
		}
		
		return scholarData;
	}
		
	public static Vector<String> parseElibrary(){
		
		Vector<String> elibraryData = new Vector<String>();
		final File folder = new File("./elibrary");
		Vector<String> tmp = listFilesFromFolder(folder);
		Vector<String> paths = new Vector<String>();
		String link = "none";
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		
		for(int i=0; i<tmp.size(); i++){
			if(tmp.get(i).contains(".csv")){
				paths.add(tmp.get(i));
			}
		}
		
		writeToLog("In ./elibrary found  "+paths.size()+" .csv files");
		
		for(int i=0; i<paths.size(); i++){
			Vector<String> result = getDataFromFile(paths.get(i));
			
			for(int j=0; j<result.size(); j++){
				String[] parts = result.get(j).split(";");
				
				try{
					elibraryData.add(dateFormat.format(date)+";"+parts[1]+";"+parts[2]+";"+parts[9]+";"+parts[3]+";"+link+";"+parts[5]+";false");
					
					writeToLog("Found record "+dateFormat.format(date)+";"+parts[1]+";"+parts[2]+";"+parts[9]+";"+parts[3]+";"+link+";"+parts[5]+";false");
				} catch(Exception ex){
					writeToLog("ERROR! "+ex.getMessage());
				}
			}
		}
		
		return elibraryData;
	}
	
	public static Vector<String> listFilesFromFolder(final File folder) {
		
		Vector<String> result = new Vector<String>();
		
	    for (final File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            listFilesFromFolder(fileEntry);
	        } else {
	            result.add(fileEntry.getPath());
	        }
	    }
	    
	    return result;
	}
	
	public static Vector<String> getDataFromFile(String path){
		Vector<String> everything = new Vector<String>();
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			everything.add(line);
			
			while (line != null) {
				sb.append(line);
				line = br.readLine();
				if(line != null){
					everything.add(line);
				}
			}
			br.close();
		}
		catch(IOException e){}
		
		return everything;
	}
	
	public static String getSearchString(String path){
		String everything = "";
		
		try {
			BufferedReader br = new BufferedReader(new FileReader(path));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			
			while (line != null) {
				sb.append(line);
				line = br.readLine();
			}
			everything = sb.toString();
			br.close();
		}
		catch(IOException e){}
		
		return everything.replace(" ", "+");
	}

	public static String detectLanguage(String header){
		String language = "русский";
		
		boolean isRussian = header.contains("А") || header.contains("а") || header.contains("Б") || header.contains("б") || header.contains("Д") || header.contains("д") || header.contains("Р") || header.contains("р");
		if( !isRussian ){
			language = "english";
		}
		
		return language;
	}

	public static void writeToLog(String data){

		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();

		try {
		    writer.write("\n"+dateFormat.format(date)+" : "+data+";\n");
		} catch (IOException ex) {
		  // report
		}
	}
	
	public static String getConfigData(){
		String everything = "";
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("config.conf"));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			
			while (line != null) {
				sb.append(line);
				line = br.readLine();
			}
			everything = sb.toString();
			br.close();
		}
		catch(IOException e){}
		
		return everything;
	}
	
	public static void downloadAllPdfs(Vector<String> vector){
		
		String[] configData = getConfigData().split(" ");
		
		for(int i = 0; i < vector.size(); i++){
			String[] parts = vector.get(i).split(";");
			if(parts[1].contains("[PDF]")){
				downloadPdf(parts[5],"/"+parts[8]);
			}
		}
		
	
	}
	
	public static boolean downloadPdf(String link, String path){
		boolean isSuccess = true;
		InputStream input = null;
	    OutputStream output = null;
	    HttpURLConnection connection = null;
	    try {
	        URL url = new URL(link);
	        connection = (HttpURLConnection) url.openConnection();
	        connection.connect();

	        // expect HTTP 200 OK, so we don't mistakenly save error report
	        // instead of the file
	        if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
	        	//writeToLog("Server returned HTTP " + connection.getResponseCode()
	             //      + " " + connection.getResponseMessage());
	        	isSuccess = false;
	        }

	        // this will be useful to display download percentage
	        // might be -1: server did not report the length
	        int fileLength = connection.getContentLength();

	        // download the file
	        input = connection.getInputStream();
	        output = new FileOutputStream(path);

	        byte data[] = new byte[4096];
	        int count;
	        while ((count = input.read(data)) != -1) {
	            output.write(data, 0, count);
	        }
	    } catch (Exception e) {
	        //writeToLog(e.toString());
	        isSuccess = false;
	    } finally {
	        try {
	            if (output != null)
	                output.close();
	            if (input != null)
	                input.close();
	        } catch (IOException ignored) {
	        }

	        if (connection != null)
	            connection.disconnect();
	    }
	    
	    return isSuccess;
	}
	
	public static int getFirstId(){
		String[] configData = getConfigData().split(" ");
		
		java.sql.Connection connection = null;
        String url = "jdbc:postgresql://127.0.0.1:5432/"+configData[0];
        String name = configData[1];
        String password = configData[2];
        int max = 0;
        try {
            Class.forName("org.postgresql.Driver");
            writeToLog("Successfully connected driver");
            connection = DriverManager.getConnection(url, name, password);
            writeToLog("Successfully connected to "+url+" user name is "+name+", password is "+password);
            
            Statement statement = null;
            statement = connection.createStatement();
            
            ResultSet result1 = statement.executeQuery(
                    "SELECT id FROM parsing_data");
            
            while (result1.next()) {
            	if( result1.getInt("id") > max ){
            		max = result1.getInt("id");
            	}
            }
        } catch (Exception ex) {
        	writeToLog("ERROR! "+ex.getMessage());
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException ex) {
                	writeToLog("ERROR! "+ex.getMessage());
                }
            }
        }
        
        return max;
	}
	//////////////////////////////////////////////////////
	public static Vector<String> parseGoogleScholarFromFileTest(){

		Vector<String> scholarData = new Vector<String>();
		boolean isEnd = false;
		
		String search="сварка+взрывом";
		
		try{
			int start = 0;
			
			int id = getFirstId();
			while(!isEnd){
				File in = new File("./docs2/input"+start+".html");
				Document doc = Jsoup.parse(in, "UTF-8");
				
				writeToLog("Get documents ./docs2/input"+start+".html");
				
				Elements div_gs_ri = doc.select("div.gs_ri"); //get div by class
				Elements next = doc.select("div#gs_n"); //get div by id
				
				String tmp = next.select("b").get(2).toString();
				
				if(tmp.contains("hidden")){
					isEnd = true;
				}
				
				for (int i=0; i< div_gs_ri.size(); i++){
					Element item = div_gs_ri.get(i);
					String link;
					DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
					Date date = new Date();
					
					String header = item.select("h3").first().text();
					String nameAndYear = item.select("div.gs_a").first().text();
					
					if(!header.contains("[ЦИТИРОВАНИЕ]")){
						link = item.select("a").first().attr("href");
					}
					else{
						link = "none";
					}
					
					String year = nameAndYear.replaceAll("[^0-9]", "");
					if(year.length() > 4){
						year = year.substring(Math.max(year.length() - 4, 0));
					}
					if(year.length() < 1){
						year = "none";
					}
					
					String[] parts = nameAndYear.split("-");
					String publishing="";
					
					for(int j=1; j < parts.length; j++ ){
						publishing += parts[j];
						
					}
					
					publishing = publishing.replaceAll("[0-9]", "");
					
					String language = detectLanguage(header);
					
					String[] configData = getConfigData().split(" ");
					
					if(header.contains("[PDF]")){
							scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar"+";"+Integer.toString(id)+".pdf;"+Integer.toString(id)+".txt");
							writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar"+";"+Integer.toString(id)+".pdf;"+Integer.toString(id)+".txt");
					}
					else{
						scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
						writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";Google Scholar;none;none");
					}
					id++;
				}
				
				start += 20;
			}
		}
		catch (IOException e) {
			writeToLog("ERROR! "+e.getMessage());
		}
		
		return scholarData;
	}
	/////////////////////////////////////////////////////
}