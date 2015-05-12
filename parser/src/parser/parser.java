package parser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
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
		
		//writeToLog("PARSING GOOGLE SCHOLAR");
		Vector<String> scholar = parseGoogleScholarFromFile();
		
		//writeToLog("WRITING SCHOLAR DATA TO DATABASE");
		writeToDB(scholar);

		/*writeToLog("PARSING ELIBRARY");
		Vector<String> elibrary = parseElibrary();
		
		writeToLog("WRITING ELIBRARY DATA TO DATABASE");
		writeToDB(elibrary);*/
		
		try {writer.close();} catch (Exception ex) {/*ignore*/}
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
	            	String query = "INSERT INTO parsing_data(id, date_of_added, header, fio, year_of_edition, publishing, link, language, is_scholar)"
	                        + " values("+max+", '"+parts[0]+"', '"+parts[1]+"', '"+parts[2]+"', '"+parts[3]+"', '"+parts[4]+"', '"+parts[5]+"', '"+parts[6]+"', "+parts[7]+")";
	            	statement.executeUpdate(query);
	            	
	            	writeToLog("Successfully executed query "+query);
	            	max++;
            	} catch (Exception ex){writeToLog("ERROR! "+ex.getMessage());}
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
	
	public static Vector<String> parseGoogleScholar(){
		Vector<String> scholarData = new Vector<String>();
		boolean isEnd = false;
		
		String search = getSearchString();
			
		try{
			int start = 0;
			while(!isEnd){
				try{
					Thread.sleep(10000);
				}
				catch(InterruptedException ie){writeToLog("ERROR! "+ie.getMessage());}
				Document doc=Jsoup.connect("http://scholar.google.ru/scholar?start="+start+"&q="+search+"&num=20").userAgent("Mozilla/5.0 Chrome/26.0.1410.64 Safari/537.31").get();
				
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
					
					scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";true");
					
					writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";true");
					
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
			while(!isEnd){
				File in = new File("/home/nikita/workspace/parser/docs2/input"+start+".html");
				Document doc = Jsoup.parse(in, "UTF-8");
				
				writeToLog("Get documents /home/nikita/workspace/parser/docs2/input"+start+".html");
				
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
					
					scholarData.add(dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";true");
					
					writeToLog("Found record - "+dateFormat.format(date)+";"+header+";"+parts[0]+";"+year+";"+publishing+";"+link+";"+language+";true");
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
		final File folder = new File("/home/nikita/workspace/parser/elibrary");
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
		
		writeToLog("In /home/nikita/workspace/parser/elibrary found  "+paths.size()+" .csv files");
		
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
	
	public static String getSearchString(){
		String everything = "";
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("input.txt"));
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
}