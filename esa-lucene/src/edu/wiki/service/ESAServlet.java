package edu.wiki.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;

import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.Transformer;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.sun.org.apache.xerces.internal.dom.DocumentImpl;
import com.sun.org.apache.xerces.internal.parsers.DOMParser;

import edu.wiki.api.concept.IConceptIterator;
import edu.wiki.api.concept.IConceptVector;
import edu.wiki.index.WikipediaAnalyzer;
import edu.wiki.search.ESASearcher;
import edu.wiki.search.NormalizedWikipediaDistance;

public class ESAServlet extends HttpServlet {
	
	private static final long serialVersionUID = 1L;
	
	protected ServletContext context;
	
	protected NormalizedWikipediaDistance nwd;
	protected ESASearcher esa;
	
	DOMParser parser = new DOMParser() ;
	protected Document doc = new DocumentImpl();
	protected DecimalFormat df = (DecimalFormat) NumberFormat.getInstance(Locale.US);
	
	
	
	static Connection connection;
	static Statement stmtQuery;
	
	static String strTitles = "SELECT id,title FROM article WHERE id IN ";
	
	public static void initDB() throws ClassNotFoundException, SQLException, IOException {
		// Load the JDBC driver 
		String driverName = "com.mysql.jdbc.Driver"; // MySQL Connector 
		Class.forName(driverName); 
		
		// read DB config
		InputStream is = ESASearcher.class.getResourceAsStream("/config/db.conf");
		BufferedReader br = new BufferedReader(new InputStreamReader(is));
		String serverName = br.readLine();
		String mydatabase = br.readLine();
		String username = br.readLine(); 
		String password = br.readLine();
		br.close();

		// Create a connection to the database 
		String url = "jdbc:mysql://" + serverName + "/" + mydatabase; // a JDBC url 
		connection = DriverManager.getConnection(url, username, password);
		
		stmtQuery = connection.createStatement();
		stmtQuery.setFetchSize(100);

  }
	
	
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		context = config.getServletContext() ;
		
		nwd = new NormalizedWikipediaDistance(context.getInitParameter("index_path"));
		try {
			esa = new ESASearcher();
			initDB();
		} catch (Exception e) {
			e.printStackTrace();
			throw new ServletException();
		}
	}
	
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

		doGet(request, response) ;

	}

	public void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

		try {

			response.setHeader("Cache-Control", "no-cache"); 
			response.setCharacterEncoding("UTF-8") ;

			String task = request.getParameter("task") ;
			
			//redirect to home page if there is no task
			if (task==null) {
				response.setContentType("text/html");
				response.getWriter().append("<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.0 Transitional//EN\"><html><head><meta http-equiv=\"REFRESH\" content=\"0;url=" + context.getInitParameter("server_path") + "></head><body></body></html>") ;
				return ;
			}
			
			//process compare request
			if (task.equals("nwd")) {
				String term1 = request.getParameter("term1");
				String term2 = request.getParameter("term2") ;
								
				if (term1 == null || term2 == null) {
					response.setContentType("text/html");
					response.getWriter().append("-1") ;
					return ;
				}
				else {
					final double distance = nwd.getDistance(term1, term2);
					response.setContentType("text/html");
					response.getWriter().append(df.format(distance)) ;
					return ;
				}
				
			}
			
			//process compare request
			if (task.equals("esa")) {
				String term1 = request.getParameter("term1");
				String term2 = request.getParameter("term2") ;
				
				if (term1 == null || term2 == null) {
					response.setContentType("text/html");
					response.getWriter().append("-1") ;
					return ;
				}
				else {
					final double sim = esa.getRelatedness(term1, term2);
					response.setContentType("text/html");
					response.getWriter().append(df.format(sim)) ;
					return ;
				}

			}
			
			//process compare request
			if (task.equals("vector")) {
				final String source = request.getParameter("source");				
				final String strLimit = request.getParameter("limit"); 
				
				int limit;
				
				if(strLimit == null){
					limit = 10;
				}
				else {
					limit = Integer.valueOf(strLimit);
				}
				
				if (source == null) {
					response.setContentType("text/html");
					response.getWriter().append("null") ;
					return ;
				}
				else {					
					response.setContentType("text/html");
					
					final IConceptVector cv = esa.getConceptVector(source);
					
					if(cv == null){
						response.getWriter().append("null") ;
					}
					
					else {
						final IConceptVector ncv = esa.getNormalVector(cv, limit);
						final IConceptIterator it = ncv.orderedIterator();
						
						HashMap<Integer, Double> vals = new HashMap<Integer, Double>(10);
						HashMap<Integer, String> titles = new HashMap<Integer, String>(10);
						
						String inPart = "(";
						
						int count = 0;
						while(it.next() && count < limit){
							inPart += it.getId() + ",";
							vals.put(it.getId(),it.getValue());
							count++;
						}
						
						inPart = inPart.substring(0,inPart.length()-1) + ")";
								
						ResultSet r = stmtQuery.executeQuery(strTitles + inPart);
						while(r.next()){
							titles.put(r.getInt(1), new String(r.getBytes(2),"UTF-8")); 
						}
						
						
						it.reset();
						count = 0;
						while(it.next() && count < limit){
							int id = it.getId();
							response.getWriter().append(id + "\t" + titles.get(id) + "\t" + df.format(vals.get(id)) + "\n") ;
							count++;
						}
					}
					return ;
				}

			}
			
			

		} catch (Exception error) {
			response.reset() ;
			response.setContentType("application/xml");
			response.setHeader("Cache-Control", "no-cache"); 
			response.setCharacterEncoding("UTF8") ;

			response.getWriter().append("error");
		}
	}

}
