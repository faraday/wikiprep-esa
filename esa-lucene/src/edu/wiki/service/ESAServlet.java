package edu.wiki.service;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
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
	
	
	public void init(ServletConfig config) throws ServletException {
		super.init(config);
		context = config.getServletContext() ;
		
		nwd = new NormalizedWikipediaDistance(context.getInitParameter("index_path"));
		try {
			esa = new ESASearcher();
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
			
			

		} catch (Exception error) {
			response.reset() ;
			response.setContentType("application/xml");
			response.setHeader("Cache-Control", "no-cache"); 
			response.setCharacterEncoding("UTF8") ;

			response.getWriter().append("error");
		}
	}

}
