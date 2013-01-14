package eu.deustotech.rdf2subdue.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.validator.routines.UrlValidator;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import com.clarkparsia.stardog.StardogException;
import com.clarkparsia.stardog.api.Connection;
import com.clarkparsia.stardog.api.ConnectionConfiguration;
import com.clarkparsia.stardog.api.Query;

public class RDF2Subdue {
	
	public static void main(String args[]) {
		try {
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd - HH:mm:ss");
			Properties configFile = new Properties();
			InputStream in;
			try {
				in = new FileInputStream("config.properties");
				configFile.load(in);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			String server = configFile.getProperty("STARDOG_SERVER");
			String db = configFile.getProperty("STARDOG_DB");
			String user = configFile.getProperty("STARDOG_USER");
			String password = configFile.getProperty("STARDOG_PASS");
			String outputFile = configFile.getProperty("GRAPH_FILE");
			int vertexLimit = Integer.parseInt(configFile.getProperty("VERTEX_LIMIT"));
			
			System.out.println((String.format("[%s] Connecting to Stardog with URL %s and DB %s...", sdf.format(System.currentTimeMillis()), server, db)));
			Connection aConn = ConnectionConfiguration.to(db).url(server).credentials(user, password).connect();
			
			System.out.println(String.format("[%s] Connected!", sdf.format(System.currentTimeMillis())));
			System.out.println(String.format("[%s] Retrieving subjects...", sdf.format(System.currentTimeMillis())));
			Set<String> subjects = new HashSet<String>();
			Query aQuery = aConn.query("SELECT DISTINCT ?s WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }");
			TupleQueryResult aResult = aQuery.executeSelect();
			
			while (aResult.hasNext()) {
				BindingSet set = aResult.next();
				String subjectString = set.getBinding("s").getValue().toString();
				subjects.add(subjectString);
			}
			aResult.close();
			System.out.println(String.format("[%s] %s subjects found!", sdf.format(System.currentTimeMillis()), subjects.size()));
			
			System.out.println(String.format("[%s] Generating and writing nodes to output file...", sdf.format(System.currentTimeMillis())));
			
			String[] schemes = {"http", "https"};
			UrlValidator urlValidator = new UrlValidator(schemes);
			
			Map<String, Vertex> vertexMap = new HashMap<String, Vertex>();
			
			int vertexCount = 1;
			for (String subject : subjects) {
				Vertex subjectVertex = null;
				if (vertexMap.containsKey(subject)) {
					subjectVertex = vertexMap.get(subject);
				} else {
					aQuery = aConn.query(String.format("SELECT ?class WHERE { <%s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?class } LIMIT 1", subject));
					aResult = aQuery.executeSelect();
					String subjectClass =  null;
					while (aResult.hasNext()) {
						BindingSet set = aResult.next();
						subjectClass = set.getBinding("class").getValue().stringValue();
					}
					aResult.close();
					subjectVertex = new Vertex(vertexCount, subjectClass);
					vertexMap.put(subject, subjectVertex);
					vertexCount++;
				}
				aQuery = aConn.query(String.format("SELECT ?p ?o  WHERE { <%s> ?p ?o }", subject));
				aResult = aQuery.executeSelect();
				while (aResult.hasNext()) {
					Vertex objectVertex = null;
					BindingSet set = aResult.next();
					String predicate = set.getBinding("p").getValue().stringValue();
					String object = set.getBinding("o").getValue().stringValue();
					if (!vertexMap.containsKey(object)) {
						if (!predicate.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
							if (!urlValidator.isValid(object)) {
								objectVertex = new Vertex(vertexCount, "Literal");
								vertexMap.put(object, objectVertex);
								vertexCount++;
								subjectVertex.addVertex(objectVertex);
							} else {
								String objectClass = null;
								Query classQuery = aConn.query(String.format("SELECT ?class WHERE { <%s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?class } LIMIT 1", object));
								TupleQueryResult classResult = classQuery.executeSelect();
								while (classResult.hasNext()) {
									BindingSet classSet = classResult.next();
									objectClass = classSet.getBinding("class").getValue().toString();
								}
								classResult.close();
								if (objectClass == null) {
									objectClass = "URI";
								}
								objectVertex = new Vertex(vertexCount, objectClass);
								vertexMap.put(object, objectVertex);
								vertexCount++;
								subjectVertex.addVertex(objectVertex);
							}
						}
					} else {
						objectVertex = vertexMap.get(object);
						subjectVertex.addVertex(objectVertex);
					}
					
				}
				aResult.close();
			}
			
			System.out.println(String.format("[%s] Finished!", sdf.format(System.currentTimeMillis())));
		} catch (StardogException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	
}