package eu.deustotech.rdf2subdue.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
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
			Query aQuery = aConn.query("SELECT DISTINCT ?s WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }");
			TupleQueryResult aResult = aQuery.executeSelect();
			
			Map<Integer, Vertex> subjectsById = new HashMap<Integer, Vertex>();
			Map<String, Vertex> subjectsByURI = new HashMap<String, Vertex>();
			int id = 1;
			while (aResult.hasNext()) {
				BindingSet set = aResult.next();
				String subjectString = set.getBinding("s").getValue().toString();
				Query classQuery = aConn.query(String.format("SELECT ?class WHERE { <%s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?class } LIMIT 1", subjectString));
				TupleQueryResult classResult = classQuery.executeSelect();
				String subjectClass = null;
				while (classResult.hasNext()) {
					BindingSet classSet = classResult.next();
					subjectClass = classSet.getBinding("class").getValue().stringValue();
				}
				classResult.close();
				Vertex vertex = new Vertex(id, subjectClass, subjectString);
				subjectsById.put(id, vertex);
				subjectsByURI.put(subjectString, vertex);
				id++;
			}
			aResult.close();
			System.out.println(String.format("[%s] %s subjects found!", sdf.format(System.currentTimeMillis()), subjectsById.size()));
			
			System.out.println(String.format("[%s] Generating and writing nodes to output file...", sdf.format(System.currentTimeMillis())));
			
			String[] schemes = {"http", "https"};
			UrlValidator urlValidator = new UrlValidator(schemes);
			
			Map<String, Vertex> objectsByURI = new HashMap<String, Vertex>();
			Map<Integer, Vertex> objectsById = new HashMap<Integer, Vertex>();
			
			for (int i = 1; i <= subjectsById.size(); i++) {
				Vertex subjectVertex = subjectsById.get(i);
				Query objectQuery = aConn.query(String.format("SELECT DISTINCT ?o WHERE { <%s> ?p ?o }", subjectVertex.getUri()));
				TupleQueryResult objectResult = objectQuery.executeSelect();
				while (objectResult.hasNext()) {
					BindingSet objectSet = objectResult.next();
					String object = objectSet.getBinding("o").getValue().stringValue();
					Vertex objectVertex = null;
					if (!subjectsByURI.containsKey(object)) {
						if (!objectsByURI.containsKey(object)) {
							String label =  null;
							
							if (!urlValidator.isValid(object)) {
								label = "Literal";
							} else {
								label = "URI";
							}
							
							objectVertex = new Vertex(id, label, object);
							objectsByURI.put(object, objectVertex);
							objectsById.put(id, objectVertex);
							id++;
							
						} else {
							objectVertex = objectsByURI.get(object);
						}
					} else {
						objectVertex = subjectsByURI.get(object);
					}
					
					subjectVertex.addVertex(objectVertex);
					
				}
				objectResult.close();
				subjectsById.put(subjectVertex.getId(), subjectVertex);
			}
				
			System.out.println(subjectsById.size());
			System.out.println(objectsById.size());			
			
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