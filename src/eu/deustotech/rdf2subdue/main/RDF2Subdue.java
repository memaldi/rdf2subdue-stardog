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
			
			String[] schemes = {"http", "https"};
			UrlValidator urlValidator = new UrlValidator(schemes);
			
			Map<Integer, Vertex> subjectsById = new HashMap<Integer, Vertex>();
			Map<String, Vertex> subjectsByURI = new HashMap<String, Vertex>();
			int id = 1;
			while (aResult.hasNext()) {
				BindingSet set = aResult.next();
				String subjectString = set.getBinding("s").getValue().toString();
				if (urlValidator.isValid(subjectString)) {
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
			}
			aResult.close();
			System.out.println(String.format("[%s] %s subjects found!", sdf.format(System.currentTimeMillis()), subjectsById.size()));
			
			System.out.println(String.format("[%s] Generating nodes and edges...", sdf.format(System.currentTimeMillis())));
			
			
			
			//Map<String, Vertex> objectsByURI = new HashMap<String, Vertex>();
			//Map<Integer, Vertex> objectsById = new HashMap<Integer, Vertex>();
			
			for (int i = 1; i <= subjectsById.size(); i++) {
				Vertex subjectVertex = subjectsById.get(i);
				Query objectQuery = aConn.query(String.format("SELECT ?o ?p WHERE { <%s> ?p ?o }", subjectVertex.getUri()));
				TupleQueryResult objectResult = objectQuery.executeSelect();
				while (objectResult.hasNext()) {
					BindingSet objectSet = objectResult.next();
					String object = objectSet.getBinding("o").getValue().stringValue();
					String property = objectSet.getBinding("p").getValue().stringValue();
					Vertex objectVertex = null;
					if (!property.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
						if (subjectsByURI.containsKey(object)) {
							objectVertex = subjectsByURI.get(object);
							objectVertex.setProperty(property);
							subjectVertex.addVertex(objectVertex);
						}
						
						
					}
				}
				objectResult.close();
				subjectsById.put(subjectVertex.getId(), subjectVertex);
			}
							
			
			Map<Integer, List<Vertex>> nodeMap = new HashMap<Integer, List<Vertex>>();
			Map<Integer, Set<String>> edgeMap = new HashMap<Integer, Set<String>>();
			
 			for (int key : subjectsById.keySet()) { 
				Vertex subjectVertex = subjectsById.get(key);
				int nodeFile = (subjectVertex.getId() / vertexLimit) + 1;
				if (!nodeMap.containsKey(nodeFile)) {
					nodeMap.put(nodeFile, new ArrayList<Vertex>());
				}
				
				List<Vertex> nodeList = nodeMap.get(nodeFile);
				//nodeList.add(String.format("v %s %s\n", subjectVertex.getId(), subjectVertex.getLabel()));
				nodeList.add(subjectVertex);
				nodeMap.put(nodeFile, nodeList);
				
				Set<Vertex> relatedVertex = subjectVertex.getVertex();
				
				for (Vertex vertex : relatedVertex) {
					int edgeFile;
					if (vertex.getId() > subjectVertex.getId()) {
						edgeFile = (vertex.getId() / vertexLimit) + 1;
					} else {
						edgeFile = (subjectVertex.getId() / vertexLimit) + 1;
					}
					
					if (!edgeMap.containsKey(edgeFile)) {
						edgeMap.put(edgeFile, new HashSet<String>());
					}
					
					Set<String> edgeList = edgeMap.get(edgeFile);
					edgeList.add(String.format("e %s %s %s\n", subjectVertex.getId(), vertex.getId(), vertex.getProperty()));
					edgeMap.put(edgeFile, edgeList);
				}
			}
 			
 			/*for (int key : objectsById.keySet()) {
 				Vertex objectVertex = objectsById.get(key);
 				int nodeFile = (objectVertex.getId() / vertexLimit) + 1;
 				if (!nodeMap.containsKey(nodeFile)) {
 					nodeMap.put(nodeFile, new ArrayList<Vertex>());
 				}
 				
 				List<Vertex> nodeList = nodeMap.get(nodeFile);
 				//nodeList.add(String.format("v %s %s\n", objectVertex.getId(), objectVertex.getLabel()));
 				nodeList.add(objectVertex);
 				nodeMap.put(nodeFile, nodeList);
 			}*/
 			
 			System.out.println(String.format("[%s] Writing nodes and edges to output file(s)...", sdf.format(System.currentTimeMillis())));
			
 			
 			for (int key : nodeMap.keySet()) {
 				FileWriter out = new FileWriter(String.format("%s_%s.g", outputFile, key));
 				List<Vertex> nodeList = nodeMap.get(key);
 				Collections.sort(nodeList);
 				for (Vertex vertex : nodeList) {
 					out.write(String.format("v %s %s\n", vertex.getId(), vertex.getLabel()));
 				}
 				out.write("%\n");
 				Set<String> edgeList = edgeMap.get(key);
 				if (edgeList != null) {
	 				for (String edge : edgeList) {
	 					out.write(edge);
	 				}
 				}
 				out.close();
 			}
 			
			System.out.println(String.format("[%s] Finished!", sdf.format(System.currentTimeMillis())));
		} catch (StardogException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (QueryEvaluationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	
}