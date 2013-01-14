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

class Tuple {
	
	private String x;
	private String y;
	
	public Tuple(String x, String y) {
		this.x = x;
		this.y = y;
	}

	public String getX() {
		return x;
	}

	public void setX(String x) {
		this.x = x;
	}

	public String getY() {
		return y;
	}

	public void setY(String y) {
		this.y = y;
	}
	
	
	
}

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
			
			List<String> nodes = new ArrayList<String>();
			Map<Integer,Set<String>> edges = new HashMap<Integer,Set<String>>();
						
			for (String subject : subjects) {
				Query objectQuery = aConn.query(String.format("SELECT ?o ?p WHERE { <%s> ?p ?o }", subject));
				TupleQueryResult objectResult = objectQuery.executeSelect();
				Set<String> tempEdgeSet = new HashSet<String>();
				Set<Tuple> objectSet = new HashSet<Tuple>();
				while(objectResult.hasNext()) {
					BindingSet set = objectResult.next();
					String objectString = set.getBinding("o").getValue().toString();
					String propertyString = set.getBinding("p").getValue().toString();
					if (!propertyString.equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {				
						nodes.add(objectString);
						objectSet.add(new Tuple(objectString, propertyString));
					}
				}
							
				nodes.add(subject);	
				
				int subjectNum = nodes.indexOf(subject) + 1;
				
				for (Tuple tuple : objectSet) {
					int objectNum = nodes.indexOf(tuple.getX()) + 1;
					tempEdgeSet.add(String.format("e %s %s %s\n", subjectNum, objectNum, tuple.getY()));
				}
				
				objectResult.close();
				edges.put(subjectNum, tempEdgeSet);
			}

			
			/*int offset = 0;
			int i = 1;
			String[] schemes = {"http", "https"};
			UrlValidator urlValidator = new UrlValidator(schemes);
			//TODO: Paralelizar
			while (offset < nodes.size()) {
				FileWriter out = new FileWriter(String.format("%s_%s.g", outputFile, i));
				int limit = (i * vertexLimit);
				if (limit > nodes.size()) {
					limit = nodes.size();
				}
				List<String> nodeSubList = nodes.subList(offset, limit);
				for (String node : nodeSubList) {
					String nodeType = null;
					if (urlValidator.isValid(node)) {
						Query classQuery = aConn.query(String.format("SELECT ?o WHERE { <%s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o } LIMIT 1", node));
						TupleQueryResult classResult = classQuery.executeSelect();			
						while (classResult.hasNext()) {
							BindingSet classSet = classResult.next();
							nodeType = classSet.getBinding("o").getValue().toString();
						}
						classResult.close();
						if (nodeType == null) {
								nodeType = "URL";
						}
					} else {
						nodeType = "Literal";
					}
					
					out.write(String.format("v %s %s\n", nodes.indexOf(node) + 1, nodeType));
					
				}
				for (String node : nodeSubList) {
					Set<String> edgeSet = edges.get(nodes.indexOf(node) + 1);
					if (edgeSet != null) {
						for (String edge : edgeSet) {
							out.write(edge);
						}
					}
				}
				out.close();
				offset += vertexLimit;
				i++;
			}*/
			
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