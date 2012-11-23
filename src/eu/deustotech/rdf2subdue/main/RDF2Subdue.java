package eu.deustotech.rdf2subdue.main;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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
			
			System.out.println((String.format("[%s] Connecting to Stardog with URL %s and DB %s...", sdf.format(System.currentTimeMillis()), server, db)));
			Connection aConn = ConnectionConfiguration.to(db).url(server).credentials(user, password).connect();
			
			System.out.println(String.format("[%s] Connected!", sdf.format(System.currentTimeMillis())));
			System.out.println(String.format("[%s] Retrieving subjects...", sdf.format(System.currentTimeMillis())));
			Set<String> subjects = new HashSet<String>();
			Query aQuery = aConn.query("SELECT DISTINCT ?s WHERE { ?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o }");
			TupleQueryResult aResult = aQuery.executeSelect();

			while (aResult.hasNext()) {
				BindingSet set = aResult.next();
				subjects.add(set.getBinding("s").getValue().toString());
			}
			System.out.println(String.format("[%s] %s subjects found!", sdf.format(System.currentTimeMillis()), subjects.size()));

			//Retrieving objects
			System.out.println(String.format("[%s] Retrieving objects", sdf.format(System.currentTimeMillis())));
			Set<String> objects = new HashSet<String>();
			aQuery = aConn.query("SELECT DISTINCT ?o WHERE { ?s ?p ?o }");
			aResult = aQuery.executeSelect();
			
			while (aResult.hasNext()) {
				BindingSet set = aResult.next();
				objects.add(set.getBinding("o").getValue().toString());
			}
			aResult.close();
			System.out.println(String.format("[%s] %s Objects found!", sdf.format(System.currentTimeMillis()), objects.size()));
			objects.removeAll(subjects);
			System.out.println(String.format("[%s] After filtering, %s distinct objects found!", sdf.format(System.currentTimeMillis()), objects.size()));
			
			System.out.println(String.format("[%s] Generating and writing nodes to output file...", sdf.format(System.currentTimeMillis())));
			List<String> nodes = new ArrayList<String>();
			FileWriter out = new FileWriter(outputFile);
			//BufferedWriter out = new BufferedWriter(fstream);
			int i = 1;
			for (String subject : subjects) {
				aQuery = aConn.query(String.format("SELECT ?o WHERE { <%s> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?o } LIMIT 1", subject));
				aResult = aQuery.executeSelect();
				String type = "Unknown";
				while (aResult.hasNext()) {
					BindingSet set = aResult.next();
					type = set.getBinding("o").getValue().toString();
					break;
				}
				out.write(String.format("v %s \"%s\"\n", i, type));
				nodes.add(subject);
				i++;
				aResult.close();
			}
			out.flush();
			String[] schemes = {"http", "https"};
			UrlValidator urlValidator = new UrlValidator(schemes);
			String type = "Unknown";
			
			for (String object : objects) {
				if (urlValidator.isValid(object)) {
					type = "URL";
				} else {
					type = "Literal";
				}
				
				out.write(String.format("v %s \"%s\"\n", i, type));
				nodes.add(object);
				i++;
			}
			out.flush();
			
			System.out.println(String.format("[%s] Generating and writing edges to output file...", sdf.format(System.currentTimeMillis())));
			for (String subject: subjects) {
				int origin = nodes.indexOf(subject) + 1;
				aQuery = aConn.query(String.format("SELECT ?p ?o WHERE { <%s> ?p ?o }", subject));
				aResult = aQuery.executeSelect();
				while (aResult.hasNext()) {
					BindingSet set = aResult.next();
					int destination;
					if (!set.getBinding("p").getValue().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")) {
						destination = nodes.indexOf(set.getBinding("o").getValue().toString()) + 1;
						out.write(String.format("e %s %s \"%s\"\n", origin, destination, set.getBinding("p").getValue().toString()));
					}
				}
				aResult.close();
			}
			
			
			out.close();
			aConn.close();
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