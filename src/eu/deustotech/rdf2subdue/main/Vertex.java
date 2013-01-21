package eu.deustotech.rdf2subdue.main;

import java.util.HashSet;
import java.util.Set;

public class Vertex implements Comparable<Vertex> {
	
	private int id;
	private String label;
	private Set<Vertex> destVertex;
	private String uri;
	private String property;
	
	public Vertex(int id, String label, String uri) {
		this.id = id;
		this.label = label;
		this.destVertex = new HashSet<Vertex>();
		this.uri = uri;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public Set<Vertex> getVertex() {
		return destVertex;
	}

	public void setVertex(Set<Vertex> destVertex) {
		this.destVertex = destVertex;
	}
	
	public void addVertex(Vertex vertex) {
		this.destVertex.add(vertex);
	}

	public String getUri() {
		return uri;
	}

	public void setUri(String uri) {
		this.uri = uri;
	}

	public String getProperty() {
		return property;
	}

	public void setProperty(String property) {
		this.property = property;
	}

	@Override
	public int compareTo(Vertex o) {
		return this.getId() - o.getId();
	}
	
	
	
}
