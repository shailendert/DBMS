package util;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class Graph<T> {
	private List<Node> node;

	public Graph(String name) {
		node = new LinkedList<Node>();
	}

	public synchronized boolean addNode(T data) {
		// already in the graph
		for (Node nn : this.node) {
			if (nn.data.equals(data))
				return false;
		}

		Node n = new Node(data);
		node.add(n);

		return true;
	}

	/**
	 * 
	 * @param data
	 */
	public synchronized void delNode(T data) {
		Node n = findNode(data);
		if (n == null)
			new util.Bug(data + "not exsit");

		delNode(n);

	}

	private synchronized void delNode(Node node) {
		// first deal the edge
		for (Edge e : node.edge) {
			if (e.from != null)// must not null
				e.from.outdegree--;

			if (e.to != null)// maybe null
				e.to.indegree--;
			else
				node.dealNullEdge(e);
		}

		/*
		 *edge 
		 */
		this.node.remove(node);

	}

	/**
	 * Node
	 * 
	 * @param data
	 * @return
	 */
	public Node findNode(T data) {
		for (Node n : this.node) {
			if (n.data.equals(data))
				return n;
		}
		return null;
	}

	public synchronized boolean addEdge(T from, T to) {
		Node f = findNode(from);
		Node t = findNode(to);

		if (f == null || t == null)
			return false;

		return addEdge(f, t);
	}

	private synchronized boolean addEdge(Node from, Node to) {
		Edge edge = new Edge(from, to);
		from.outdegree++;
		to.indegree++;
		from.edge.add(edge);
		return true;
	}

	private synchronized boolean dfs(Node n, Set<Graph<T>.Node> visited) {
		if (visited.contains(n))
			return true;

		boolean isCycle = false;
		visited.add(n);
		for (Edge e : n.edge) {
			if (e.to != null) {
				isCycle = dfs(e.to, visited);
				if (isCycle)
					return true;
			} else {
				n.dealNullEdge(e);
			}
		}

		return false;
	}

	/**
	 * 
	 * @param data
	 * @return true is exsit cycle, false if no cycle.
	 */
	public synchronized boolean isCycle(T data) {
		Node node = findNode(data);
		if (node == null)
			new util.Bug("data not in the graph");

		Set<Node> visited = new HashSet<Node>();

		return dfs(node, visited);

	}

	/**
	 * Class Node
	 * 
	 *
	 */
	class Node {
		private T data;
		private Set<Edge> edge;
		private Integer indegree;
		private Integer outdegree;

		public Node(T data) {
			this.data = data;
			this.edge = new HashSet<Edge>();
			this.indegree = 0;
			this.outdegree = 0;
		}

		private synchronized void dealNullEdge(Edge e) {
			edge.remove(e);
		}

	}

	/**
	 * Class Edge
	 *
	 *
	 */
	class Edge {
		private Node from;
		private Node to;

		public Edge(Node from, Node to) {
			this.from = from;
			this.to = to;

		}

	}

}
