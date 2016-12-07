package edu.berkeley.cs186.database.concurrency;

import java.util.*;

/**
 * A waits for graph for the lock manager (used to detect if
 * deadlock will occur and throw a DeadlockException if it does).
 */
public class WaitsForGraph {
  private Boolean cycleExists;
    Map<Long, Boolean> marked;// = new HashMap<>();
    Map<Long, Boolean> onStack;// = new HashMap<>();
  // We store the directed graph as an adjacency list where each node (transaction) is
  // mapped to a list of the nodes it has an edge to.
  private Map<Long, ArrayList<Long>> graph;

  public WaitsForGraph() {
    graph = new HashMap<Long, ArrayList<Long>>();
    this.cycleExists = false;
     this.marked = new HashMap<>();
     this.onStack = new HashMap<>();
  }

  public boolean getCycleSwitch() {
      return this.cycleExists;
  }

  public boolean containsNode(long transNum) {
    return graph.containsKey(transNum);
  }

  protected void addNode(long transNum) {
    if (!graph.containsKey(transNum)) {
      graph.put(transNum, new ArrayList<Long>());
    }
  }

  protected void addEdge(long from, long to) {
    if (!this.edgeExists(from, to)) {
      ArrayList<Long> edges = graph.get(from);
      edges.add(to);
    }
  }

  protected void removeEdge(long from, long to) {
    if (this.edgeExists(from, to)) {
      ArrayList<Long> edges = graph.get(from);
      edges.remove(to);
    }
  }

  protected boolean edgeExists(long from, long to) {
    if (!graph.containsKey(from)) {
      return false;
    }
    ArrayList<Long> edges = graph.get(from);
    return edges.contains(to);
  }

  /**
   * Checks if adding the edge specified by to and from would cause a cycle in this
   * WaitsForGraph. Does not actually modify the graph in any way.
   * @param from the transNum from which the edge points
   * @param to the transNum to which the edge points
   * @return
   */
  protected boolean edgeCausesCycle(long from, long to) {
    //TODO: Implement Me!!


   this.marked = new HashMap<>();
   this.onStack = new HashMap<>();
//
    this.addEdge(from, to);
////      this.addNode(from);
////      this.addNode(to);

    for (Long node : this.graph.keySet()) {
        marked.put(node, false);
        onStack.put(node, false);
    }
      findCycle(from);
      this.removeEdge(from, to);

    return this.cycleExists;


  }

    /*
    * Credit to http://stackoverflow.com/questions/19113189/detecting-cycles-in-a-graph-using-dfs-2-different-approaches-and-whats-the-dif
    * */
    public void findCycle(long v) {
        marked.put(v, true);
        onStack.put(v, true);


        for (Long neighbor : graph.get(v)) {
            if (this.marked.containsKey(neighbor) && !this.marked.get(neighbor)) {
                findCycle(neighbor);

            } else if (this.onStack.containsKey(neighbor) && this.onStack.get(neighbor)) {
                this.cycleExists = true;
                return;
            }
        }
//    }
        onStack.put(v, false);
        return;
    }


}
