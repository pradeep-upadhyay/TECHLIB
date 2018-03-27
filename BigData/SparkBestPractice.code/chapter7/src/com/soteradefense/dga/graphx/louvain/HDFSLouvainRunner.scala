package com.soteradefense.dga.graphx.louvain

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._

/**
 * Execute the louvain algorithim and save the vertices and edges in hdfs at each level.
 * Can also save locally if in local mode.
 * 
 * See LouvainHarness for algorithm details
 */
class HDFSLouvainRunner(minProgress:Int,progressCounter:Int,hdfspath:String, outputdir:String) extends LouvainHarness(minProgress, progressCounter) {

  var qValues = Array[(Int,Double)]()
  var hdfsconf = new Configuration()
  var hdfs = FileSystem.get(hdfsconf)
  //val qvalpath = outputdir+"/qvalues"
  
  override def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
	  graph.vertices.saveAsTextFile(hdfspath+outputdir+"/level_"+level+"_vertices")
      graph.edges.saveAsTextFile(hdfspath+outputdir+"/level_"+level+"_edges")
      //graph.vertices.map( {case (id,v) => ""+id+","+v.internalWeight+","+v.community }).saveAsTextFile(outputdir+"/level_"+level+"_vertices")
      //graph.edges.mapValues({case e=>""+e.srcId+","+e.dstId+","+e.attr}).saveAsTextFile(outputdir+"/level_"+level+"_edges")  
      qValues = qValues :+ ((level,q))
      println(s"qValue: $q")
      
      
      // overwrite the q values at each level   
      //hdfs.delete(new Path("hdfs://m1.hadoop"+qvalpath), true)
      sc.parallelize(qValues, 1).saveAsTextFile(hdfspath+outputdir+"/qvalues_"+level)
  }
  
}