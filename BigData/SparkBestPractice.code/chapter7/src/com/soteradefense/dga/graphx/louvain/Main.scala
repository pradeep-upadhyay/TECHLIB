package com.soteradefense.dga.graphx.louvain

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._


// specify command line options and their defaults
case class Config(
    input:String = "/user/ningliu/tw.data",
    //output: String = "/data/weibo_data/result",
    output: String = "/user/ningliu/result",
    //hdfs: String = "hdfs://admin.hdfs.test.namenode0",
    //hdfspath: String = "hdfs://solr1.bi",
    hdfspath: String = "hdfs://",
    //hdfspath: String = "",
    master:String="local",
    appName:String="graphX analytic",
    jars:String="",
    sparkHome:String="",
    edgedelimiter:String = " ",
    parallelism:Int = 100,
    minProgress:Int = 2000,
    progressCounter:Int = 2,
    ipaddress: Boolean = false,
    properties:Seq[(String,String)]= Seq.empty[(String,String)] )


/**
 * Execute the louvain distributed community detection.
 * Requires an edge file and output directory in hdfs (local files for local mode only)
 */
object Main {
  
  def main(args: Array[String]) {
    var edgeFile, outputdir,master,jobname,jars,sparkhome ,edgedelimiter = ""
    var hdfspath = ""
    var properties:Seq[(String,String)]= Seq.empty[(String,String)]
    var parallelism,minProgress,progressCounter = -1
    var ipaddress = false
    
    // Parse Command line options
    /*
    val parser = new scopt.OptionParser[Config](this.getClass().toString()){
      opt[String]('i',"input") action {(x,c)=> c.copy(input=x)}  text("input file or path  Required.")
      opt[String]('o',"output") action {(x,c)=> c.copy(output=x)} text("output path Required")
      opt[String]('m',"master") action {(x,c)=> c.copy(master=x)} text("spark master, local[N] or spark://host:port default=local")
      opt[String]('h',"sparkhome") action {(x,c)=> c.copy(sparkHome=x)} text("SPARK_HOME Required to run on cluster")
      opt[String]('n',"jobname") action {(x,c)=> c.copy(appName=x)} text("job name")
      opt[Int]('p',"parallelism") action {(x,c)=> c.copy(parallelism=x)} text("sets spark.default.parallelism and minSplits on the edge file. default=based on input partitions")
      opt[Int]('x',"minprogress") action {(x,c)=> c.copy(minProgress=x)} text("Number of vertices that must change communites for the algorithm to consider progress. default=2000")
       opt[Int]('y',"progresscounter") action {(x,c)=> c.copy(progressCounter=x)} text("Number of times the algorithm can fail to make progress before exiting. default=1")
      opt[String]('d',"edgedelimiter") action {(x,c)=> c.copy(edgedelimiter=x)} text("specify input file edge delimiter. default=\",\"")
      opt[String]('j',"jars") action {(x,c)=> c.copy(jars=x)} text("comma seperated list of jars")
      opt[Boolean]('z',"ipaddress") action {(x,c)=> c.copy(ipaddress=x)} text("Set to true to convert ipaddresses to Long ids. Defaults to false")
      arg[(String,String)]("<property>=<value>....") unbounded() optional() action {case((k,v),c)=> c.copy(properties = c.properties :+ (k,v)) }
    }
    
    parser.parse(args,Config()) map {
      config =>
        edgeFile = config.input
        outputdir = config.output
        master = config.master
        jobname = config.appName
        jars = config.jars
        sparkhome = config.sparkHome
        properties = config.properties
        parallelism = config.parallelism
        edgedelimiter = config.edgedelimiter
        minProgress = config.minProgress
        progressCounter = config.progressCounter
        ipaddress = config.ipaddress
        if (edgeFile == "" || outputdir == "") {
          println(parser.usage)
          sys.exit(1)
        }
    } getOrElse{
      sys.exit(1)
    }
    */
    
    var config = new Config()
    master = config.master
    jobname = config.appName
    
    edgeFile = args(0) //config.input
    outputdir = config.output
    hdfspath = config.hdfspath
    parallelism = config.parallelism
    edgedelimiter = config.edgedelimiter
    minProgress = config.minProgress
    progressCounter = config.progressCounter
    ipaddress = config.ipaddress
    
    
    // set system properties
    properties.foreach( {case (k,v)=>
      println(s"System.setProperty($k, $v)")
      System.setProperty(k, v)
    })
    
    // Create the spark context
    var sc: SparkContext = null
    if (master.indexOf("local") == 0 ){
      println(s"sparkcontext = new SparkContext($master,$jobname)")
      val conf = new SparkConf().set("spark.driver.maxResultSize", "10g").set("spark.local.dir","/data/hadoop/datanodedir/").setAppName(jobname)
      sc = new SparkContext(conf)
      //sc = new SparkContext(new SparkConf().setAppName(jobname))
    }
    else{
      println(s"sparkcontext = new SparkContext($master,$jobname,$sparkhome,$jars)")
      sc = new SparkContext(master,jobname,sparkhome,jars.split(","))
    }
   
    // read the input into a distributed edge list
    val inputHashFunc = if (ipaddress) (id:String) => IpAddress.toLong(id) else (id:String) => id.toLong
    //var rowCount = 0
    
    /**
     * 读取数据文件，每行格式：id1 id2，表示id1和id2的关系好
     * 这里我们使用无向图
     *
     */
    var edgeRDD = sc.textFile(edgeFile).map(row=> {
	      val tokens = row.split(edgedelimiter).map(_.trim())
	      tokens.length match {
	        case 2 => {new Edge(inputHashFunc(tokens(0)),inputHashFunc(tokens(1)),1L) }
	        case 3 => {new Edge(inputHashFunc(tokens(0)),inputHashFunc(tokens(1)),tokens(2).toLong)}
	        //case _ => {throw new IllegalArgumentException("invalid input line: "+row)}
          case _ => { new Edge(0, 0, 1L)}
	      }
	  })	
   
    println(s"total rows: "+edgeRDD.count())
	
	  // 根据配置项决定是否要把当前的RDD分片，并行处理
    // 默认会根据hdfs存储的block数作为分片数
	  if (parallelism != -1 ) edgeRDD = edgeRDD.coalesce(parallelism,shuffle=true)
  
    // 将RDD转换为图
    val graph = Graph.fromEdges(edgeRDD, None)
  
    // 这里使用HDFSLouvainRunner类，如果需要将输出另外处理，可以通过继承LouvainHarness类实现
    val runner = new HDFSLouvainRunner(minProgress,progressCounter,hdfspath,outputdir)
    runner.run(sc, graph)
    

  }
  

  
}



