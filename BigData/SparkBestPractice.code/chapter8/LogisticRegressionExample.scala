/*
LogisticRegressionExample.scala
author: linshifei  <linshifei365@gmail.com> 
version: 2014.12.24

代码变量命名上,这里按作者的个人习惯,和spark源码风格
由于数据量比较大,根据自己的集群情况，代码运行可能需要修改的默认配置有： 
      spark.local.dir     
      spark.executor.memory   
      spark.driver.maxResultSize
      spark.akka.frameSize  
      spark.default.parallelism
      spark.network.timeout
      spark.akka.timeout  
*/
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.classification.{LogisticRegressionWithSGD,LogisticRegressionWithLBFGS,LogisticRegressionModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.{RegressionMetrics,BinaryClassificationMetrics}
import org.apache.spark.mllib.optimization.{SquaredL2Updater, L1Updater}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.util.MLUtils

object LogisticRegressionExample {

    def main(args: Array[String]) {

      val conf = new SparkConf()
      val sc = new SparkContext(conf)

      var num_iterations = 50
      var train_algorithm = "SGD"
      var train_file_path = "hdfs:///data/track2_datasets_files_for_competitors/"
      var training_file_name = train_file_path + "training1kw.txt"
      var clear_threshold_test = false
      var reg_type = "L1"


      // 参数解析可以使用 scopt.OptionParser
      if(args.length >= 1){
          train_file_path =  args(0)
      }

      if(args.length >= 2){
          training_file_name = train_file_path  + args(1)
      }

      if(args.length >= 3){
          num_iterations = args(2).toInt
      }  

      if(args.length >= 4){
          train_algorithm = args(3)
      } 

      if(args.length >= 5){
          if(args(4) == "L2"){
              reg_type = "L2"
          }
      } 

      println("using train_file_path :" + train_file_path)
      println("using training_data :" + training_file_name)
      println("num_iterations :" + num_iterations.toString())
      println("train_algorithm :" + train_algorithm)
      println("reg_type :" + reg_type)

      // 使用kddcup2012数据集, refer:http://www.kddcup2012.org/c/kddcup2012-track2
      val training_data_raw = sc.textFile(training_file_name)
      val qid = sc.textFile(train_file_path + "queryid_tokensid.txt")
      val uid = sc.textFile(train_file_path + "userid_profile.txt")
      val keywords = sc.textFile(train_file_path + "purchasedkeywordid_tokensid.txt")
      val title = sc.textFile(train_file_path + "titleid_tokensid.txt")
      val description = sc.textFile(train_file_path + "descriptionid_tokensid.txt")

       // keywords 格式是：id x|y
      // 转换为id 为 key,其他为value (array)
      val m_keywords = keywords.map(line => {
        val arr=line.split('\t');
        (arr(0), arr(1).split('|'))
      }).reduceByKey((a1, a2) => a1).cache()

      // title 格式是：id x|y
      // 转换为id 为 key,其他为value (array)
      val m_title = title.map(line => {
        val arr=line.split('\t');
        (arr(0), arr(1).split('|'))
      }).reduceByKey((a1, a2) => a1).cache()

      // description 格式是：id x|y
      // 转换为id 为 key,其他为value (array)
      val m_description = description.map(line => {
        val arr=line.split('\t');
        (arr(0), arr(1).split('|'))
      }).reduceByKey((a1, a2) => a1).cache()

      // uid 格式是：uid 1 2 3
      // 转换为uid 为 key,其他为value (array)
      val m_uid = uid.map(line => {
        val uid_array=line.split('\t');
        (uid_array(0), uid_array.drop(1))  
      }).reduceByKey((a1, a2) => a1)

     
      // qid 格式是：qid x|y
      // 转换为qid 为 key,其他为value (array)
      val m_qid = qid.map(line => {
        val arr=line.split('\t');
        (arr(0), arr(1).split('|'))
      }).reduceByKey((a1, a2) => a1)

      // 给每个session 加一个编号
      val training_data_raw_idx = training_data_raw.zipWithIndex().map(x => (String.valueOf(x._2), x._1))

      /*
      展开training_data_raw，并保留原行数据 ：
      1. Click: as described in the above list. 
      2. Impression: as described in the above list. 
      4. AdID: as described in the above list. 
      5. AdvertiserID: a property of the ad. 
      8. QueryID:  id of the query. 
      9. KeywordID: a property of ads. 
      10. TitleID: a property of ads. 
      11. DescriptionID: a property of ads. 
      12. UserID 
      主要过程是：line => (("type",i,p._2(1)), instance)
      其中，instance(7)是 query id  , instance(11)是  user id  ...
      join 默认２边都有的记录
      */
      val train_instance_expand = training_data_raw_idx.map(l => {
        val arr = l._2.split('\t')
        // instance 只保留 :index,Click ,Impression, UserID ,AdID ，AdvertiserID,query,title,description,Position
        (arr(8), Array(l._1,arr(0),arr(1),arr(11),arr(3),arr(4),arr(7),arr(9),arr(10),arr(6)))  
      }).join(m_keywords).map(a => (a._2._1(7),a._2)  // value :  instance,keywords_token
      ).join(m_title).map(a => (a._2._1._1(8),(a._2._1._1,a._2._1._2,a._2._2))  // value :  instance,keywords_token，title_token
      ).join(m_description).map(a => (a._2._1._1(6),(a._2._1._1,a._2._1._2,a._2._1._3,a._2._2))   // value :  instance,keywords_token，title_token,description_token
      ).join(m_qid).map(a => (a._2._1._1(3), (a._2._1._1,a._2._1._2,a._2._1._3,a._2._1._4,a._2._2)) // value : instance,keywords_token，title_token,description_token,qid_arr
      ).join(m_uid).map(a => (a._2._1._1,a._2._1._2,a._2._1._3,a._2._1._4,a._2._1._5,a._2._2) // value : instance,keywords_token，title_token,description_token,qid_arr,uid_arr
      ).flatMap( p => {  // p的格式是 1:instance,2:keywords_token，3:title_token,4:description_token,5:qid_arr,6:uid_arr

      var array_size = p._6.length  //   AdvertiserID 和 user 交叉
      array_size =  array_size + p._5.length  //  AdID  和 query_token 交叉,每次请求AdID只有一个
      array_size =  array_size + p._5.length  //  AdvertiserID 和 query_token 交叉
      array_size =  array_size + 3  //  title keyword  description是否包含query

      val a = new Array[((String,String, String), Array[String])](array_size)
      var n = 0

      var token_hit_count = 0

      token_hit_count = checkTokenMatch(p._2,p._5)
      a(n) = (("1","1",token_hit_count.toString()), p._1) //  标识第一类特征  title keyword  description是否包含query
      n += 1         
     
      token_hit_count = checkTokenMatch(p._3,p._5)
      a(n) = (("1","2",token_hit_count.toString()), p._1)  
      n += 1         
      
      token_hit_count = checkTokenMatch(p._4,p._5)
      a(n) = (("1","3",token_hit_count.toString()), p._1)  
      n += 1         

      for (i <- p._5){    // 展开 query_token
          a(n) = (("2",i,p._1(4)), p._1) // 标识第二类AdID  和 query_token 交叉，  p._1 是instance
          n += 1
        }

      for (i <- p._6){    // 展开user
          a(n) = (("3",i,p._1(5)), p._1) // 标识AdvertiserID 和 user 交叉 
          n += 1
        }

       for (i <- p._5){    // 展开query_token
          a(n) = (("4",i,p._1(5)), p._1) // 标识AdvertiserID 和 query_token 交叉
          n += 1
        }       

        a
      }).cache()  

      //train_instance_expand 是  (feature_list,instance) ,比如： (("type",i,p._2(1)), p._1)
      //需要去重,本地加编号n  
      var n = 0

      val feature_index = train_instance_expand.reduceByKey((a1, a2) => a1).map(_._1).collect().map(a => {
        n += 1
        (a, n)
      })

      // collect 以后是数组arrray ，重新回到 RDD
      val feature_index_rdd = sc.parallelize(feature_index)

      // 以每个instance的feature 列表做作为key 进行join ，join得到每一种instance需要的feature编号: ((feature_list) ,(instance,feature_id))
      // 然后反转:以每一个 instance 为key，这里需要倒过来，即对同一个组合 groupByKey。
      val train_instance = train_instance_expand.join(feature_index_rdd).map(a => {(a._2._1.mkString("\t"), a._2._2)}   
      ).groupByKey().map( a => {
        val arr = a._1.split('\t')  // mkstring 以后是 字符串
        var s = ""
        for(n <- a._2) {    //  groupByKey 以后 同一个类型组合所有的 编号在这里
          s = s + "\t" + n + "\t1"  // 这里使用稀疏矩阵 ，1 表示这一维不为空
        }
        arr(1) + "\t" + arr(2) + s
      })

    // 这里需要过滤 点击大于曝光  曝光大于0的情况
    // zipWithIndex 每个元素和其所在的下标组成一个pair
    val train_instance_filter = train_instance.map(_.split("\t").zipWithIndex).filter(arrWithIdx => 
      arrWithIdx(0)._1.toInt <= arrWithIdx(1)._1.toInt && arrWithIdx(1)._1.toInt > 0
    )

    //  最大的编号
    val maxSize = n + 1

    val train_instance_flat = train_instance_filter.flatMap { arrWithIdx => {
        val feature_index_arr = arrWithIdx.filter(i => i._2 > 1 && i._2 % 2 == 0).map(_._1.toInt)
        val feature_value_arr = arrWithIdx.filter(i => i._2 > 1 && i._2 % 2 == 1).map(_._1.toDouble)

        // 点击数 ，正例数
        val click_count = arrWithIdx(0)._1.toInt
        // 曝光数，负例数
        val imp_count = arrWithIdx(1)._1.toInt
        var outs = new Array[(LabeledPoint)](imp_count)

        for (i <- click_count until imp_count)
            outs(i-click_count)         = LabeledPoint(0, Vectors.sparse(maxSize, feature_index_arr, feature_value_arr)) 
            
        for (i <- 0 until click_count )
            outs(i+imp_count-click_count) = LabeledPoint(1, Vectors.sparse(maxSize, feature_index_arr, feature_value_arr)) 
        outs
    }}.cache()


    val num_training = train_instance_flat.count()
    println(s"Training instance count: $num_training")

     val updater = reg_type match {
      case "L1" => new L1Updater()
      case "L2" => new SquaredL2Updater()
    }

    val model = train_algorithm match {
      case "LBFGS" =>
        val algorithm = new LogisticRegressionWithLBFGS()
        algorithm.optimizer
          .setNumIterations(num_iterations)
          .setUpdater(updater)
        algorithm.run(train_instance_flat)
      case "SGD" =>
        val algorithm = new LogisticRegressionWithSGD()
        algorithm.optimizer
          .setNumIterations(num_iterations)
          .setUpdater(updater)
        algorithm.run(train_instance_flat)
    }

    model.clearThreshold()
  
    val weight_count = model.weights.size
    println(s"\nModel weights count :${weight_count}")

    val prediction = model.predict(train_instance_flat.map(_.features))
    val score_and_labels = prediction.zip(train_instance_flat.map(_.label))
    val metrics = new BinaryClassificationMetrics(score_and_labels)
    println(s"Test areaUnderROC = ${metrics.areaUnderROC()}.")

    val metrics2 = new RegressionMetrics(score_and_labels)
    println(s"\nTest  meanAbsoluteError = ${metrics2.meanAbsoluteError}")

    val  date_format = new SimpleDateFormat("yyyyMMddHHmm")
    val  model_path =   train_file_path + "model_"  +  date_format.format(new Date())
    model.save(sc, model_path)
    sc.stop()
    
    }

    def checkTokenMatch(token1: Array[String],token2: Array[String]) : Int = {
      if(token1.length ==0 || token2.length ==0){
         return 0
      }

      var hit = 0
      for( item1 <- token1){
        for(item2 <- token2){
            if(item1 == item2){
               hit += 1
            }          
        }
      }
      return hit
    }
}
