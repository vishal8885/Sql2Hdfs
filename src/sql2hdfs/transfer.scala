package sql2hdfs

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.SparkContext
import java.io.File
import java.util.Date
import javax.swing.JOptionPane

object transfer {
  def run(url : String, driver : String, user : String, password : String, table : String, query : String, destination : String){
    val sw = new StopWatch()
    System.setProperty("hadoop.home.dir", "C:\\Users\\tarun\\Documents\\vishal\\winutils-master\\hadoop-2.6.0")
      val conf = new SparkConf()
                  .setAppName("sql2hdfs")
                  .setMaster("local")
                  .set("spark.driver.allowMultipleContexts", "true");
    
			val sc = new SparkContext(conf)
      
			val sqlContext=new SQLContext(sc);
      
			val spark=sqlContext.sparkSession
					val df = spark.read.format("org.apache.spark.sql.execution.datasources.jdbc.JdbcRelationProvider")
					.option("url", url)
					.option("driver", driver)
					.option("dbtable", table)
					.option("user", user)
					.option("password", password)
					.load()
					
					df.registerTempTable(table)
			    
					try{
					  sw.start();
					  val dfe = spark.sql(query)
					}
					catch{
					  case e: Exception =>{
					    printf(e.printStackTrace().toString())
					  }
					}
					finally{
					  val dfe = spark.sql(query)
					dfe.repartition(1).write
					.format("com.databricks.spark.csv")
					.option("header",true)
					.option("delimiter", ",")
					.save(destination)
					  sw.stop();
					  JOptionPane.showMessageDialog(null, "Table saved at " + destination+" in "+ sw.getMinutes+" minutes");
					  sqlContext.dropTempTable(table)
            spark.close()
					
					val folder: File = new File(destination)
			
          val listOfFiles: Array[File] = folder.listFiles()
          
          for (i <- 0 until listOfFiles.length) {
            
               if ( listOfFiles(i).getName.contains("part") &&  !listOfFiles(i).getName.contains("crc")){
                 
                 listOfFiles(i).renameTo(new File(destination+"//"+table+".csv"))
                 
               } 
               else
                  listOfFiles(i).delete()
             }	
					  sw.stop();
					  JOptionPane.showMessageDialog(null, "Table saved at " + destination+" in "+ sw.getMinutes+" minutes");
					  sqlContext.dropTempTable(table)
            spark.close()
					}
  }
}