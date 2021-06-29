import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, count, when,sum}
import org.apache.spark.sql.types._

object FinalFile {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("S3Client")
      .config("spark.master", "local")
      .getOrCreate()

    val sc = spark.sparkContext;

    //Conenction to AWS S3 bucket
    System.getProperty("com.amazonaws.services.s3.enableV4")
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.access.key", "AKIA2XG2JOPTIBH5JMPC")
    sc.hadoopConfiguration.set("fs.s3a.secret.key", "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")
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")


    val fileName = "s3a://bootcamp-project-storage/group2/batch/files-2019-Oct.csv."
    //Schema for the data
    val schema1 = new StructType()
      .add("event_time",StringType,true)
      .add("event_type",StringType,true)
      .add("product_id",LongType,true)
      .add("category_id",LongType,true)
      .add("category_code",StringType,true)
      .add("brand",StringType,true)
      .add("price",DoubleType,true)
      .add("user_id",LongType,true)
      .add("user_session",StringType,true)

    val df = spark.read.format("csv").option("header","true").schema(schema1).load(fileName)

    //Transformation for nulls and date
    val df_nonull=df.na.fill("N/A",Seq("category_code"))
    val df_trans=df_nonull.withColumn("brand",coalesce(df("brand"),df("Product_id")))
    val dfn= df_trans.withColumn("Date", df("Event_time").cast(DateType))

  //Product Analysis
    val df_transform_product_oct=dfn.groupBy(dfn("Date"),dfn("product_id"))
      .agg(count(when(dfn("Event_type")==="view",1)).as("view_count"),
      count(when(dfn("Event_type")==="cart",1)).as("cart_count"),
      count(when(dfn("Event_type")==="remove_from_cart",1)).as("remove_from_cart_count"),
      count(when(dfn("Event_type")==="purchase",1)).as("purchase_count"))

   //Brand Analysis
    val df_transform_brand_oct=dfn.groupBy(dfn("Date"),dfn("brand"))
      .agg(count(when(dfn("Event_type")==="view",1)).as("view_count"),
        count(when(dfn("Event_type")==="cart",1)).as("cart_count"),
        count(when(dfn("Event_type")==="remove_from_cart",1)).as("remove_from_cart_count"),
        count(when(dfn("Event_type")==="purchase",1)).as("purchase_count"))

    //User Analysis
    val df_transform_user_oct=dfn.groupBy(dfn("Date"),dfn("user_id"))
      .agg(count(when(dfn("Event_type")==="view",1)).as("view_count"),
        count(when(dfn("Event_type")==="cart",1)).as("cart_count"),
        count(when(dfn("Event_type")==="remove_from_cart",1)).as("remove_from_cart_count"),
        count(when(dfn("Event_type")==="purchase",1)).as("purchase_count"))

    //Sales Analysis
    val  df_transform_sales_oct = dfn.where(dfn("Event_type")==="purchase")
      .groupBy(dfn("Date"),dfn("product_id"),dfn("category_id"),dfn("brand"))
      .agg(sum(dfn("price")))


    val url="jdbc:mysql://localhost/ques2"
    val user="root"
    val password="ttn"
    val conProperties = new java.util.Properties()
    val driver = "com.mysql.cj.jdbc.Driver"
    conProperties.put("user",user)
    conProperties.put("password",password)
    conProperties.put("url",url)
    conProperties.put("driver",driver)
    Class.forName(driver)
    df_trans.write.jdbc(url=url,table="originalData_October",conProperties)
    df_transform_product_oct.write.jdbc(url=url, table="ProductAnalysis_October1",conProperties)
    df_transform_brand_oct.write.jdbc(url=url, table="BrandAnalysis_October1",conProperties)
    df_transform_user_oct.write.jdbc(url=url, table="UserAnalysis_October1",conProperties)
    df_transform_sales_oct.write.jdbc(url=url, table="SalesAnalysis_October1",conProperties)
  }
}