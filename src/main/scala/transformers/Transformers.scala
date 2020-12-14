package transformers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

class Transformers {

  def firstLevelValue(df: DataFrame, replace: DataFrame, column: String, colTransformer: String) = {
    df.join(replace, Seq(column), "left").withColumn(colTransformer, when(col(colTransformer).isNull,column).otherwise(colTransformer))
  }

}
