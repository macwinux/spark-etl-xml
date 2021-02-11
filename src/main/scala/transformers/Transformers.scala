package transformers

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}

object Transformers {

  def firstLevelValue(df: DataFrame, replace: DataFrame, column: String, colTransformer: String): DataFrame = {
    df.join(replace, Seq(column), "left").withColumn(column,when(col(colTransformer).isNotNull,col(colTransformer)).otherwise(col(column)))
  }
;
  def secondLevelValue(df: DataFrame, replace: DataFrame, firstLevelArrayColumn: String, secondLevelColumn: String, colTransformer: String): DataFrame = {
    df.withColumn(secondLevelColumn,explode(col(s"${firstLevelArrayColumn}.${secondLevelColumn}")))
      .join(replace, Seq(secondLevelColumn),"left").withColumn(secondLevelColumn,when(col(colTransformer).isNotNull,col(colTransformer)).otherwise(col(secondLevelColumn)))

  }

}
