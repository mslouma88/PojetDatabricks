// Databricks notebook source
// MAGIC %md
// MAGIC ![SDA2024_image](files/tables/Universite.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Bienvenue dans la partie N°2 de notre projet SPARK/SCALA  [Partie Databricks]

// COMMAND ----------

print("Ce projet a été réalisé par M. MEJRI Salam  | Encadré Par M. DIATTARA Ibrahima")

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import spark.sqlContext.implicits
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._

// COMMAND ----------

// DBTITLE 1,Partie 1: Load Data || Question1 :

val path = "/FileStore/tables/TP_Databricks.json"


// COMMAND ----------

def readerjson(path: String): (DataFrame, DataFrame) ={
val df = spark.read.option("multiline", "true").json(path)

// DataFrame Transaction

val dfTransactions = df.select(explode($"Transaction")).select($"col.*")

// DataFrame Devis

val dfDevis = df.select(explode($"Devis")).select($"col.*")

(dfTransactions, dfDevis)
}

// COMMAND ----------

// DBTITLE 1,Affichage du 1er DataFrame "Transaction"
val (dfTransactions, dfDevis) = readerjson(path)
display(dfTransactions)

// COMMAND ----------

// DBTITLE 1,Affichage du 2eme DataFrame "Devis"
display(dfDevis)

// COMMAND ----------

// DBTITLE 1,Partie 2: Agrégation Simple || Question 1:
def NbCommandeParProduitDansPays(dfTransactions: DataFrame): DataFrame = {

  val Nb_command = dfTransactions.groupBy("TypeProduit")
   .pivot("Pays")
   .agg(count("*") as "NbCommandes")
   .na.fill(0)

  return Nb_command
}

val Nb_command: DataFrame = NbCommandeParProduitDansPays(dfTransactions)
 display(Nb_command)//.show()

// COMMAND ----------

// DBTITLE 1,Partie 3: Cross Data || Question 1:
def PrixEur(dfTransactions: DataFrame, dfDevis: DataFrame):DataFrame = {

  val dfJointure = dfTransactions.join(dfDevis, Seq("Devis"), "inner")

  val dfPrixEur = dfJointure.withColumn("PrixEur", round($"Prix"*$"Taux", 1))

  return dfPrixEur
}

val dfPrixEur: DataFrame = PrixEur(dfTransactions, dfDevis)

display(dfPrixEur)

// COMMAND ----------

// DBTITLE 1,Partie 4: Agrégation avec windows partition || Question 1:
def Top2TransactionPerPays(df: DataFrame): DataFrame = {

  val windowSpec  = Window.partitionBy("Pays").orderBy(desc("PrixEur"))

  val dfRank = dfPrixEur

    .withColumn("Rank", rank().over(windowSpec))
    .filter($"rank" <= 2)
    return dfRank
}
val dfRank = Top2TransactionPerPays(dfPrixEur)

display(dfRank)

// COMMAND ----------

// DBTITLE 1,Partie 5 : Agrégation Combinée
def Cube(df: DataFrame):DataFrame = {

  val dfCube = dfPrixEur
  .cube("Pays", "TypeProduit")
  .agg(sum("PrixEur") as "CA_euro")
  .na.fill(Map("Pays"->"ALL_Country", "TypeProduit"->"ALL_Product"))
 
  return dfCube
}
val dfCube = Cube(dfPrixEur)
display(dfCube)


// COMMAND ----------

// DBTITLE 1,Partie 6 - API SQL
dfPrixEur.createOrReplaceTempView("selltable")

// COMMAND ----------

// MAGIC %sql
// MAGIC select coalesce(TypeProduit,"ALL_Product") as TypeProduit,
// MAGIC        coalesce(Pays,"ALL_Country") as Pays, 
// MAGIC        SUM(PrixEur) as CA_euro 
// MAGIC        from selltable  GROUP BY CUBE (TypeProduit, Pays) ORDER BY TypeProduit

// COMMAND ----------

// DBTITLE 1,b. Conclusion:
// MAGIC %md
// MAGIC <span style="color: #FF5733;">Conclusion:</span>
// MAGIC On peut noter que la Tunisie est le pays qui gènère le plus gros chiffre d'affaires soit 46 655 € sur 47256,30 
