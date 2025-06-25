import org.apache.spark.sql.SparkSession

object ExerciceXII {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Exercice XII")
      .master("local[*]") // exécution locale sur tous les coeurs
      .getOrCreate()

    val sc = spark.sparkContext

    // Partie 1 : articles communs entre paniers
    val panier1 = sc.parallelize(Seq("Gâteau", "Lait", "Fromage", "Papier toilette"))
    val panier2 = sc.parallelize(Seq("Pain", "Eau", "Jus de fruit", "Lait", "Fromage"))

    // Intersection - articles communs
    val articlesCommuns = panier1.intersection(panier2).collect()

    println("Articles communs aux deux paniers :")
    articlesCommuns.foreach(println)

    // Partie 2 : fusion et formatage
    val liste1 = List("crayon", "stylo", "taille-crayon")
    val liste2 = List("livre de mathématiques", "livre français", "livre anglais")

    val listeFusionnee: List[List[String]] = List(liste1, liste2)

    // Fonction pour formater chaque article
    def formater(article: String): String = {
      if (article.startsWith("livre")) {
        article match {
          case "livre de mathématiques" => s"un $article est nécessaire dans la classe."
          case "livre français" => s"le $article est nécessaire dans la classe."
          case "livre anglais" => s"le livre d'anglais est nécessaire dans la classe."
          case _ => s"un $article est nécessaire dans la classe."
        }
      } else {
        s"un $article est nécessaire dans la classe."
      }
    }

    // Aplatir la liste et appliquer le formatage
    val resultats = listeFusionnee.flatten.map(formater)

    println("\nListe formatée :")
    resultats.foreach(println)

    spark.stop()
  }
}
