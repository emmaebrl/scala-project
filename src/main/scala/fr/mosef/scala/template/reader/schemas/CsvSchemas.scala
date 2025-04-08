package fr.mosef.scala.template.reader.schemas

import org.apache.spark.sql.types._

object CsvSchemas {
  val rappelSchema: StructType = StructType(Seq(
    StructField("reference_fiche", StringType, true),
    StructField("ndeg_de_version", StringType, true),
    StructField("nature_juridique_du_rappel", StringType, true),
    StructField("categorie_de_produit", StringType, true),
    StructField("sous_categorie_de_produit", StringType, true),
    StructField("nom_de_la_marque_du_produit", StringType, true),
    StructField("noms_des_modeles_ou_references", StringType, true),
    StructField("identification_des_produits", StringType, true),
    StructField("conditionnements", StringType, true),
    StructField("date_debut_fin_de_commercialisation", StringType, true),
    StructField("temperature_de_conservation", StringType, true),
    StructField("marque_de_salubrite", StringType, true),
    StructField("informations_complementaires", StringType, true),
    StructField("zone_geographique_de_vente", StringType, true),
    StructField("distributeurs", StringType, true),
    StructField("motif_du_rappel", StringType, true),
    StructField("risques_encourus_par_le_consommateur", StringType, true),
    StructField("preconisations_sanitaires", StringType, true),
    StructField("description_complementaire_du_risque", StringType, true),
    StructField("conduites_a_tenir_par_le_consommateur", StringType, true),
    StructField("numero_de_contact", StringType, true),
    StructField("modalites_de_compensation", StringType, true),
    StructField("date_de_fin_de_la_procedure_de_rappel", StringType, true),
    StructField("informations_complementaires_publiques", StringType, true),
    StructField("liens_vers_les_images", StringType, true),
    StructField("lien_vers_la_liste_des_produits", StringType, true),
    StructField("lien_vers_la_liste_des_distributeurs", StringType, true),
    StructField("lien_vers_affichette_pdf", StringType, true),
    StructField("lien_vers_la_fiche_rappel", StringType, true),
    StructField("rappelguid", StringType, true),
    StructField("group_key", StringType, true)
  ))
}