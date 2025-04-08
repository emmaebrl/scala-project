# Scala Spark Template

Template modulaire en Scala pour lire, transformer et écrire des données avec Apache Spark.

## 🚀 Fonctionnalités

- Lecture automatique de fichiers CSV, Parquet ou tables Hive
- Traitement de données configurable par types de rapports (`report1`, etc.)
- Écriture des résultats configurée via un fichier `.properties`
- Compatible avec Spark local ou cluster

---

## ▶️ Exécution

### 📄 Télécharger le fichier JAR depuis GitHub

Télécharge le fichier `scala-project-1.3.2-jar-with-dependencies.jar` depuis :  
👉 [https://github.com/emmaebrl/scala-project/packages/2457639](https://github.com/emmaebrl/scala-project/packages/2457639)

---

### ▶️ Lancer le projet avec `java -cp`

Une fois le JAR téléchargé, exécute la commande suivante :

```bash
java -cp scala-project-1.3.2-jar-with-dependencies.jar fr.mosef.scala.template.Main \
  <master-url> \
  <input-path> \
  <output-path> \
  <report1,report2,...> \
  [optional-config-path]
```

## 🧾 Paramètres CLI

| Position | Nom           | Obligatoire | Description                                                                 |
|----------|----------------|-------------|-----------------------------------------------------------------------------|
| `0`      | `MASTER_URL`   | ❌           | URL du cluster Spark. Exemples : `local[2]`, `yarn`. Par défaut : `local[1]`. |
| `1`      | `SRC_PATH`     | ✅           | Chemin d'entrée des données : `.csv`, `.parquet`, ou `hive:nom_table`.       |
| `2`      | `DST_PATH`     | ❌           | Dossier de sortie pour les rapports. Par défaut : `./default/output-writer`. |
| `3`      | `REPORT_TYPES` | ❌           | Liste des types de rapports à générer (séparés par des virgules). Par défaut : `report1`. |
| `4`      | `CONFIG_PATH`  | ❌           | Chemin d'un fichier `.properties` de configuration. Par défaut : `application.properties` en ressources. |


✅ Exemples
**Exemple avec CSV et config externe :**
```bash
spark-submit \
--class fr.mosef.scala.template.Main \
--master local[2] \
./scala-template.jar \
local[2] \
./data/input.csv \
./data/output \
report1,report2 \
./config/application.properties
```

**Exemple avec table Hive :**
```bash
spark-submit \
--class fr.mosef.scala.template.Main \
--master yarn \
./scala-template.jar \
yarn \
hive:ma_table_hive \
/hdfs/output \
report1
```

**Exemple minimal :**
```bash
spark-submit \
--class fr.mosef.scala.template.Main \
./scala-template.jar \
local[2] \
./data/input.csv
```

**🛠 Structure du projet**
```text
src/
├── main/
│   ├── scala/
│   │   └── fr/mosef/scala/template/
│   │       ├── Main.scala
│   │       ├── processor/
│   │       │   ├── Processor.scala
│   │       │   └── impl/ProcessorImpl.scala
│   │       ├── reader/
│   │       │   ├── Reader.scala
│   │       │   └── impl/ReaderImpl.scala
│   │       └── writer/Writer.scala
│   └── resources/
│       └── application.properties
```

**📄 Exemple application.properties**
```text
properties
format=parquet
header=true
mode=overwrite
coalesce=true
```
