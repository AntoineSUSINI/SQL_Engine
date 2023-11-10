import scala.collection.immutable.TreeMap

type Key = String
type Value = String
type TableName = String
type ColumnName = String
type RowValue = Any


// Définition de la structure des données pour représenter les schéma d'une table
// Schéma d'une table = crucial pour la définition de la structure des données stockées

// Classe schéma : clé primaire key et une liste de déclarations de colonnes columns
case class Schema(key: ColumnDeclaration, columns: List[ColumnDeclaration])
// Classe de déclaration de colonnes : nom de colonne et son type
case class ColumnDeclaration(name: ColumnName, colType: ColumnType)


// Représentation des types de données possibles pour les colonnes :
// utilisée pour spécifier le type de chaque colonne dans le schéma
enum ColumnType(val name: String):
  case IntType extends ColumnType("INTEGER")
  case StringType extends ColumnType("STRING")
  case BooleanType extends ColumnType("BOOLEAN")
  case TimestampType extends ColumnType("TIMESTAMP")


// Définition de l'objet ColumnType
object ColumnType:
  // byName crée une carte (Map) des noms de types de colonnes vers les types de colonnes
  //correspondants, ce qui permet de rechercher rapidement un type de colonne par son nom
  val byName: Map[String, ColumnType] = ColumnType.values.map(t => (t.name, t)).toMap
  // "from" prend un nom de type en entrée et renvoie le type de colonne correspondant
  // s'il existe.
  def from(name: String): Option[ColumnType] = byName.get(name)

// 1. Result
// Utilisée pour représenter le résultat d'une requête
case class Result(columns: List[ColumnName], rows: List[Row])

// 2. Row
// représente une ligne de données dans une table
case class Row(data: Map[ColumnName, RowValue]) {
  def get(columnName: ColumnName): Option[RowValue] = data.get(columnName)
}

// Définition des erreurs possibles
enum StoreError:
  case KeyNotFound(key: Key)
  case DuplicateKey(key: Key)
  case StorageIssue(message: String)

enum DatabaseError:
  case TableNotFound(tableName: TableName)
  case ColumnNotFound(columnName: ColumnName)
  case InvalidQuery(message: String)

case class Record(key: Key, value: Value)


// Définition de l'interface Store
// Définition de l'ensemble des méthodes que toute implémentation de stockage de données
// doit fournir
trait Store {
  def get(key: Key): Either[StoreError, Value]
  def put(key: Key, value: Value): Either[StoreError, Unit]
  def delete(key: Key): Either[StoreError, Unit]
  def scan(): Either[StoreError, Iterator[Record]]
  def getFrom(key: Key): Either[StoreError, Iterator[Record]]
  def getPrefix(prefix: String): Either[StoreError, Iterator[Record]]
}

// Implémentation de base du KV-store en mémoire (idem que celle donnée)
class MemoryStore extends Store {
  // note: TreeMap est un type de Map où les données sont triées en fonction de la clé
  private var data: TreeMap[Key, Value] = TreeMap.empty

  override def get(key: Key): Either[StoreError, Value] =
    data.get(key).toRight(StoreError.KeyNotFound(key))

  override def put(key: Key, value: Value): Either[StoreError, Unit] = {
    data = data.updated(key, value)
    Right(())
  }

  override def delete(key: Key): Either[StoreError, Unit] =
    if (data.contains(key)) {
      data = data - key
      Right(())
    } else {
      Left(StoreError.KeyNotFound(key))
    }

  override def scan(): Either[StoreError, Iterator[Record]] =
    Right(data.iterator.map { case (k, v) => Record(k, v) })

  override def getFrom(key: Key): Either[StoreError, Iterator[Record]] =
    Right(data.from(key).iterator.map { case (k, v) => Record(k, v) })

  override def getPrefix(prefix: String): Either[StoreError, Iterator[Record]] =
    Right(data.iterator
      .filter { case (k, _) => k.startsWith(prefix) }
      .map { case (k, v) => Record(k, v) })
}

// Définition du plan d'exécution : permet de représenter le plan d'exécution complet de
//la requête SQL de manière structurée.
case class ExecutionPlan(firstOperation: Operation)

// Interface d'opération : assure que toutes les opérations suivent une structure commune
sealed trait Operation {
  val next: Option[Operation]
}

// Définition de l'interface Database
trait Database {
  def openOrCreate(tableName: TableName, schema: Schema): Either[DatabaseError, Table]
  def drop(tableName: TableName): Either[DatabaseError, Unit]
}

// Définition de l'interface Table
trait Table {
  def insert(values: Map[ColumnName, RowValue]): Either[DatabaseError, Unit]
  def execute(executionPlan: ExecutionPlan): Either[DatabaseError, Result]
}

sealed trait ColumnExpression
object ColumnExpression {
  case class Column(name: ColumnName) extends ColumnExpression
  case object All extends ColumnExpression
  case class LitInt(value: Int) extends ColumnExpression
  case class LitString(value: String) extends ColumnExpression
}

sealed trait FilterExpression
object FilterExpression {
  case class Equal(col1: ColumnExpression, col2: ColumnExpression) extends FilterExpression
  case class GreaterOrEqual(col1: ColumnExpression, col2: ColumnExpression) extends FilterExpression
}

// 1. Fonction gérant TableScan
def handleTableScan(
                     tableName: String,
                     next: Option[Operation],
                     store: Store
                   ): Either[StoreError, Iterator[Record]] = {
  store.get(tableName).flatMap { _ =>
    store.scan()
  }
}

// 2. Fonction gérant requête Projection
def handleProjection(
                      tableName: String,
                      column: ColumnExpression,
                      otherColumns: List[ColumnExpression],
                      next: Option[Operation],
                      store: Store
                    ): Either[StoreError, Iterator[Record]] = {
  store.get(tableName).flatMap { _ =>
    val columnsToFetch = column match {
      case ColumnExpression.Column(name) => name :: otherColumns.collect { case ColumnExpression.Column(name) => name }
      case _ => otherColumns.collect { case ColumnExpression.Column(name) => name }
    }

    store.scan().map { recordsIterator =>
      recordsIterator.map { record =>
        val keyParts = record.key.split("#")
        if (columnsToFetch.contains(keyParts.last)) {
          Record(record.key, record.value)
        } else {
          Record(record.key, "")
        }
      }
    }
  }
}

// 3. Requête Filter
// 3.1 Fonctions avant de gérer la requête Filter
// 3.1.1 Fonction qui gère les expressions de filtre telles que '=' ou '<='
def evaluateFilter(filter: FilterExpression, record: Record): Boolean = {
  filter match {
    case FilterExpression.Equal(col1, col2) =>
      val value1 = evaluateColumnExpression(col1, record)
      val value2 = evaluateColumnExpression(col2, record)
      value1.toString == value2.toString

    case FilterExpression.GreaterOrEqual(col1, col2) =>
      val value1 = evaluateColumnExpression(col1, record).toString.toIntOption.getOrElse(0)
      val value2 = evaluateColumnExpression(col2, record).toString.toIntOption.getOrElse(0)
      value1 >= value2

    case _ => true
  }
}

//3.1.2 Fonction qui gère les expressions de colonnes pour savoir où chercher la data
def evaluateColumnExpression(colExpr: ColumnExpression, record: Record): RowValue = {
  colExpr match {
    case ColumnExpression.Column(name) =>
      record.key.split("#").find(_.startsWith(name)).getOrElse("")

    case ColumnExpression.LitInt(value) => value
    case ColumnExpression.LitString(value) => value
    case _ => ""
  }
}

// 3.2 Fonction de filtre => gestion de la requête Filter
def handleFilter(
                  filters: List[FilterExpression],
                  next: Option[Operation],
                  store: Store
                ): Either[StoreError, Iterator[Record]] = {
  store.scan().map { recordsIterator =>
    val filteredRecords = recordsIterator.filter { record =>
      filters.forall(evaluateFilter(_, record))
    }
    filteredRecords
  }
}

//4. Fonction gérant la requête Range
def handleRange(
                 tableName: String,
                 start: Int,
                 count: Int,
                 next: Option[Operation],
                 store: Store
               ): Either[StoreError, Iterator[Record]] = {
  store.scan().map { recordsIterator =>
    val rangedRecords = recordsIterator.slice(start, start + count)
    rangedRecords
  }
}


// Définition de l'opération de scan de table
case class TableScan(tableName: TableName, next: Option[Operation]) extends Operation

// Définition de l'opération de projection
case class Projection(
                       tableName: TableName,
                       column: ColumnExpression,
                       otherColumns: List[ColumnExpression],
                       next: Option[Operation]
                     ) extends Operation

// Définition de l'opération de filtrage
case class Filter(
                   tableName: TableName,
                   filterExprs: List[FilterExpression],
                   next: Option[Operation]
                 ) extends Operation

// Définition de l'opération de plage (range query)
case class Range(
                  tableName: TableName,
                  start: Int,
                  count: Int,
                  next: Option[Operation]
                ) extends Operation



// Classe QueryEngine => encapsulation du moteur de requête
// Paramètre : Store => interaction avec les données
class QueryEngine(store: Store) {
  // Méthode d'exécution d'une requête en fonction du plan d'exécution
  def executeQuery(executionPlan: ExecutionPlan): Either[StoreError, Iterator[Record]] = {
    executeOperation(executionPlan.firstOperation)
  }

  // Méthode d'exécution des opération
  // Prend une opération en argument et exécute l'opération en appelant
  // la fonction appropriée en fonction du type d'opération.
  private def executeOperation(operation: Operation): Either[StoreError, Iterator[Record]] = {
    // Définition de toutes les opérations possibles
    // On fait interragir les différentes fonctions ditrectement avec Store
    operation match {
      case TableScan(tableName, next) =>
        handleTableScan(tableName, next, store)

      case Projection(tableName, column, otherColumns, next) =>
        handleProjection(tableName, column, otherColumns, next, store)

      case Filter(tableName, filterExprs, next) =>
        handleFilter(filterExprs, next, store)

      case Range(tableName, start, count, next) =>
        handleRange(tableName, start, count, next, store)

      case null => Left(StoreError.KeyNotFound("Invalid operation"))
    }
  }
}



object Main {
  def main(args: Array[String]): Unit = {
    val memoryStore = new MemoryStore()
    memoryStore.put("PEOPLE", "")
    memoryStore.put("PEOPLE#000#id", "STRING")
    memoryStore.put("PEOPLE#001#name", "STRING")
    memoryStore.put("PEOPLE#002#age", "INT")
    memoryStore.put("123#id", "123")
    memoryStore.put("123#name", "Jon")
    memoryStore.put("123#age", "32")
    memoryStore.put("456#id", "456")
    memoryStore.put("456#name", "Mary")
    memoryStore.put("456#age", "25")

    val executionPlan = ExecutionPlan(
      Projection("PEOPLE", ColumnExpression.Column("name"), List.empty, None)
    )

    val queryEngine = new QueryEngine(memoryStore)
    val result = queryEngine.executeQuery(executionPlan)

    // Vérification des résultats
    result match {
      case Left(error) => println(s"Erreur lors de l'exécution de la requête : $error")
      case Right(records) =>
        // Parcourez les enregistrements pour obtenir le résultat
        val filteredRecords = records.filter(record => record.key == "123#name")
        filteredRecords.foreach { record =>
          println(s"Clé: ${record.key}, Valeur: ${record.value}")
        }
    }
  }
}


