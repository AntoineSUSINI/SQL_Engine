import scala.collection.immutable.TreeMap

object SQLEngine {
  type Key = String
  type Value = String
  type TableName = String
  type ColumnName = String
  type RowValue = Any

  case class Schema(key: ColumnDeclaration, columns: List[ColumnDeclaration])
  case class ColumnDeclaration(name: ColumnName, colType: ColumnType)

  enum ColumnType(val name: String):
    case IntType extends ColumnType("INTEGER")
    case StringType extends ColumnType("STRING")
    case BooleanType extends ColumnType("BOOLEAN")
    case TimestampType extends ColumnType("TIMESTAMP")

  object ColumnType:
    val byName: Map[String, ColumnType] = ColumnType.values.map(t => (t.name, t)).toMap
    def from(name: String): Option[ColumnType] = byName.get(name)

  case class Result(columns: List[ColumnName], rows: List[Row])

  case class Row(data: Map[ColumnName, RowValue]) {
    def get(columnName: ColumnName): Option[RowValue] = data.get(columnName)
  }

  enum StoreError:
    case KeyNotFound(key: Key)
    case DuplicateKey(key: Key)
    case StorageIssue(message: String)

  enum DatabaseError:
    case TableNotFound(tableName: TableName)
    case ColumnNotFound(columnName: ColumnName)
    case InvalidQuery(message: String)

  case class Record(key: Key, value: Value)

  trait Store {
    def get(key: Key): Either[StoreError, Value]
    def put(key: Key, value: Value): Either[StoreError, Unit]
    def delete(key: Key): Either[StoreError, Unit]
    def scan(): Either[StoreError, Iterator[Record]]
    def getFrom(key: Key): Either[StoreError, Iterator[Record]]
    def getPrefix(prefix: String): Either[StoreError, Iterator[Record]]
  }

  class MemoryStore extends Store {
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

  case class ExecutionPlan(firstOperation: Operation)

  sealed trait Operation {
    val next: Option[Operation]
  }

  trait Database {
    def openOrCreate(tableName: TableName, schema: Schema): Either[DatabaseError, Table]
    def drop(tableName: TableName): Either[DatabaseError, Unit]
  }

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
    // Add other cases as needed
  }

  def handleTableScan(
    tableName: String,
    next: Option[Operation],
    store: Store
  ): Either[StoreError, Iterator[Record]] = {
    store.get(tableName).flatMap { _ =>
      store.scan()
    }
  }

  def handleProjection(
    tableName: String,
    column: ColumnExpression | ColumnExpression.All.type,
    otherColumns: List[ColumnExpression | ColumnExpression.All.type],
    next: Option[Operation],
    store: Store
  ): Either[StoreError, Iterator[Record]] = {
    store.get(tableName).flatMap { _ =>
      val columnsToFetch = (column match {
        case ColumnExpression.All => ColumnExpression.All :: otherColumns
        case ce: ColumnExpression => ce :: otherColumns
      }).collect {
        case ce: ColumnExpression.Column => ce.name
      }
      store.scan().map { recordsIterator =>
        val projectedRecords = recordsIterator.map { record =>
          val recordValues = record.value.split("#").toList
          val filteredValues = columnsToFetch.zip(recordValues).collect {
            case (columnName, value) if columnsToFetch.contains(columnName) => value
          }
          Record(record.key, filteredValues.mkString("#"))
        }
        projectedRecords
      }
    }
  }



  def evaluateFilter(filter: FilterExpression, record: Record): Boolean = {
    filter match {
      case FilterExpression.Equal(col1, col2) =>
        evaluateColumnExpression(col1, record) == evaluateColumnExpression(col2, record)

      case FilterExpression.GreaterOrEqual(col1, col2) =>
        val value1 = evaluateColumnExpression(col1, record).toString.toIntOption.getOrElse(0)
        val value2 = evaluateColumnExpression(col2, record).toString.toIntOption.getOrElse(0)
        value1 >= value2

      // Add other cases as needed

      case null => true // Default to accepting all records
    }
  }

  def evaluateColumnExpression(colExpr: ColumnExpression, record: Record): RowValue = {
    colExpr match {
      case ColumnExpression.Column(name) =>
        record.key.split("#").find(_.startsWith(name)).getOrElse("")

      case ColumnExpression.LitInt(value) => value
      case ColumnExpression.LitString(value) => value
      case _ => "" // Handle other cases
    }
  }

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


class QueryEngine(store: Store) {
  def executeQuery(executionPlan: ExecutionPlan): Either[StoreError, Iterator[Record]] = {
    executeOperation(executionPlan.firstOperation)
  }

  private def executeOperation(operation: Operation): Either[StoreError, Iterator[Record]] = {
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
      TableScan("PEOPLE",
        Filter(Equal(Column("id"), LitString("123")), None,
          Projection(Column("name"), Nil, None)
        )
      )
    )

    val queryEngine = new QueryEngine(memoryStore)
    val result = queryEngine.executeQuery(executionPlan)

    // Vérification des résultats
    result match {
      case Left(error) => println(s"Erreur lors de l'exécution de la requête : $error")
      case Right(records) =>
        // Parcourez les enregistrements pour obtenir le résultat
        records.foreach { record =>
          println(s"Clé: ${record.key}, Valeur: ${record.value}")
        }
    }
  }
}


