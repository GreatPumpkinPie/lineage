import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser

/**
  * @Author: yangrui
  * @Date: 2019/1/16 17:27
  * @Version 1.0
  */
object Parser extends App {
    val plan = CatalystSqlParser.parsePlan("select * from table")

    plan match {
        case plan:UnresolvedRelation => {
            val project = plan.asInstanceOf[UnresolvedRelation]
            println(project.tableName)

        }
    }
}
