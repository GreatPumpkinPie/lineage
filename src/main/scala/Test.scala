import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{Analyzer, SimpleAnalyzer, UnresolvedRelation}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical._
import org.mortbay.util.SingletonList
import java._

import org.apache.spark.sql.execution.datasources.CreateTable
/**
  * @Author: yangrui
  * @Date: 2019/1/16 15:49
  * @Version 1.0
  */
object Test{
    case class Person(name: String, age: Int)
    case class DcTable(database: String, alias: String)


    def main(args: Array[String]): Unit = {
        System.setProperty("hadoop.home.dir","D:/hadoop-common-2.2.0-bin-master" )
        val sparksession = SparkSession.builder().appName("123").master("local").getOrCreate()
        val people1 = Person("abc",1)
        val people2 = Person("cdsc",2)

        val rdd = sparksession.sparkContext.makeRDD(Array(people1,people2))
        import sparksession.implicits._
        rdd.toDF().createOrReplaceTempView("table")
        //    sparksession.sql("select * from table").show()


        val sql = """insert into table sss.aaa select a,b from (select a from dd.rr union select p from pp.aa left join ddd.aaa on pp.aa.p = ddd.aaa.p ) pas"""

        val input = new java.util.Stack[DcTable]()
        val output = new java.util.Stack[DcTable]()

        val use = "(^| )USE ".r
        val set = "(^| )SET ".r
        val drop = "(^| )DROP TABLE ".r
        val create = "(^| )CREATE TABLE ".r
        val alter = "(^| )ALTER TABLE ".r

//        println((use findAllIn " cause i Usen Use ").size)


        sql.toUpperCase().split(";").filter(str => (use findAllIn str).isEmpty && (set findAllIn str).isEmpty && (drop findAllIn str).isEmpty && (create findAllIn str).isEmpty && (alter findAllIn str).isEmpty).map{
            line => {
                CatalystSqlParser.parsePlan(line)
            }
        }.foreach(plan=>{
            resolve(plan,"",input,output)
        })


        println(input)
        println(output)

    }


    def resolve(plan: LogicalPlan, currentDB:String, inputTables: java.util.Stack[DcTable], outputTables: java.util.Stack[DcTable]): Unit ={
        plan match {

            case plan: Project =>
                val project = plan.asInstanceOf[Project]
//                project.validConstraints.foreach(mapping=>{
//                    println(mapping.canonicalized.simpleString)
//                })
//                println(project.constraints)
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: Union =>
                val project = plan.asInstanceOf[Union]
                for(child <- project.children){
                    resolve(child, currentDB, inputTables, outputTables)
                }

            case plan: Join =>
                val project = plan.asInstanceOf[Join]
                resolve(project.left, currentDB, inputTables, outputTables)
                resolve(project.right, currentDB, inputTables, outputTables)

            case plan: Aggregate =>
                val project = plan.asInstanceOf[Aggregate]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: Filter =>
                val project = plan.asInstanceOf[Filter]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: Generate =>
                val project = plan.asInstanceOf[Generate]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: RepartitionByExpression =>
                val project = plan.asInstanceOf[RepartitionByExpression]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: SerializeFromObject =>
                val project = plan.asInstanceOf[SerializeFromObject]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: MapPartitions =>
                val project = plan.asInstanceOf[MapPartitions]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: DeserializeToObject =>
                val project = plan.asInstanceOf[DeserializeToObject]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: Repartition =>
                val project = plan.asInstanceOf[Repartition]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: Deduplicate =>
                val project = plan.asInstanceOf[Deduplicate]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: Window =>
                val project = plan.asInstanceOf[Window]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: MapElements =>
                val project = plan.asInstanceOf[MapElements]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: TypedFilter =>
                val project = plan.asInstanceOf[TypedFilter]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: Distinct =>
                val project = plan.asInstanceOf[Distinct]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: SubqueryAlias =>
                val project = plan.asInstanceOf[SubqueryAlias]
                val childInputTables = new java.util.Stack[DcTable]()
                val childOutputTables = new java.util.Stack[DcTable]()

                resolve(project.child, currentDB, childInputTables, childOutputTables)
                if(childInputTables.size()>0){
                    childInputTables.forEach(tb=>inputTables.push(tb))
                }else{
                    inputTables.push(DcTable(currentDB, project.alias))
                }

            case plan: UnresolvedRelation =>
                val project = plan.asInstanceOf[UnresolvedRelation]
                val dcTable = DcTable(project.tableIdentifier.database.getOrElse(currentDB), project.tableIdentifier.table)
                inputTables.push(dcTable)

            case plan: InsertIntoTable =>
                val project = plan.asInstanceOf[InsertIntoTable]
                val insert = project.table.asInstanceOf[UnresolvedRelation].tableIdentifier
                outputTables.push(DcTable(insert.database.getOrElse(""),insert.table))
                resolve(project.query, currentDB, inputTables, outputTables)


            case plan: CreateTable =>
                val project = plan.asInstanceOf[CreateTable]
                if(project.query.isDefined){
                    resolve(project.query.get, currentDB, inputTables, outputTables)
                }
                val tableIdentifier = project.tableDesc.identifier
                val dcTable = DcTable(tableIdentifier.database.getOrElse(currentDB), tableIdentifier.table)
                outputTables.push(dcTable)

            case plan: GlobalLimit =>
                val project = plan.asInstanceOf[GlobalLimit]
                resolve(project.child, currentDB, inputTables, outputTables)

            case plan: LocalLimit =>
                val project = plan.asInstanceOf[LocalLimit]
                resolve(project.child, currentDB, inputTables, outputTables)

            case _ => {
                println("解析错误")
            }
        }
    }
}

