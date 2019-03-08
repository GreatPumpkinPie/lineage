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


        val sql = """USE app;set mapred.output.compress=true;
                                                         set hive.exec.compress.output=true;
                                                         set mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec;
                                                         set hive.exec.dynamic.partition=true;
                                                         set hive.exec.dynamic.partition.mode=nonstrict;
                                                         set hive.exec.max.dynamic.partitions=100000;
                                                         set hive.exec.max.dynamic.partitions.pernode=100000;
                                                         set hive.exec.reducers.bytes.per.reducer = 400000000;
                                                         set hive.exec.parallel=true;
                                                         set hive.auto.convert.join=true;
                                                         set hive.groupby.skewindata=true;
                                                         set mapred.reduce.tasks=100;

                                                 use tmp;
                                                 drop table if EXISTS tmp.tmp_m99_meta_table_distinct;
                                                 create table if not EXISTS tmp.tmp_m99_meta_table_distinct as
                                                 select relation_id.id as meta_table_relation_id
                                                     ,meta_tb.db_cluster_name
                                                     ,meta_tb.org_id
                                                     ,meta_tb.market_code
                                                     ,meta_tb.db_name
                                                     ,meta_tb.tb_name
                                                     ,meta_tb.tb_created
                                                 from
                                                 (
                                                     select shop_meta.org_id,shop_meta.db_cluster_name,shop_meta.tbl_owner market_code,shop_meta.ku_name db_name,shop_meta.tbl_name tb_name,shop_meta.tb_created
                                                     from
                                                     (
                                                         select case when tbl_owner in ('mart_coo','mart_lfc','mart_lim','mart_smt','mart_cwm') then 3
                                                                     when regexp_extract(tbl_name,'(.*?)m08(.*?)',1)<>'' then 3
                                                                     when regexp_extract(tbl_name,'(.*?)s08(.*?)',1)<>'' then 3
                                                                     when regexp_extract(tbl_name,'(.*?)m09(.*?)',1)<>'' then 3
                                                                     when regexp_extract(tbl_name,'(.*?)s09(.*?)',1)<>'' then 3
                                                                     else 1 end as org_id
                                                             ,db_cluster_name,tbl_owner,ku_name,tbl_name,max(tbl_create_time) tb_created
                                                         from gdm.gdm_m99_cluster_metastore_tbl_part_param_info
                                                         where dt='2019-02-27' and coalesce(db_cluster_name,'')<>'' and coalesce(tbl_owner,'')<>'' and coalesce(ku_name,'')<>'' and coalesce(tbl_name,'')<>''
                                                         group by db_cluster_name,tbl_owner,ku_name,tbl_name
                                                     ) shop_meta
                                                 ) meta_tb
                                                 left join ( select id,market_code,cluster_code,db_name,tb_name from bdm.bdm_bds_metrics_bds_meta_table_relation where dt='2019-02-27' and coalesce(cluster_code,'')<>'') relation_id
                                                 on meta_tb.market_code=relation_id.market_code and meta_tb.db_cluster_name=relation_id.cluster_code and meta_tb.db_name=relation_id.db_name and meta_tb.tb_name=relation_id.tb_name
                                                 where coalesce(meta_tb.db_name,'data_export')<>'data_export'
                                                 ;
                                                 use app;
                                                 alter table app.app_m99_table_last_scan_da drop PARTITION(dt='2019-02-28');
                                                 INSERT OVERWRITE TABLE app.app_m99_table_last_scan_da PARTITION(dt='2019-02-28')
                                                 select
                                                     meta_table.meta_table_relation_id
                                                     ,case when dept.dept_id_2 ='38840' then 3 else meta_table.org_id end as org_id
                                                     ,meta_table.market_code
                                                     ,meta_table.db_cluster_name
                                                     ,meta_table.db_name
                                                     ,meta_table.tb_name
                                                     ,coalesce(scan_limit.last_scan_time,meta_table.tb_created,'1900-01-01') as last_scan_time
                                                     ,coalesce(storage.num_tb,0) num_tb
                                                     ,coalesce(mem.memmory_sum,0) executor_memory
                                                     ,coalesce(mem.cpu_time,0) cpu_time
                                                     ,coalesce(storage.user_erp,'') as user_erp
                                                     ,coalesce(modifi_time.modification_time,'1900-01-01') modification_time
                                                     ,dept.dept_name_1
                                                     ,dept.dept_name_2
                                                     ,dept.dept_name_3
                                                     ,dept.dept_name_4
                                                     ,dept.dept_name_5
                                                     ,dept.dept_name_6
                                                     ,case when coalesce(modification_time,'1900-01-01')<'2018-12-01' and modifi_time.modification_time = modifi_time.access_time then 1 when coalesce(modifi_time.access_time,'1970-01-01 08:00')='1970-01-01 08:00' then 1 else 0 end as is_zombie
                                                     ,coalesce(con_score.origin_score,0.0) origin_score
                                                 from tmp.tmp_m99_meta_table_distinct meta_table
                                                 left join
                                                 (
                                                     select table_name,max(dt) last_scan_time
                                                     from
                                                     (
                                                         --有效的被扫描表
                                                         select a.table_name,a.dt
                                                         from
                                                         (select coalesce(bee_businessid,'') bee_businessid,table_name,dt from adm.adm_m99_task_table_relation_all_da where table_type='src') a
                                                         join
                                                         (select coalesce(bee_businessid,'') bee_businessid,table_name,dt from adm.adm_m99_task_table_relation_all_da where table_type='dst' and substr(table_name,1,3)<>'bkt') b
                                                         on a.dt=b.dt and a.bee_businessid=b.bee_businessid
                                                         where a.table_name != b.table_name
                                                     ) scan_tb
                                                     group by table_name
                                                 ) scan_limit
                                                 on meta_table.tb_name=scan_limit.table_name
                                                 left join
                                                 (
                                                     select db_mart,db_cluster_name,db_name,tbl_name,num_tb
                                                         ,case when size(user_erp)=1 then user_erp[0]
                                                             when user_erp[0]='' then user_erp[1]
                                                             else user_erp[0]
                                                             end as user_erp
                                                     from
                                                     (
                                                         select db_mart,db_cluster_name,db_name,tbl_name,sum(num_tb) num_tb,collect_set(split(user_erp,',')[0]) user_erp
                                                         from (
                                                             select db_mart,db_cluster_name,substr(ku_name,1,length(ku_name)-3) as db_name,tbl_name,sum(coalesce(num_tb,0)) num_tb,coalesce(user_erp,'') user_erp
                                                             from app.app_cluster_table_store_d
                                                             where dt='2019-02-27' and coalesce(db_cluster_name,'')<>''
                                                             group by db_mart,db_cluster_name,ku_name,tbl_name,user_erp
                                                         ) a
                                                         group by db_mart,db_cluster_name,db_name,tbl_name
                                                     )b
                                                 ) storage
                                                 on meta_table.market_code=storage.db_mart and meta_table.db_cluster_name=storage.db_cluster_name and meta_table.db_name=storage.db_name and meta_table.tb_name=storage.tbl_name
                                                 left join
                                                 (
                                                     select table_exec.table_name,sum(table_exec.memmory_sum) memmory_sum,sum(table_exec.cpu_time) cpu_time
                                                     from
                                                     (
                                                         select cluster_id,mart_user,table_name,memmory_sum,cpu_time
                                                         from
                                                         (
                                                             select task_table.task_id
                                                                 ,task_table.cluster_id
                                                                 ,task_table.mart_user
                                                                 ,task_table.table_name
                                                                 ,coalesce(exe_tb.mem_time,0) as memmory_sum
                                                                 ,coalesce(exe_tb.cpu_time,0) as cpu_time
                                                             from (select * from adm.adm_m99_task_table_relation_all_da where dt='2019-02-27' and table_type='dst') task_table
                                                             left join
                                                             (
                                                                 select task_tb.task_id
                                                                     ,sum(instance_tb.map_cpu_time_spent + instance_tb.reduce_cpu_time_spent)/60 cpu_time
                                                                     ,sum(instance_tb.mem_time) mem_time
                                                                 from
                                                                 (select task_id,instance_id from gdm.gdm_m99_buffalo_task_log_da where dt= '2019-02-27') task_tb
                                                                 join
                                                                 (
                                                                     select bee_businessid,map_cpu_time_spent,reduce_cpu_time_spent
                                                                         ,maptotal * map_avg_time * sigmap_req_mem / 1024 + reducetotal * reduce_avg_time * sigreduce_req_mem / 1024 as mem_time
                                                                     from fdm.fdm_m99_job_hs_detail_di where dt='2019-02-27'
                                                                 ) instance_tb
                                                                 on task_tb.instance_id = instance_tb.bee_businessid
                                                                 group by task_tb.task_id
                                                             ) exe_tb
                                                             on task_table.task_id=exe_tb.task_id
                                                         )  resource_tb
                                                     ) table_exec
                                                     group by table_exec.table_name
                                                 ) mem
                                                 on meta_table.tb_name=mem.table_name
                                                 left join
                                                 (
                                                     SELECT
                                                       tb_name,
                                                       max(modification_time) as modification_time,
                                                       max(access_time) as access_time
                                                     from
                                                     (
                                                         select tb_name,modification_time,access_time
                                                         from gdm.gdm_m99_cluster_hdfs_fsimage_parse_di lateral view explode(split(hive_table,',')) t as tb_name
                                                         where dt='2019-02-27' and coalesce(hive_table,'') <> ''
                                                     ) fsimage
                                                     group by
                                                       tb_name
                                                 ) modifi_time
                                                 on  meta_table.tb_name=modifi_time.tb_name
                                                 left join
                                                 (select * from gdm.gdm_m02_emply_org_da where dt='2019-02-27' and coalesce(erp_acct_no,'')<>'') erp_dept
                                                 on storage.user_erp=erp_dept.erp_acct_no
                                                 left join (select * from dim.dim_org where coalesce(dept_id,'')<>'') dept
                                                 on erp_dept.dept_num=dept.dept_id
                                                 left join (select table_name,origin_score from app.app_table_connect_score_origin where dt='2019-02-27' ) con_score
                                                 on meta_table.tb_name=con_score.table_name"""

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

