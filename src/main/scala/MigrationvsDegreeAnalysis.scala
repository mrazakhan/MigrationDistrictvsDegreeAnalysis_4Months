import org.apache.spark.SparkContext._
import java.security.MessageDigest
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.security.MessageDigest
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.math
import java.io._

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator


class DSV (var line:String="", var delimiter:String=",",var parts:Array[String]=Array("")) extends Serializable {

         parts=line.split(delimiter,-1)

def hasValidVal(index: Int):Boolean={
    return (parts(index)!=null)&&(parts.length>index)
}
def contains(text:String):Boolean={

    for(i <- 1 to (parts.length-1))
        if(parts(i).contains(text))
            return false

    true
}

}
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
//    kryo.register(classOf[Location])
//    kryo.register(classOf[ModalCOG])
    kryo.register(classOf[DSV])
  }
}


object MigrationDegreeAnalyzer_4months extends Serializable{
                val conf = new SparkConf().setMaster("yarn-client")
                //setMaster("spark://messi.ischool.uw.edu:7077")
                .setAppName("MigrationDegreeAnalysis_4Months")
                .set("spark.shuffle.consolidateFiles", "true")
                .set("spark.storage.blockManagerHeartBeatMs", "300000")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "MyRegistrator")
                .set("spark.akka.frameSize","1024")
                .set("spark.default.parallelism","200")
                //.set("spark.executor.memory", "40g")
                .set("spark.kryoserializer.buffer.max.mb","10024")
                .set("spark.kryoserializer.buffer.mb","1024")

                val sc = new SparkContext(conf)

        //val inputPath="hdfs:///user/mraza/Rwanda_In/CallsVolDegree/"
        //val outputPath = "hdfs:///user/mraza/Rwanda_Out/DistrictDegreeMigration_4months/"

		val inputPath="Rwanda_In/CallsVolDegree/"
        val outputPath = "Rwanda_Out/DistrictDegreeMigration_4months/"

def main(args:Array[String]){

        var month1=args(0)

        var callDegreeFile1=args(1)
        var callDegreeFile2=args(2)
        var callDegreeFileL1=args(3)
        var callDegreeFileL2=args(4)
        //SubscriberId,Month,A-District,A-Province,B-District,B-Province,A-Volume,A-Degree,A-TotalVolume,A-TotalDegree
//L72656815,0501,Bugesera,East,Bugesera,East,8,4,8,4


var cd_n1= sc.textFile(inputPath+callDegreeFile1,10).filter(d=>d.contains("Degree")==false).map(line=>line.replaceAll("\\)","").replaceAll("\\(","")).map(line=>(new DSV(line,"\\,"))).map(d=>(d.parts(0),(d.parts(1),d.parts(2),d.parts(3),d.parts(4),d.parts(5),d.parts(6),d.parts(7),d.parts(8),d.parts(9))))

cd_n1.count()

		
var cd_n2= sc.textFile(inputPath+callDegreeFile2,10).filter(d=>d.contains("Degree")==false).map(line=>line.replaceAll("\\)","").replaceAll("\\(","")).map(line=>(new DSV(line,"\\,"))).map(d=>(d.parts(0),(d.parts(1),d.parts(2),d.parts(3),d.parts(4),d.parts(5),d.parts(6),d.parts(7),d.parts(8),d.parts(9))))

        cd_n2.count()


var cd_l1= sc.textFile(inputPath+callDegreeFileL1,10).filter(d=>d.contains("Degree")==false).map(line=>line.replaceAll("\\)","").replaceAll("\\(","")).map(line=>(new DSV(line,"\\,"))).map(d=>(d.parts(0),(d.parts(1),d.parts(2),d.parts(3),d.parts(4),d.parts(5),d.parts(6),d.parts(7),d.parts(8),d.parts(9))))

cd_l1.count()

var cd_l2= sc.textFile(inputPath+callDegreeFileL2,10).filter(d=>d.contains("Degree")==false).map(line=>line.replaceAll("\\)","").replaceAll("\\(","")).map(line=>(new DSV(line,"\\,"))).map(d=>(d.parts(0),(d.parts(1),d.parts(2),d.parts(3),d.parts(4),d.parts(5),d.parts(6),d.parts(7),d.parts(8),d.parts(9))))

cd_l2.count()

		
//var KigaliDegreTest=cd_l1.map{case(k,v)=>(k,(v,if(v._5=="Kigali") v._7 else 0))}

var KigaliDegree=cd_l1.map{case(k,v)=>(k,(if(v._5=="Kigali") v._7.toInt else 0))}.reduceByKey(_+_)
KigaliDegree.count()

var KigaliVolume=cd_l1.map{case(k,v)=>(k,(if(v._5=="Kigali") v._6.toInt else 0))}.reduceByKey(_+_)
KigaliVolume.count()

var TotalDegree=cd_l1.map{case(k,v)=>(k,v._9.toInt)}
//TotalDegree.take(10).foreach(println)

var TotalVolume=cd_l1.map{case(k,v)=>(k,v._8.toInt)}
//TotalVolume.take(10).foreach(println)

var NonKigaliDegree=cd_l1.map{case(k,v)=>(k,(if(v._5!="Kigali") v._7.toInt else 0))}.reduceByKey(_+_)
NonKigaliDegree.count()

//NonKigaliDegree.take(10).foreach(println)

var NonKigaliVolume=cd_l1.map{case(k,v)=>(k,(if(v._5!="Kigali") v._6.toInt else 0))}.reduceByKey(_+_)
NonKigaliVolume.count()
//NonKigaliVolume.take(10).foreach(println)

var HomeDistDegree = cd_l1.map{case(k,v)=>(k,(if(v._2==v._4) v._7.toInt else 0))}.reduceByKey(_+_)
HomeDistDegree.count()

//HomeDistDegree.take(10).foreach(println)

var HomeDistVolume = cd_l1.map{case(k,v)=>(k,(if(v._2==v._4) v._6.toInt else 0))}.reduceByKey(_+_)
HomeDistVolume.count()

//HomeDistVolume.take(10).foreach(println)

var HomeProvDegree=cd_l1.map{case(k,v)=>(k,(if(v._3==v._5) v._7.toInt else 0))}.reduceByKey(_+_)
HomeProvDegree.count()
//HomeProvDegree.take(10).foreach(println)

var HomeProvVolume=cd_l1.map{case(k,v)=>(k,(if(v._3==v._5) v._6.toInt else 0))}.reduceByKey(_+_)
HomeProvVolume.count()
//HomeProvVolume.take(10).foreach(println)

		//Degrees table before map
        //(L90126642,((((0,1),1),1),1))
var DegreesTable=KigaliDegree.join(NonKigaliDegree).join(HomeDistDegree).join(HomeProvDegree).join(TotalDegree).map{case(k,v)=>(k,(v._1._1._1._1,v._1._1._1._2,v._1._1._2,v._1._2,v._2))}.distinct()
//DegreesTable.take(10).foreach(println)


var VolumesTable=KigaliVolume.join(NonKigaliVolume).join(HomeDistVolume).join(HomeProvVolume).join(TotalVolume).map{case(k,v)=>(k,(v._1._1._1._1,v._1._1._1._2,v._1._1._2,v._1._2,v._2))}.distinct()
//VolumesTable.take(10).foreach(println)



var migration_c=cd_n1.join(cd_n2,20).map{case(k,v)=>(k,(v._1._2,v._1._3,v._2._2,v._2._3))}.distinct()
//migration_c.take(10).foreach(println)

//migration stats for last two months
var migration_l=cd_l1.join(cd_l2,20).map{case(k,v)=>(k,(v._1._2,v._1._3,v._2._2,v._2._3))}.distinct()
migration_l.take(10).foreach(println)

var migration = migration_c.join(migration_l,20).map{case(k,v)=>(k,(v._1._1,v._1._2,v._1._3,v._1._4,v._2._1,v._2._2,v._2._3,v._2._4))}.distinct()

//migration.take(10).foreach(println)

var finalTable=migration.join(DegreesTable,20).distinct()
//finalTable.take(10).foreach(println)


var finalTable_m=finalTable.map{case(k,v)=>(k,(v._1._1,v._1._2,v._1._3,v._1._4,v._1._5,v._1._6,v._1._7,v._1._8,v._2._1,v._2._2,v._2._3,v._2._4,v._2._5))}
//finalTable_m.take(10).foreach(println)


//v1=Move to Kigali,  v2=moveFromKigali,v3=Remain Out of Kigali, v4=Remain In Kigali, v5=Kigali Degree, v6=NonKigali Degree v7=HomeDist Degree v8=Home Prov Degree v9=Total Degree
//v1=Move to Kigali => if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali")  Current v._2 and next prov v._4 are Kigali not the last one v._6

var finalTable2=finalTable_m.map{case(k,v)=>(k,(
if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8) 1 else 0,//MoveToKigali
if((v._2 != v._6 && v._2==v._4) && v._6==v._8 && v._6=="Kigali") 1 else 0,//MoveFromKigali
if((v._2==v._4&&v._4==v._6&&v._6==v._8)&&(v._2 != "Kigali")) 1 else 0,//RemainOutOfKigali
if(v._2 == "Kigali" && v._4=="Kigali" && v._6 =="Kigali" && v._8 =="Kigali") 1 else 0 ,//RemainInKigali
if ((v._2!=v._4 && v._6!=v._8 && v._4!=v._6) && (v._2!="Kigali" &&v._4!="Kigali" &&v._6!="Kigali" && v._8!="Kigali")) 1 else 0,//ExtremeMoveOutOfKigali
if ((v._2!=v._4 && v._6!=v._8 && v._4==v._6) && (v._2!="Kigali" &&v._4!="Kigali" &&v._6!="Kigali" && v._8!="Kigali")) 1 else 0,//SoftMoveOutOfKigali
v._9,v._10,v._11,v._12,v._13))}.distinct()


//finalTable2.take(10).foreach(println)
//ExtremeMoveOutsideKigali=Every month different and none equal to Kigali
//SoftMoveOutsideKigali =Previous 2 month different, Next 2 month different, last 1 month and next 1 month are  the same and every thing outside Kigali
		
var DistDegrees=finalTable2.map{case(k,v)=>(k,(month1,v._1,v._2,v._3,v._4,v._5,v._6,v._7,v._8,v._9))}.distinct().coalesce(1).mapPartitions(it=>(Seq("(Subscriber,(Month,MoveToKigali,MoveFromKigali,RemainOutOfKigali,RemainInKigali,ExtremeMoveOutsideKigali,SoftMoveOutsideKigali,KigaliDegree,NonKigaliDegree,HomeDistDegree,HomeProvDegree,TotalDegree))")++it).iterator)

//DistDegrees.take(10).foreach(println)

DistDegrees.saveAsTextFile(outputPath+"MigrationDistrictDegree_Modal"+month1)
//Districts:Bugesera,Burera,Gakenke,Kigali,Gatsibo,Gicumbi,Gisagara,Huye,Kamonyi,Karongi,Kayonza,Kigali,Kirehe,Muhanga,Musanze,Ngoma,Ngororero,Nyabihu,Nyagatare,Nyamagabe,Nyamasheke,Nyanza,Kigali,Nyaruguru,Rubavu,Ruhango,Rulindo,Rusizi,Rutsiro,Rwamagana

var moveToKigaliDetails_a =finalTable_m.map{case(k,v)=>(k,(if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Bugesera") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Burera") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Gakenke") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Gatsibo") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Gicumbi") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Gisagara") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Huye") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Kamonyi") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Karongi") 1 else 0))}

var moveToKigaliDetails_b =finalTable_m.map{case(k,v)=>(k,(if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Kirehe") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Muhanga") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Musanze") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Ngoma") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Ngororero") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Nyabihu") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Nyagatare") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Nyamagabe") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Nyamasheke") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Nyanza") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Nyaruguru") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Rubavu") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Ruhango") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Rulindo") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Rusizi") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Rutsiro") 1 else 0,if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali" && v._6==v._8&& v._6=="Rwamagana") 1 else 0))}
var moveToKigaliDetails_a2= moveToKigaliDetails_a.map{case(k,v)=>(k,(month1,"Bugesera","Burera","Gakenke","Gatsibo","Gicumbi","Gisagara","Huye","Kamonyi"))}
var moveToKigaliDetails_b2= moveToKigaliDetails_b.map{case(k,v)=>(k,(month1,
"Karongi","Kirehe","Muhanga","Musanze","Ngoma","Ngororero","Nyabihu","Nyagatare","Nyamagabe","Nyamasheke","Nyanza","Nyaruguru","Rubavu","Ruhango","Rulindo","Rusizi","Rutsiro","Rwamagana"))}


moveToKigaliDetails_a2.saveAsTextFile(outputPath+"MoveToKigaliDetails_a"+month1)
moveToKigaliDetails_b2.saveAsTextFile(outputPath+"MoveToKigaliDetails_b"+month1)

var moveFromKigaliDetails_a =finalTable_m.map{case(k,v)=>(k,(
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Bugesera") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Burera") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Gakenke") 1 else 0,//MoveFromKigal
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Gatsibo") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Gicumbi") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Gisagara") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Huye") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Kamonyi") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Karongi") 1 else 0//MoveFromKigali
))}

var moveFromKigaliDetails_b =finalTable_m.map{case(k,v)=>(k,(
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Kirehe") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Muhanga") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Musanze") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Ngoma") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Ngororero") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Nyabihu") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Nyagatare") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Nyamagabe") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Nyamasheke") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Nyanza") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Nyaruguru") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Rubavu") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Ruhango") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Rulindo") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Rusizi") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Rutsiro") 1 else 0,//MoveFromKigali
if(v._6 == v._8 && v._8=="Kigali" && v._2==v._4&& v._2=="Rwamagana") 1 else 0//MoveFromKigali
))}




var moveFromKigaliDetails_a2= moveFromKigaliDetails_a.map{case(k,v)=>(k,(month1,"Bugesera","Burera","Gakenke","Gatsibo","Gicumbi","Gisagara","Huye","Kamonyi","Karongi"))}

var moveFromKigaliDetails_b2= moveFromKigaliDetails_b.map{case(k,v)=>(k,(month1,"Kirehe","Muhanga","Musanze","Ngoma","Ngororero","Nyabihu","Nyagatare","Nyamagabe","Nyamasheke","Nyanza","Nyaruguru","Rubavu","Ruhango","Rulindo","Rusizi","Rutsiro","Rwamagana"))}

moveFromKigaliDetails_a2.saveAsTextFile(outputPath+"MoveFromKigaliDetails"+month1)
moveFromKigaliDetails_b2.saveAsTextFile(outputPath+"MoveFromKigaliDetails"+month1)


var DistVolumes=migration.join(VolumesTable).distinct()
//DistVolumes.take(10).foreach(println)

var DistVolumes_m=DistVolumes.map{case(k,v)=>(k,(v._1._1,v._1._2,v._1._3,v._1._4,v._1._5,v._1._6,v._1._7,v._1._8,v._2._1,v._2._2,v._2._3,v._2._4,v._2._5))}
//DistVolumes_m.take(10).foreach(println)


//v1=Move to Kigali,  v2=moveFromKigali,v3=Remain Out of Kigali, v4=Remain In Kigali, v5=Kigali Volume, v6=NonKigali Volume v7=HomeDist Volume v8=Home Prov Volume v9=Total Volume
var DistVolumes2=DistVolumes_m.map{case(k,v)=>(k,(if(v._2 == v._4 && v._4=="Kigali" && v._6!="Kigali") 1 else 0,if(v._2 != v._6 && v._6==v._8 && v._2=="Kigali") 1 else 0,if(v._2 != "Kigali" && v._4!="Kigali" && v._6 !="Kigali" && v._8 !="Kigali") 1 else 0,if(v._2 == "Kigali" && v._4=="Kigali" && v._6 =="Kigali" && v._8 =="Kigali") 1 else 0 , v._9,v._10,v._11,v._12,v._13))}.distinct()

//DistVolumes2.take(10).foreach(println)

DistVolumes2.saveAsTextFile(outputPath+"MigrationDistrictVolume_Modal"+month1)
}
}
