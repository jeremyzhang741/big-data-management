// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
val inputFilePath  = "FULL_PATH_OF_YOUR_INPUT_FILE"
val outputDirPath = "FULL_PATH_OF_YOUR_OUTPUT_DIRECTORY"


// Write your solution here
//read input file
val textFile = sc.textFile(inputFilePath)
val items = textFile.map(line =>line.split(",")).collect()

var urls_list:List[(String,Long)]=List()
//clean the useful data
for (it <- items) {
  if(it(0)!="") {
    
    val size_num = it(3).replaceAll("[A-Za-z]","").toLong
    val size_type = it(3).replaceAll("[0-9]", "")

    if(size_type=="KB"){
      
      urls_list = urls_list :+ ((it(0),size_num*1024))
    }
    else if(size_type=="MB"){
      
      urls_list = urls_list :+ ((it(0),size_num*1024*1024))
    }
    else if(size_type=="B"){

      
      urls_list = urls_list :+ ((it(0),size_num))
    }
    
  }

}
//set a RDD list
val Urls = sc.parallelize(urls_list)

//map the data and get result
val result_List = Urls.groupByKey().map(x=>{val res_min=(x._2).min+"B";val res_max=(x._2).max+"B";
  var sum:Long=0;var num1=0;var sum1:Long=0;for(i<-x._2){sum=sum+i;num1=num1+1};val avg=sum/num1;val res_mean=avg.floor.toLong;
  for(e<-x._2){val diff=e-avg;val diff_squa=diff*diff;sum1=sum1+diff_squa};val res1=sum1/num1;val res_var=res1.floor.toLong;(x._1, res_min, res_max, res_mean+"B", res_var+"B")})

val lines = result_List.map(x=>{val res=x._1+","+x._2+","+x._3+","+x._4+","+x._5;res})
//output the result
lines.repartition(1).saveAsTextFile(outputDirPath)