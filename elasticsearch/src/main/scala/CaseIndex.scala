import java.io.File
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.xml.XML
import org.apache.http.entity.StringEntity
import org.apache.http.client.methods.HttpPost
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils
import scalaj.http._
import scala.util.parsing.json.JSON






object CaseIndex {

//this function is for reading file inner Directory
    def readfilename(dir: File): Iterator[File] = {
        val d = dir.listFiles.filter(_.isDirectory)
        val f = dir.listFiles.filter(_.isFile).toIterator
        f ++ d.toIterator.flatMap(readfilename _)

    }



    def main(args: Array[String]): Unit = {


        val conf = new SparkConf()
        conf.setMaster("local").setAppName("CaseIndex")
        val sc = new SparkContext(conf)
        val inputFilePath:File = new File(args(0))
        var path = args(0)
        val files_name = readfilename(inputFilePath)
        //create the url for connect the elasticsearch
        val url_create = "http://localhost:9200/legal_idx?pretty"
        //build the elasticsearch database
        Http(url_create).method("PUT").asString
        //create the url for connect corenlp server
        val url = "http://localhost:9000/?properties=%7B%22annotators%22%3A%22ner%22%2C%22outputFormat%22%3A%22json%22%7D"
        //this loop start processing each file 
        for (filename <- files_name){
            var url_elst = "http://localhost:9200/legal_idx/cases/"
            var send_data= "{~id~:~"
            val textFile = filename.toString()
            //read xml file
            val xml = XML.loadFile(textFile)
            val content_1 = (xml \ "name").map(_.text)
            val content_2 = (xml \ "catchphrases" \ "catchphrase").map(_.text)
            val content_3 = (xml \ "sentences" \ "sentence").map(_.text)
            val content_4 = (xml \ "AustLII").map(_.text)
            val textFilename1 = textFile.replace(path,"").replace("/","")
            val textFilename = textFilename1.replace(".xml","")
            send_data+=s"$textFilename"
            send_data+="~,"
            send_data += "~name~:~"
            url_elst+=s"$textFilename"
            url_elst+="?pretty"
            for (item <- content_1){
                val content_string = item.toString()
                send_data += s"$content_string"

            }
            send_data+= "~,~Source_URL~:~"
            for (item <- content_4){
                val content_string = item.toString()
                send_data += s"$content_string"
            }
            send_data+= "~,~catchphrases~:~"
            for (item <- content_2){
                val content_string = item.toString()
                send_data += s"$content_string "
            }
            send_data+= "~,~Sentences~:~"
            for (item <- content_3){
                val content_string = item.toString()
                send_data += s"$content_string "
            }
            send_data+="~,~location~:~"



            var content = content_1.union(content_4)
            content = content.union(content_2)
            content = content.union(content_3)
            var person_ls = "~"
            var organ_ls = "~"
            // this loop is to process the nlp result
            for (item <- content){
                val content_str = item.toString()
                val requests = s"$content_str"
                val se = new StringEntity(requests)
                se.setContentEncoding("UTF-8")
                se.setContentType("application/json")
                val post = new HttpPost(url)
                val client = new DefaultHttpClient
                post.setEntity(se)
                val response = client.execute(post)
                val entity = response.getEntity()
                if(entity != null) {
                    val resStr = EntityUtils.toString(entity, "UTF-8");
                    //start to get the useful data from json
                    class CC[T] { def unapply(a:Any):Option[T] = Some(a.asInstanceOf[T]) }
                    object M extends CC[Map[String, Any]]
                    object L extends CC[List[Any]]
                    object D extends CC[List[Any]]
                    object Q extends CC[Map[String, Any]]
                    object B extends CC[String]
                    object C extends CC[String]
                    val result =  for {
                        Some(M(map)) <- List(JSON.parseFull(resStr))
                        L(sentences) = map("sentences")
                        M(sentence) <- sentences
                        D(tokens) = sentence("tokens")
                        Q(toke) <- tokens
                        B(word) = toke("word")
                        C(ner) = toke("ner")

                    } yield {
                        List(word,ner)
                    }

                    for (li <- result){
                        if (li(1)=="LOCATION"){
                            val word = li(0)
                            send_data+=s"$word"
                            send_data+=" "

                            }
                    }
                    for (li <- result){
                        if (li(1)=="PERSON"){
                            val word = li(0)
                            person_ls+=word
                            person_ls+=" "

                        }
                    }
                    for (li <- result){
                        if (li(1)=="ORGANIZATION"){
                            val word = li(0)
                            organ_ls+=word
                            organ_ls+=" "
                        }
                    }
                }

            }
            person_ls+="~"
            organ_ls+="~"
            send_data+="~"
            send_data = send_data.replaceAll("\n", " ").replaceAll(" {2,}", "")
            send_data+=",~person~:"
            send_data+=person_ls
            send_data+=",~organization~:"
            send_data+=organ_ls
            send_data+="}"
            send_data = send_data.replaceAll("\"","\\\\\"")
            send_data = send_data.replaceAll("\\~","\"")
            //send the processed data to elasticsearch server
            val status = Http(url_elst).method("PUT").postData(send_data).header("content-type", "application/json").asString


        }


    }
}
