
// Credentials to connect to mysql database
val driver = "com.mysql.cj.jdbc.Driver"
val url = "jdbc:mysql://localhost:3306/books_db"
val mysqluser = "***********"
val password = "******"
          
          
// Loading table data
                   
val books= spark.read.format("jdbc").option("url", url).option("driver", driver).option("dbtable", "books").option("user",mysqluser).option("password", password).load()

val book_tags= spark.read.format("jdbc").option("url", url).option("driver", driver).option("dbtable", "book_tags").option("user",mysqluser).option("password", password).load()

val tags= spark.read.format("jdbc").option("url", url).option("driver", driver).option("dbtable", "tags").option("user",mysqluser).option("password", password).load()

val ratings= spark.read.format("jdbc").option("url", url).option("driver", driver).option("dbtable", "ratings").option("user",mysqluser).option("password", password).load()

val to_read= spark.read.format("jdbc").option("url", url).option("driver", driver).option("dbtable", "to_read").option("user",mysqluser).option("password", password).load()


//Writing the data into hdfs
         
books.write.format("csv").option("sep", ",").option("header", "true").save("hdfs://primarynode:9000/books_db/books.csv")

book_tags.write.format("csv").option("sep", ",").option("header", "true").save("hdfs://primarynode:9000/books_db/book_tags.csv")

tags.write.format("csv").option("sep", ",").option("header", "true").save("hdfs://primarynode:9000/books_db/tags.csv")

ratings.write.format("csv").option("sep", ",").option("header", "true").save("hdfs://primarynode:9000/books_db/ratings.csv")

to_read.write.format("csv").option("sep", ",").option("header", "true").save("hdfs://primarynode:9000/books_db/to_read.csv")
