// Databricks notebook source
//Homewoek 3
//Junya Zhao
val df1 = spark.read.option("header", "true").option("inferSchema","true").csv("/FileStore/tables/movies.csv")
df1.createOrReplaceTempView("movies_table")
val df2 = spark.read.option("header", "true").option("inferSchema","true").csv("/FileStore/tables/movie_ratings.csv")
df2.createOrReplaceTempView("movie_reviews_table")
df2.show()
df1.count()

// COMMAND ----------

//Q3: Write DataFrame-based Spark code to find the number of distinct movies in the file movies.csv.
import org.apache.spark.sql.functions._
df1.agg(countDistinct ("title")).show
//so there is 1409 distinct movies in the file movies.csv.

// COMMAND ----------

//Q4: Write DataFrame-based Spark code to find the titles of the movies that appear in the file movies.csv but do not have a rating in the file movie_ratings.csv. Remark: the answer could be empty. 
//answer 1: In this problem I joined two table on both "year" and "title", I assumpted that moive that have the same name but in the different year are different movies.
import org.apache.spark.sql.functions._
val joined_Q4= df1.join(df2, df1("title")===df2("title")&&df1("year")===df2("year"),"left_outer").where(df2("title").isNull && df1("year").isNotNull).select(df1("title"),df1("year")).show()
//So the result show that the Movie "Beginners" that made in year 2004 do not have rating information.


//answer2:  if I joined two table on "title" only, I assumpted that moive have the same name but remade in different year are same moive.
val joined_Q4_0=df1.join(df2, df1("title")===df2("title"),"left_outer").where(df2("title").isNull && df1("year").isNotNull).select(df1("title"),df1("year")).show()

// COMMAND ----------

//Q5: Write DataFrame-based Spark code to find the number of movies that appear in the ratings file (i.e., movie_ratings.csv) but not in the movies file (i.e., movies.csv).
//Note: In this problem I joined two table on both "year" and "title", I assumpted that moive that have the same name but in the different year are different movies.
import org.apache.spark.sql.functions._
val joined=df1.withColumnRenamed("title","Movietitle").join(df2,'Movietitle==='title && df1("year")===df2("year")  ,"right_outer")
val result_Q5= joined.where(joined("Movietitle").isNull).agg(countDistinct("title")).show

//If I only consider the moive that have the same name but remade in different year as same moive, I dont care if they are in differnt year or not, the code is as follow
val joined_0=df1.withColumnRenamed("title","Movietitle").join(df2,'Movietitle==='title ,"right_outer")
val result_Q5_0=joined_0.where(joined_0("Movietitle").isNull).agg(countDistinct("title")).show 

// COMMAND ----------

//Q6: Write a DataFrame-based Spark code to find the total number of distinct movies that appear in either movies.csv, or movie_ratings.csv, or both.
import org.apache.spark.sql.functions._
val all= df1.withColumnRenamed("title", "Movietitle").join(df2, 'Movietitle==='title && df1("year")===df2("year"),"outer")
all.agg(countDistinct("title")).show()

// COMMAND ----------

//Q7: Write a DataFrame-based Spark code to find the title and year for movies that were remade. These movies appear more than once in the ratings file with the same title but different years. Sort the output by title.
import org.apache.spark.sql.functions._
val renamed = df2.groupBy("title").agg(countDistinct("year") as "counter").filter($"counter" >1).select("title")
val joined_result=df2.withColumnRenamed("title","Movietitle").
join(renamed,'title==='Movietitle,"right_outer").select("title","year").orderBy('title,'year.asc).show()

// COMMAND ----------

//Q8: Write a DataFrame-based Spark code to find the rating for every movie that the actor "Branson, Richard" appeared in. Schema of the output should be (title, year, rating)
//Note: In this problem I joined two table on both "year" and "title", I assumpted that moive that have the same name but in the different year are different movies.
import org.apache.spark.sql.functions._
val actorBranson=df1.withColumnRenamed("title","Movietitle").withColumnRenamed("year","Movieyear").join(df2,'Movietitle==='title &&'Movieyear==='year ,"inner").filter($"actor"==="Branson, Richard")
actorBranson.select("title","year","rating").show()

// COMMAND ----------

//Q9: Write a DataFrame-based Spark code to find the highest-rated movie per year and include all the actors in that movie. The output should have only one movie per year, and it should contain four columns: year, movie title, rating, and a list of actor names. Sort the output by year.
import org.apache.spark.sql.functions._
val rating=df2.groupBy("year").agg(max("rating") as "Max_Rating")
val joinedrating= rating.join(df2.withColumnRenamed("year","oldyear"),'year==='oldyear &&'Max_Rating==='rating)
val result= joinedrating.join(df1.withColumnRenamed("title","Movietitle").withColumnRenamed("year","Movieyear"), 'title==='Movietitle).groupBy("year","title","rating").agg(collect_set("actor")).orderBy('year.asc).show()

// COMMAND ----------

//Q10: Write a DataFrame-based Spark code to determine which pair of actors worked together most. Working together is defined as appearing in the same movie. The output should have three columns: actor 1, actor 2, and count. The output should be sorted by the count in descending order.
import org.apache.spark.sql.functions._
val selfjoined=df1.withColumnRenamed("actor","actor1").withColumnRenamed("title","Movietitle").join(df1.withColumnRenamed("actor","actor2"), 'Movietitle==='title).where("actor1!=actor2").groupBy("actor1","actor2").agg(countDistinct("title") as "count").orderBy('count.desc).show(1)

// COMMAND ----------

//Q11: [Extra Credit 20 points] 
import org.apache.spark.sql.functions._
//Task question 1:  how many movies that the actor who appear in the highest rating movie have been played in ? What is the averge rating of all the movie that the actor have played in?
//Step1: get the highest rating movie among all the moive in movie_rating.csv file
val HeighestRating= df2.agg(max("rating") as "MAX").join(df2,'MAX==='rating)
//Step2: get the actor name in the the highest rating moive, and then list all the movies that the actor have played in.
val joinedresult=HeighestRating.join(df1,Seq("title","year")).join(df1,Seq("actor"))
.drop(HeighestRating.col("year")).drop(HeighestRating.col("title")).select("title","year")
//Step3: calculate and show the total number of all the movie that the actor have played in.
val number = joinedresult.join(df2,Seq("title","year")).agg(count("title")).show()
//Step3: calculate and show the averge rating of all the movie that the actor have played in.
val average = joinedresult.join(df2,Seq("title","year")).agg(avg("rating")).show()
//Note: he have played in 12 movies, but the averga rating is only 1.81, which is much lower than the highest rating movie 'Beginners'

//-------------------------------------------------------------------------------------------//
// Task question 2: Movitated by Q7, there are 22 movies were remade. So I want to know if those moive make any improvement? if yes, what is the improvement percentage? is there any movie that were remade, but it get worse rating than before? if yes, how bad it is?
//Step1: get the list of renamed movie, this step is samlilar as Q7, i just used different method in thi step
val renamed = df2.groupBy("title").agg(countDistinct("year") as "counter").filter($"counter" >1).select("title")
val joined_2=df2.join(renamed,Seq("title"),"right_outer").orderBy('title,'year.asc)
//Step: self join moive rating table itself, so i get the pairwise comparison of every remade movies.
val pairwise= joined_2.join(joined_2.withColumnRenamed("year","year_1").withColumnRenamed("rating","rating_1"),Seq("title")).where("year!=year_1 and year<year_1")
//Step: calculate the Improvement percentage and rating difference of every remade movies, and add them  as another two column. see what's the performance
pairwise.selectExpr("title", "(rating_1/rating-1)*100 AS Improvement_percentage", "(rating_1-rating) AS difference").orderBy('Improvement_percentage.desc).show()
// The result show that only RoboCop, The Jungle Book, Footloose and Conan the Barbarian have made the improvement. The rating difference of Footloose and Conan the Barbarian are only 1.03 ad 2.33. The rest of remade moives actually have lower rating than before, it shows that remaking movies may not as good as the orginal one.

// COMMAND ----------


