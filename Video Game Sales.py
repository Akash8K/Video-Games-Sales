# Databricks notebook source
# MAGIC %md 
# MAGIC <h1 style="text-align:center"><font style="color:red; font-size:70px; text-shadow: 2px 2px 2px #3b3b3b;">Video Games Sales</font></h1>

# COMMAND ----------

# DBTITLE 1,Questions/Problems
# 	1) Find Overall Most Sold Game in North America
# 	2) Find Overall Most Sold Game in Japan
# 	3) Find Overall Most Sold Game in Europe
# 	4) Find Overall Most Sold Game in Other Countries
# 	5) Find Overall Most Sold Game Globally
# 	6) Find Globally Highest Sold Games - Platform wise
# 	7) Find Highest Sold Games in Japan - Platform wise 
# 	8) Which game was sold lowest globally ?
# 	9) Platform which has lowest sales globally 
# 	10) In which year , Nintendo and Take-Two-Interactive  games Sales was highest ?
# 	11) In which year, Nintendo and Take-Two-Interactive  games Sales was lowest?
# 	12) Average sales of Action Games in Europe
# 	13) Find Average sales of Shooter Games in Japan
# 	14) Total sales of Electronic Arts games Globally
# 	15) Total sales of Sega vs Total sales of Capcom in Japan
# 	16) Find how many total games of Take-Two Interactive
# 	17) Find the year in which Nintendo's lowest sales globally 
# 	18) Which platform has most Racing Games?
# 	19) Which genre games sales is highest/Lowest globally ?
#   20) Which year has most Role-Playing Games?
#   21) In which year , Activision and Ubisoft games Sales was highest ?

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Data Import</font></h3>

# COMMAND ----------

# DBTITLE 1,Import the Files/Datasets in the HDFS to create the dataframe using PySpark
# creating a new directory for the project files


# dbutils.fs.mkdirs("/FileStore/Video_Games/")

dbutils.fs.ls("/FileStore/Video_Games")

#upload the files manually inside the Video_Games directory after executing this code. 

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Dataframe</font></h3>

# COMMAND ----------

# DBTITLE 1,PySpark Dataframe
Games_Sales = spark.read.csv("/FileStore/Video_Games/video_games_sales.csv",inferSchema=True,header=True)
Games_Sales.printSchema() #-->to check the schema is correct or not
display(Games_Sales)

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Dataset Summary</font></h3>

# COMMAND ----------

# DBTITLE 1,Calculate The Summary of data
#using describe method to calculate the summary

Games_Sales.describe().show()
      

#and getting count of rows and columns from the dataset
def rows_n_columns(path):
    print("count of rows in mentioned dataset => ",path.count())
    print("\n","count of columns in mentioned dataset => ",len(path.columns))

rows_n_columns(Games_Sales)


# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Removing Nulls</font></h3>

# COMMAND ----------

# DBTITLE 1,Now creating a python Function to drop the null values from dataset
#empty data removing function --> 

def remove_null_values(df):
                         return print(f"count after dropping nulls --> {df.na.drop().count()} \n Total null rows removed --> {df.count() - df.na.drop().count()}")

#call the function -->
remove_null_values(Games_Sales)

# COMMAND ----------

# DBTITLE 1,Now The Data is Clean So we are ready to solve the questions
Games_Sales.show()

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Overall Sales</font></h3>

# COMMAND ----------

# DBTITLE 1,Overall most sold game
#overall most sold game in North America
from pyspark.sql.functions import desc

na_max = Games_Sales.orderBy(desc('na_sales')).limit(1).select('name','genre','publisher','year','na_sales')
display(na_max)

#overall most sold game in Europe
eu_max = Games_Sales.orderBy(desc('eu_sales')).limit(1).select('name','genre','publisher','year','eu_sales')
display(eu_max)

#overall most sold game in Japan
jp_max = Games_Sales.orderBy(desc('jp_sales')).limit(1).select('name','genre','publisher','year','jp_sales')
display(jp_max)

#overall most sold game in other countries
other_max = Games_Sales.orderBy(desc('other_sales')).limit(1).select('name','genre','publisher','year','other_sales')
display(other_max)

#overall most sold game Globally
global_max = Games_Sales.orderBy(desc('global_sales')).limit(1).select('name','genre','publisher','year','global_sales')
display(global_max)

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Globally Highest Sold games - Platform wise</font></h3>

# COMMAND ----------

# DBTITLE 1,Globally Highest Sold games - Platform wise
#globally highest sold games - platform wise

from pyspark.sql.functions import col, max, first
globally_highest = (Games_Sales
             .groupBy('platform')
             .agg(max('global_sales').alias('highest_global_sales'), 
                  first('name').alias('name'),
                  first('year').alias('year')
                 )
             .orderBy('highest_global_sales', ascending=False))

display(globally_highest)

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Highest Sold Games in Japan -Platform wise</font></h3>

# COMMAND ----------

# DBTITLE 1,Highest Sold Games in Japan -Platform wise
#highest sold games in japan - platform wise

globally_highest = (Games_Sales
             .groupBy('platform')
             .agg(max('jp_sales').alias('highest_Japan_sales'), 
                  first('name').alias('name'),
                  first('year').alias('year')
                 )
             .orderBy('highest_Japan_sales', ascending=False))

display(globally_highest)

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Game which has lowest sales overall</font></h3>

# COMMAND ----------

# DBTITLE 1,Which game was sold lowest globally ?
#Lowest sold game Globally

global_min = Games_Sales.orderBy(('global_sales')).limit(1).select('name','genre','publisher','year','global_sales')
display(global_min)


# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Platform which has lowest sales overall</font></h3>

# COMMAND ----------

# DBTITLE 1,Platform which has lowest sales globally
min_global = Games_Sales.orderBy(('global_sales')).limit(1).agg(first('platform').alias('Platform_which_has_lowest_sales_globally'))
min_global.show()


# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Nintendo and Take-Two Interactive</font></h3>

# COMMAND ----------

# DBTITLE 1,In which year - Nintendo and Take-Two-Interactive  games Sales was highest 
from pyspark.sql.functions import max

filtered_df = Games_Sales.filter(Games_Sales.publisher.isin(['Nintendo', 'Take-Two Interactive']))

result_df = filtered_df.groupBy('publisher', 'year').agg(max('global_sales').alias('max_global_sales')) \
          .orderBy('max_global_sales', ascending=False) \
          .groupBy('publisher').agg({'year': 'first', 'max_global_sales': 'first'})

display(result_df)


#explaination for personal understanding -->


# The code starts by importing the 'max' function from the PySpark SQL module. This function is used to find the maximum value of a column in a PySpark dataframe.
# Next, the 'Games_Sales' dataframe is filtered to include only the rows where the 'publisher' column contains the values 'Nintendo' or 'Take-Two Interactive'. The resulting dataframe is stored in the variable 'filtered_df'.
# The 'filtered_df' dataframe is then grouped by the 'publisher' and 'year' columns, and the maximum value of the 'global_sales' column is found for each group using the 'max' function. The resulting dataframe is stored in the variable 'result_df'.
# The 'result_df' dataframe is then ordered in descending order based on the 'max_global_sales' column, and only the first row for each 'publisher' is selected using the 'groupBy' and 'agg' functions. The resulting dataframe is displayed using the 'display' function.
# The same code has been rewritten in a more concise form using method chaining. The 'agg' function is used to find the maximum value of the 'global_sales' column for each group, and the resulting dataframe is immediately ordered in descending order based on the 'max_global_sales' column. The 'groupBy' and 'agg' functions are then used again to select only the first row for each 'publisher' and display the resulting dataframe. 
# The backslash ('\') is used to continue a line of code onto the next line. This is done for readability purposes, to avoid having a very long line of code that is difficult to read. The backslash tells Python that the code continues on the next line.




# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Ubisoft And Activision</font></h3>

# COMMAND ----------

# DBTITLE 1,In which year - Ubisoft and Activision  games Sales was highest 
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

filtered_df = Games_Sales.filter(Games_Sales.publisher.isin(['Ubisoft', 'Activision']))

window_spec = Window.partitionBy('publisher').orderBy(desc('global_sales'))

result_df = filtered_df.select('publisher', 'year', 'global_sales', \
              rank().over(window_spec).alias('rank')) \
              .where('rank = 1').drop('rank')

display(result_df)

#explaination for self understanding -->

# The first line of code imports the necessary functions from the PySpark library to create a window and rank the data.

# The second line of code filters the data to only include games sold by two specific publishers, Nintendo and Take-Two Interactive. The filtered data is stored in a new variable called filtered_df.

# The third line of code creates a window specification that partitions the data by publisher and orders it by global sales in descending order. This window specification is stored in a new variable called window_spec.

# The fourth line of code selects specific columns from the filtered data, including the publisher, year, and global sales. It also ranks the data according to the window specification created in the previous line of code and gives the rank an alias of 'rank'. This new column is also included in the data selection.

# The fifth line of code filters the data to only include rows where the rank is equal to 1.

# The sixth line of code drops the rank column from the resulting data and displays it using the display function.

# In summary, this code filters and ranks video game sales data by publisher, selecting only the top-selling game for each publisher and displaying the results in a table format. The output table will include the publisher name, the year the game was released, and the global sales of the top-selling game for each publisher.

# The code uses PySpark's powerful window functions to rank the data and select only the top-selling game for each publisher. This is a great example of how PySpark can be used to efficiently manipulate large datasets and extract valuable insights.


# COMMAND ----------

# DBTITLE 1,In which year - Nintendo and Take-Two-Interactive  games Sales was Lowest
from pyspark.sql.functions import min

filtered_df = Games_Sales.filter(Games_Sales.publisher.isin(['Nintendo', 'Take-Two Interactive']))

result_df = filtered_df.groupBy('publisher', 'year').agg(min('global_sales').alias('min_global_sales')) \
          .orderBy('min_global_sales', ascending=True) \
          .groupBy('publisher').agg({'year': 'first', 'min_global_sales': 'first'})

display(result_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Average sales of Action Games in Europe</font></h3>

# COMMAND ----------

# DBTITLE 1,Average sales of Action Games in Europe
from pyspark.sql.functions import avg, col, sum

avg_sales = Games_Sales.filter("genre = 'Action' and eu_sales is not null").agg(avg("eu_sales")).collect()[0][0]
print(f"Average sales of Action games in Europe: {round(avg_sales*100,4)} Millions ")

#code explaination for self understanding -->

# 1. `avg_sales = Games_Sales.filter("genre = 'Action' and eu_sales is not null")`: This line filters out the rows where the genre is "Action" and the `eu_sales` column is not null.

# 2. `.agg(avg("eu_sales"))`: This line applies the `avg` function to the `eu_sales` column to calculate the average sales of Action games in Europe.

# 3. `.collect()[0][0]`: This line collects the result of the above aggregation, which returns a list of rows, and then extracts the first element of the first row which is the average sales value.

# 4. `print(f"Average sales of Action games in Europe: {avg_sales*100} Millions ")`: This line prints the average sales value of Action games in Europe in millions. The `f` before the string allows us to use curly braces `{}` to insert the value of the variable `avg_sales` into the string directly.


# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Average sales of Shooter Games in Japan</font></h3>

# COMMAND ----------

# DBTITLE 1,Find Average sales of Shooter Games in Japan
jp_avg_sales = Games_Sales.filter("genre = 'Shooter' and jp_sales is not null").agg(avg("jp_sales")).collect()[0][0]
print(f"Average sales of Shooter games in Japan: {round(jp_avg_sales*100,4)} Millions ")

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Total Sales of EA Games Global Level</font></h3>

# COMMAND ----------

# DBTITLE 1,Total sales of EA games Globally
EA_total = Games_Sales.filter(col('publisher')=='Electronic Arts').agg(sum('global_sales')).collect()[0][0]
print(f"Total sales of EA games globally: {round(EA_total,2)} Millions")

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Sega VS Capcom</font></h3>

# COMMAND ----------

# DBTITLE 1,Total sales of Sega vs Total sales of Capcom in Japan
sega_sales = Games_Sales.filter(col("publisher") == "Sega").agg(sum("jp_sales")).collect()[0][0]
capcom_sales = Games_Sales.filter(col("publisher") == "Capcom").agg(sum("jp_sales")).collect()[0][0]

print(f"Total sales of Sega games in Japan: {round(sega_sales,2)} Millions")
print(f"Total sales of Capcom games in Japan: {round(capcom_sales,2)} Millions")

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Total Games By take two Interactive</font></h3>

# COMMAND ----------

# DBTITLE 1,Total Number Games Published by take-two interactive
from pyspark.sql.functions import count
take_two_games = Games_Sales.filter(col("publisher") == "Take-Two Interactive").agg(count("name")).collect()[0][0]
print(f"Total number of games published by Take-Two Interactive: {take_two_games}")

#code explaination for personal understanding -->

# Here, we first filter the rows where the publisher is "Take-Two Interactive" using the `filter()` function. Then, we apply the `count()` function to the `name` column to calculate the total number of games published by Take-Two Interactive. Finally, we collect the result and print it with an appropriate message using an f-string.


# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Lowest Selling Year for Nintendo</font></h3>

# COMMAND ----------

# DBTITLE 1,find the year in which Nintendo had the lowest global sales
lowest_sales_year = Games_Sales.filter(col("publisher") == "Nintendo").groupBy("year").agg(min("global_sales")).orderBy("min(global_sales)").select("year").collect()[0][0]
print(f"Year in which Nintendo had the lowest global sales: {int(lowest_sales_year)}")

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Platform with most racing ggames</font></h3>

# COMMAND ----------

# DBTITLE 1,find which platform has most Racing Games
most_racing_platform = Games_Sales.filter(col("genre") == "Racing").groupBy("platform").agg(count("name")).orderBy("count(name)", ascending=False).select("platform").collect()[0][0]
print(f"The platform that has the most racing games is: {most_racing_platform}")

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Highest selling genre globally</font></h3>

# COMMAND ----------

# DBTITLE 1,Which genre games sales is highest/Lowest globally ?
highest_sales_genre = Games_Sales.groupBy("genre").agg(sum("global_sales")).orderBy("sum(global_sales)", ascending=False).select("genre").collect()[0][0]
lowest_sales_genre = Games_Sales.groupBy("genre").agg(sum("global_sales")).orderBy("sum(global_sales)").select("genre").collect()[0][0]

print(f"The genre of games with the highest global sales is: {highest_sales_genre}")
print(f"The genre of games with the lowest global sales is: {lowest_sales_genre}")

# COMMAND ----------

# MAGIC %md 
# MAGIC <h3 style="text-align:center"><font style="color:#fcdf03; font-size:50px; text-shadow: 2px 2px 2px #424242; text-transform:uppercase">Year on which most rpg's published </font></h3>

# COMMAND ----------

# DBTITLE 1,Which year has most Role-Playing Games?
from pyspark.sql.functions import count, col

most_rpg_year = Games_Sales.filter(col("genre") == "Role-Playing").groupBy("year").agg(count("name")).orderBy("count(name)", ascending=False).select("year").collect()[0][0]
print(f"The year with the most Role-Playing games is: {int(most_rpg_year)}")
