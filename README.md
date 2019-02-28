Page Rank Implementation in Java Using MapReduce 
-------------------------------------------------
MapReduce is a software framework for parallel computation in a distributed environemnt often called, clusters. As the very name suggests,
it contains a Mapper class,Reducer class and a driver class which drives these classes.

Goal
------
* Implement Page Rank algorithm as descibed below.

* Dataset : parsed version of Wikipedia Edit History in a tagged multi line format.

* Output : print article name followed by their Page Rank (separate line for each article).

* Format : Text (being written to a file)


How to Execute
---------------
~~~~
mvn clean package

export HADOOP_CLASSPATH="$PWD/target/uog-bigdata-0.0.1-SNAPSHOT.jar"


hadoop finalMR.Main_class [INPUT_FILE] [Intermediate_output] [#_OF_ITERATION] [Timestamp_ISO8601 format]

for example:
hadoop finalMR.Main_class /user/enwiki/enwiki-20080103-sample.txt iter 5 2008-01-01T00:00:00Z
~~~~

User should enter execute in the terminal in the following order :
1. input file path
2. intermediate output path
3. number of iterations for the PageRank algorithm (integer >= 1),
4. the date Y for which the PageRank scores will be computed (in ISO8601 format).
#### PLEASE NOTE: The final output will be stored in a file called result



Class Description
------------------------
## Job1_Mapper - Parser

### Key : serializable int 
### Value :line by line from input text

Read Input File line by line, Compare timestamps, send only revisions with timestamp before the input date 

## Job1: Parser

## Job1_Reducer
Input:
### Key : Article_name
### Value : Initial Rank(1.0)+Outlinks+timestamp

Parser Reducer emits key value pair.
the reducer also combines article_names (key) each key corresponds to values which is a list of outlinks with different dates. 
The reducer gets the most updated article_name by comparing the dates with the input_date from the terminal

## Job2: Rank Calculations

## Job2_Mapper 
Input:
### Key : Article_name  
### Value : Outlinks for the recent outlinks before timestamp



## Job2_Reducer  

### Key : Article_name
### Value : if it starts with # :Strings 0 # character, String 1 outlinks
	### if it doesnt start with # : String 0 article_name , String 1 old rank ,String 2 article_count

Rank Calculation Reducer class which calculates Page Rank based considering damping factor 0.85.

## Job3: Output
## Job3_Mapper - 

### Key : Article_name 
### Value : Rank written by the context object

This class is for Sorting (based on article name ) and sanitization. the output is written as <article name, rank>  



Article - This class acts as getter and setter for timestamp and outlinks.

Main Class  - This is a driver class for all the above classes and it drives the aforementioned jobs. We define here input/ouput format etc.



The flow
---------
Main Class --> Job1_Mapper --> Job1_Reducer --> Job2_Mapper --> Job2_Reducer --> Job3_Mapper  --> Final Output

Please note that Page rank calculation is an iterative process and is runas defined by number of iteration from command line.
Job 2(Rank Calculator) has been made to execute as user defined number of iterations from the commandline(5 as suggested in coure work).

Assumptions
-------------

1. We have asssumed enough memory in the system to store Reducer's Output as Reducer's ouput is stored locally as per HDFS architecture.

2. The program is written for wiki data set in a multi line format and it is assumed that while running the program user enters command in same orderas specified above.

3. We have also asssumed that the input has lines starts with "MAIN", "REVISION" and "TEXTDATA".

4. The code would also run when the input was fifty times bigger.



Performance
---------------
1. We can have more Mappers and read the file in chunks instead of line by line.

2. We can avoid passing complex key value pairs from mapper to reducer so that we can minimize network I/O.

3. We can also include Combiners to improve Reducer's job performance.

4. We can simplify the task as much as possible before sending it to the Reducer.

5. We have used String Tokenizer rather than String Split which improves performance.

6. We are using HashSet in stead of looping to remove duplicates.

7. The number of reducers should not be static
