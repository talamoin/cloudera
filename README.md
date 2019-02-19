Page Rank Implementation in Java Using MapReduce 
-------------------------------------------------
MapReduce is a software framework for parallel computation in a distributed environemnt often called, clusters. As the very name suggests,
it contains a Mapper class,Reducer class and a driver class which drives these classes.

Goal
------
Implement Page Rank alorithm as descibed below.
Dataset : parsed version of Wikipedia Edit History in a tagged multi line format.
Output : print article name followed by their Page Rank (separate line for each article).
Format : Text (being written to a file)

Our Solution :

Class Description
------------------------
Job1_Mapper - 

Key : Article_name and Value : Initial Rank(1.0)+Outlinks+timestamp appended together

Parser Mapper which reads input from file line by line.The input is split based on white spaces and tabs using a string Tokenizer 
which performs better than normal Split.

Job1_Reducer - 

Key : Article_name and Value : Initial Rank(1.0)+Outlinks

Parser Reducer emits key value pair.
the reducer also combines article_names (key) each key corresponds to values which is a list of outlinks with different dates. 
The reducer gets the most updated article_name by comparing the dates with the input_date from the terminal

Job2_Mapper -

Key : Article_name  and Value : Outlinks

Rank Calculation Mapper class whose ouput is being fed into Job2_Mapper also which parses the rank and the 
outlinks value and converts it to a List

Job2_Reducer -

Key : Article_name and Value :

Rank Calculation Reducer class which calculates Page Rank based considering damping factor 0.85.

Job3_Mapper - 

Key :  and Value : 

This class is for Sorting and sanitization.

Article - This class acts as getter and setter for timestamp and outlinks.

Main Class  - This is a driver class for all the above classes and it drives the aforementioned jobs. We define here input/ouput format etc.



The flow
---------
Main Class --> Job1_Mapper --> Job1_Reducer --> Job2_Mapper --> Job2_Reducer --> Job3_Mapper  --> Final Output

Please note that Page rank calculation is an iterative process and is run untill convergence which is defined by number of iteration.
in order to achieve this, Job 2(Rank Calculator) has been made to execute as user defined number of iterations from the commandline(ideally 5).

How to Execute
---------------
clean maven using mvn clean package

User should enter execute in the terminal in the following order :
input file path, output path,(assumed non-existent), number of iterations for the PageRank algorithm (integer >= 1),
the date Y for which the PageRank scores will be computed (in ISO8601 format).

Assumptions
-------------

1.We have asssumed enough memory in the system to store Reducer's Output as Reducer's ouput is stored locally as per HDFS architecture.

2. The program is written for wiki data set in a multi line format and it is assumed that while running the program user enters command in same order
as specified above.

3. We have also asssumed that the input has lines starts with "MAIN", "REVISION" and "TEXTDATA".

4. The code would also run when the input was fifty times bigger.



Performance
---------------
1. We can have more Mappers and read the file in chunks instead of line by line.

2. We can avoid passing complex key value pairs from mapper to reducer so that we can minimize network I/O.

3. We can also include Combiners to improve Reducer's job performance.

4. We can simplify the task as much as possible before sending it to the Reducer.

5. We have used String Tokenizer rather than String Split which improves performance.
