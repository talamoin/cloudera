# cloudera
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
Job1_Mapper - Parser Mapper which reads input from file line by line with Key : Article_name and Value : Outlinks and timestamp appended together.
The input is split based on white spaces and tabs using a string Tokenizer.

Job1_Reducer - Parser Reducer which assigns Page rank to each article as 1 and emits key value pair.
the reducer also combines article_names (key) each key corresponds to values which is a list of outlinks with different dates. 
The reducer gets the most updated article_name by comparing the dates with the input_date from the terminal

Job2_Mapper - Rank Calculation Mapper class whose ouput is being fed into Job2_Mapper also which parses the rank and the 
outlinks value and converts it to a List.

Job2_Reducer -Rank Calculation Reducer class which calculates Page Rank.

Job3_Mapper - This class is for Sorting and sanitization.

Article - This class acts as getter and setter for timestamp and outlinks.

Main Class  - This is a driver class for all the above classes and we define here all our

We have removed any references to 
1. self loops -
2. multiple occurences of outlinks by using Hashset in our implementation as it removes duplicate element.

The flow
---------
Main Class --> Job1_Mapper --> Job1_Reducer --> Job2_Mapper --> Job2_Reducer --> Job3_Mapper

How to Execute
---------------
clean maven using mvn clean package

User should enter execute in the terminal in the following order :
input file path, output path,(assumed non-existent), number of iterations for the PageRank algorithm (integer >= 1),
the date Y for which the PageRank scores will be computed (in ISO8601 format).

Variables
----------


Assumptions
-------------

It is assumed that while running the program user enters command in same order as specified above.
The program is written for wiki data set in a multi line format.

Performance
--------------------------


