<h2>A: MAPREDUCE</h2>

<h3>Setting up the Environment</h3>
<p>1. Create a new conda virtual environment <br/>

`conda create -n mapreduce_env python=3` <br/></p>
<p>2. Activate the environment <br/>

`conda activate mapreduce_env` <br/></p>
<p>3. Install the dependencies using the following command:<br />

`pip install -r requirements` <br/> </p>

<h3> Running the exercises </h3>
<p>4. Run the Part (1) using the following command: <br/>

`python mapreduce_1.py <path-to-orders.csv> <path-to-customers.csv>` <br/></p>
<p>5. Run the Part (2) using the following command: <br/>

`python mapreduce_2.py <path-to-orders.csv> <path-to-customers.csv>` <br/></p>

<h3> Output </h3>
<p>The output is logged into console as well as saved to the dir:
 

`output/mapreduce_1` for task 1.</br>

`output/mapreduce_2` for task 2.</br>

The files can be on local system or in the hdfs. In case of reading from hdfs, make sure that the hadoop cluster is running and files are added to the hadoop cluster. </p>
