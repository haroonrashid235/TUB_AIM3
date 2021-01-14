<h2>B: SPARK</h2>

<h3>Setting up the Environment</h3>
<p>1. Create a new conda virtual environment <br/>

`conda create -n spark_env python=3` <br/></p>
<p>2. Activate the environment <br/>

`conda activate spark_env` <br/></p>
<p>3. Install the dependencies using the following command:<br />

`pip install -r requirements` <br/> </p>

<h3> Running the exercises </h3>
<p>4. Run the Part (1) using the following command: <br/>

`python spark_1.py` <br/></p>
<p>5. Run the Part (2) using the following command: <br/>

`python spark_2.py` <br/></p>

<h3> Output </h3>
<p>The output is logged into console as well as saved to the dir:

`output/spark1` for task 1.</br>

`output/shortest_path` for task 2.</br>

In case of pyspark, the cluster doesn't need to be run separately. Pyspark does everything to execute spark jobs locally.
