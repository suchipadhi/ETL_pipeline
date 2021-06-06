# Getting Started with Python app for dataset extraction, cleaning and transformation and loading into csv.

Creating an application to develop an ETL pipeline that:
* [extracts data from a given file (we provide an example file)]
* [transforms the data (as described in the specifications)]
* [and returns a matrix (list of lists)]

## Prerequisites

You'll need the following:
* [Python (3.5)]

First, you have to install the dependencies listed in the [requirements.txt] to run it locally.

  ```
pip install -r requirements.txt
  ```

Inorder to check the result you find a "Output.csv" file in the folder which transforms the dataset as per the mentioned directions.

Below i have mentioned the link to my github repo:
* [https://github.com/suchipadhi/ETL_pipeline] 

1. To check rows in the output file:
INPUT Text file:
```
;aspiration;body-style;bore;city-mpg;compression-ratio;curb-weight;drive-wheels;engine-location;engine-size;engine-type;fuel-system;fuel-type;height;highway-mpg;horsepower;length;make;normalized-losses;num-of-cylinders;num-of-doors;peak-rpm;price;stroke;weight;wheel-base;width
0;std;convertible;3.47;21;9.00;2548;rwd;front;130;dohc;mpfi;gas;48.80;27;111,03;168.80;alfa-romero;-;four;two;5000;1349500;2.68;3;88.60;64.10

 ``` 

Snippet of OUTPUT CSV file:
  ```
  "[['engine-location', 'num-of-cylinders', 'engine-size', 'weight', 'horsepower', 'aspiration', 'price', 'make'], [10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10]]
  ```
