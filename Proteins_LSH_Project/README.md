# PDD 2024 - Lab Project 1
**Protein clustering with LSH**

# Introduction
The aim of the project was to check if LSH clustering is sufficient for clustering protein expressions obtained from UniProt database.

# Usage
## Requirements

1. Docker
2. Python 3.10

Project was developed on Windows 11 with WSL2.

## Setting up
Before you start the containers, edit the docker-compose.yml and dockerfile files. Change the volume paths to project paths on your PC (use an absolute path). In the dockerfile, change the path to the notebook_dir (last line) to the same path.

To set up the containers, run the following commands:

`docker pull bitnami/spark:3.5.0`

`jupyter/pyspark-notebook:latest`

`docker-compose up --build`

Check your Docker Desktop app to see if the containers are running.

If the jupyter server is asking for a token, execute the following command in the Jupyter container:

`jupyter server list`

to obtain the token for your notebook. You can then access the server (although the token should not be needed).

## Running the notebook
Jupyter notebook server is available at:

http://localhost:8888/lab/workspaces/auto-8

Spark GUI is available at:

http://localhost:8080/

To run the notebook, enter the Jupyter server and run the lsh_analysis.ipynb file. Make sure that the PROJECT_PATH variable in the second cell is set to the project directory on your PC.

# Author
Łukasz Niedźwiedzki (Student ID 419328)

## Course Information
Big data processing and cluster computing course at Faculty Of Mathematics, Informatics and Mechanics, University of Warsaw

# To do:
1. Fix preprocess.ipynb
2. Better analysis