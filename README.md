ETHOS.zoomin: A spatial disaggregation workflow tool developed within the LOCALISED project.
==============================

A workflow tool that:
1. Pulls data from a database.
2. Disaggregates it based on proxy specifications present in the database.
3. Evaulates quality rating - based on quality of the data to be disaggregated, proxy data and 
the confidence in the assigned proxy.  
4. Dumps the disaggregated data into the database.

--------

Installation steps 
------------

0. Before you begin:

Please make sure you have miniforge installed on your machine

Also create the initial database. Steps to create the Database will follow soon. 


1. Clone this repository:
    ```bash
    git clone https://jugit.fz-juelich.de/iek-3/shared-code/localised/ETHOS.zoomin.git
    ```

2. Install dependencies and the repo in a clean conda environment:
    ```bash
    cd ETHOS.zoomin
    mamba env create --file=requirements.yml
    conda activate zoomin
    pip install -e .
    ```

3. Run the workflow from command line:
    ```bash
    bash run_deployment.sh
    ```

<p><small>Project based on the <a target="_blank" href="https://drivendata.github.io/cookiecutter-data-science/">cookiecutter data science project template</a>. #cookiecutterdatascience</small></p>

## Acknowledgement
This work was developed as part of the project [LOCALISED](https://www.localised-project.eu/) —Localised decarbonization pathways for citizens, local administrations and businesses to inform for mitigation and adaptation action. This project received funding from the European Union’s Horizon 2020 research and innovation programme under grant agreement No. 101036458. This work was also supported by the Helmholtz Association as part of the program “Energy System Design”. 
