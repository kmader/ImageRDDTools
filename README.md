# ImageRDDTools

The project is made up of code and notebooks. The code for the relevant tools is in the ```src/``` folder.

- ```orthoworkflow.py``` is the primary file for the workflows and image setup
- ```simplespark.py``` is the fake spark for running workflows locally
- ```utils.py``` are the helper functions for both and interpython version support 

## Ortho Pipeline


### Approach 1

Naive read-all many times implementation (minimum shuffle, maximum reading)

![rdd_charts/naive_rdd.svg](Naive reading approach)

### Approach 2

Grouping the files before reading

![rdd_charts/naive_rdd.svg](Grouped reading approach)

### Approach 3

![rdd_charts/partial_rdd.svg](Partial reading approach)