# ImageRDDTools

The project is made up of code and notebooks. The code for the relevant tools is in the ```src/``` folder.

- ```orthoworkflow.py``` is the primary file for the workflows and image setup
- ```simplespark.py``` is the fake spark for running workflows locally
- ```utils.py``` are the helper functions for both and interpython version support 

## Ortho Pipeline


### Approach 1

Naive read-all many times implementation (minimum shuffle, maximum reading)

![Naive reading approach](rdd_charts/naive_rdd.svg?raw=true)

### Approach 2 (Selected)

Grouping the files before reading

![Grouped reading approach](rdd_charts/naive_rdd.svg?raw=true)

### Approach 3

![Partial reading approach](rdd_charts/partial_rdd.svg?raw=true)

## JP2000 Pipeline

The full JP2 pipelines are shown with the grouped reading approach for its performance and simplicity of implementations

### Single File Output

![Single Output approach](rdd_charts/full_pipe_rdd.svg?raw=true)

### Multiple File Output

This approach is not being taken but could potentially substantailly improve overall performance, since blocks could be written much sooner

![Naive reading approach](rdd_charts/full_par_pipe_rdd.svg?raw=true)