# FlowJo export operator

The `FlowJo export operator` is an operator that creates an output that is created by running a FlowJo workflow on Tercen. The output can be imported in FlowJo.

##### Usage

Input projection|.
---|---
`row`               | factors, filename, rowId and result rows

Input parameters|.
---|---
`output_folder`     | directory where the output tables will be put

Output relations|.
---|---
`filename`          | name of the FCS file
result columns      | Results of the FlowJo workflow, e.g. FLOWSom clusters, UMAP results

##### Details

This operator can be used after one of these operators:
* [FLOWSom operator](https://github.com/tercen/flowsom_operator)
* [UMAP operator](https://github.com/tercen/umap_operator)
* [fast_tSNE operator](https://github.com/tercen/fast_tSNE_operator)
* [MEM_operator](https://github.com/tercen/MEM_operator)
