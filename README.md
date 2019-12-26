# workspace

THis is an attempt to build the lambda batch layer for one of the Big Data projects.

the architecture diagram is:

CDC -> NiFi  ------------------------------------------------------------------------------->  (PIG: execution) -> RESULTS
                                                                                                    /
UI(Mapping and execution plan) -> (lambda_lbatch .Parsing an execution plan and produce Pig script)/

