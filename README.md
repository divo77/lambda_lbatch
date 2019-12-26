# Some info

THis is an attempt to build the lambda batch layer for one of the Big Data projects.

the architecture diagram is:

```
CDC -> NiFi  -------------->  (PIG: execution) -> RESULTS
                         /
UI() -> (lambda_lbatch )/
```

- CDC                              - Maxwell to read the MySql logs
- Nifi                             - NiFi to transform the CDC stream to both Immutable and RAW streams and store it on Lake
- lambda_lbatch                    - Parsing an execution plan and produce Pig script
- UI(Mapping and execution plan)   - provides  the UI to join immutables and produce the execution plan for a batch(speed) layer.
