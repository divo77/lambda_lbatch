(ns workspace.gl.vars)


(def WAIT_ON_LOCK_TIME 3)
(def WAIT_ON_LOCK_RUN_ATTEMPTS 3)
(def INCREMENTAL_TS 0)
(def DEFAULT_CONFIG "/config/default.conf")

(def VERSION "1.0.001")

(def pig-debug-command-counter (ref 0))
(def pig-command-collection (ref []))
(def node-properties-collection (ref []))
(def node-timestamp-properties-collection (ref []))
(def node-cogroup-collection (ref []))
(def node-cogroup-flatten-collection (ref []))
(def pig-branch-commands (ref []))

(def node-join-collection (ref []))


(def parsed-linkfile (atom {}))
(def parsed-schema (atom {}))

(def data-uds-name "INPUT_DATA")
(def DISTINCT_INPUT_DATA false)

(def joining_flag (ref 0))

(def COGROUP_AFTER_FILTER false)                            ;Type of the filter command if FILTER_TYPE=F then do cogroup
;\ on LOAD-COGROUP-FILTER later if FILTER_TYPE=FC then LOAD-FILTER-COGROUP

(def LOAD_PREFILTER true)                                   ;USE filter command right after LOAD to reduce the size of dataset

(def JOINING_METHOD "COGROUP")



(def reference-node

  {:nodeId   "reference",
   :commands [
              {:type      "join",
               :joinType  "full",
               :immutable "reference.referencedisplay",
               :on        [],
               :select    [{:key "reference.referencename", :as "reference_name"}
                           {:key "reference.referencecode", :as "reference_code"}
                           ]
               }
              {:type      "append",
               :immutable "reference.referencedisplay",
               :on        [{:property "reference_name", :for "reference.referencename"}
                           {:property "reference_code", :for "reference.referencecode"}
                           ],
               :as        "reference_displayname"}
              ]
   }

  )

(def pig_ret_codes
  [{:code 0 :desc "Success"}
   {:code 1 :desc "Retriable failure"}
   {:code 2 :desc "Failure"}
   {:code 3 :desc "Partial failure"}
   {:code 4 :desc "Illegal arguments passed to Pig"}
   {:code 5 :desc "IOException thrown"}
   {:code 6 :desc "PigException thrown"}
   {:code 7 :desc "ParseException thrown (can happen after parsing if variable substitution is being done)"}
   {:code 8 :desc "Throwable thrown (an unexpected exception)"}])