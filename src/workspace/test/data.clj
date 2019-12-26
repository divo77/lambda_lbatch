(ns workspace.test.data)



; filtering the initial schema to have only immutables  used be a linkfil
;(reset! parsed-linkfile (file-loader "/home/docker/immutables/subv_test8/miniMemberGroup_short.json"))
;(reset! parsed-schema (assoc-in {:schema []} [:schema]
;                                (into []
;                                      (filter (fn [x] (in? (into (get-immutable-list)  ["reference.referencedisplay"] ) (get-in x [:name])   ))
;                                              (get-in
;                                                (file-loader "/home/docker/immutables/subv_test8/environmentSchema.json")
;                                                [:schema])
;                                              )
;                                      )
;                                ))

;(reset! parsed-schema (file-loader "/home/docker/immutables/subv_test8/environmentSchema.json"))
(def test_inc_join {:nodeId "memgroupalias",
                    :commands [{:type "join",
                                :joinType "full",
                                :immutable "memgroupalias.memgroupid",
                                :on [],
                                :select [{:key "memgroupalias.memgroupaliasid", :as "memGroupAliasID"}]}],
                    :as "nameType",
                    :incremental true,
                    :referenceCode {:referenceName "nameType", :as "nameType-ref"}
                    }
  )
(def test_inc_append {:type        "append",
                      :immutable   "reference.referencedisplay",
                      :on          [{:property "reference_name", :for "reference.referencename"}
                                    {:property "reference_code", :for "reference.referencecode"}],
                      :as          "reference_displayname"
                      :incremental true})


(def test_ref_join {:type      "join",
                    :joinType  "full",
                    :immutable "reference.referencedisplay",
                    :on        [],
                    :select    [{:key "reference.referencename", :as "reference_name"}
                                {:key "reference.referencecode", :as "reference_code"}]})
(def test_ref_append {:type      "append",
                      :immutable "reference.referencedisplay",
                      :on        [{:property "reference_name", :for "reference.referencename"}
                                  {:property "reference_code", :for "reference.referencecode"}],
                      :as        "reference_displayname"})

(def test_command {:type      "join",
                   :joinType  "full",
                   :immutable "acctbal.benperiodenddate",
                   :on        [],
                   :select    [{:key "acctbal.acctbalsetid", :as "acctbal_acctbalsetid"}
                               {:key "aacctbal.acctbalseqnum", :as "acctbal_acctbalseqnum"}]})

(def test_cad_command {:type          "append",
                       :immutable     "memgroupaddressphone.phonetype",
                       :on            [{:property "memGroupAddressPhoneID", :for "memgroupaddressphone.memgroupaddressphoneid"}],
                       :as            "phoneType", :incremental true
                       :referenceCode {:referenceName "phoneType", :as "phoneType-ref", :incremental true}})

(def test_user_params
  {:schema_file_name       "/home/docker/immutables/memgroupalias_memgroupaliasidsubv_test7/environmentSchema.json",
   :run_pig                false, :temp_folder "/home/docker/immutables/subv_test7/tmp",
   :mapping_file_name      "/home/docker/immutables/sub v_test7/miniMemberGroup.json",
   :data_folder_hdfs       "/datalake/optum/optuminsight/codadev1/dev/cirrus-test/rso_01/immutable",
   :log_folder             "/home/docker/immutables/subv_test7/logs",
   :pig_script_debug_mode  "NONE",
   :pig_udf_libs           [{:library_name      "/mapr/datalake/optum/optuminsight/codadev1/software/sbvgen/pigudf/coda-pig-udf-0.0.1-SNAPSHOT.jar",
                             :java_udf_fuction  "org.optum.coda.pig.udf.CodaJsonStoreFunc()",
                             :pig_function_name "codajsonstorage"}
                            {:library_name      "/mapr/datalake/optum/optuminsight/codadev1/software/sbvgen/pigudf/datafu-pig-incubating-1.3.1.jar",
                             :java_udf_fuction  "datafu.pig.bags.EmptyBagToNullFields()",
                             :pig_function_name "EmptyBagToNullFields"}],
   :pig_store_function     "codajsonstorage()",
   :pig_script_params      [{:parameter_name "mapred.child.java.opts", :value "-Xmx1024m"}
                            {:parameter_name "mapred.job.queue.name", :value "'coda_q1'"}
                            {:parameter_name "mapreduce.job.counters.max", :value "5000"}],
   :pig_mode               "MAPREDUCE",
   :pig_output_folder      "/home/docker/immutables/subv_test7/results/",
   :pig_output_folder_hdfs "/home/docker/immutables/subv_test7/results/"}
  )
