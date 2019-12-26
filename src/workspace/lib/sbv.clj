(ns workspace.lib.sbv
  (:gen-class)
  (:use clojure.test)
  (:require
    [cheshire.core :refer :all]
    [clojure.string :as string]
    [clojure.java.shell :only [sh]]
    [clj-time.core :as t]
    [clj-time.format :as f]
    [clojure.tools.cli :refer [parse-opts]]
    [clojure.edn :as edn]
    [me.raynes.fs :as rfs]
    [workspace.gl.vars :refer :all]
    ;  [clojure.java.io :only [output-stream]]
    )
  )


(use 'com.rpl.specter)
(use 'com.rpl.specter.macros)

(defn file-loader [input-file]
  ;;loads in a link file, returns hash-map
  (parse-string (slurp input-file) true))

(defn create-timestamp-file
  "docstring"
  [p_file_name
   p_timestamp
   p_status

   ]
  (try
    (spit p_file_name {:status p_status :timestamp p_timestamp})
    (catch Throwable t
      (throw (ex-info (format "Error writing timestamp file file: %s" p_file_name)
                      {:file p_file_name} t))))

  )

(defn get-immutable-name
  [p_command]
  (get-in p_command [:immutable]))

(defn create-property
  "docstring"
  [
   p_property_name
   p_data_type
   ]
  [{:name p_property_name :dataType p_data_type}]
  )

(defn in?
  "true if coll contains elm"
  [coll elm]
  (if (some #(= elm %) coll) true false))

(defn get-schema-seq
  []
  (vec (take (count (get-in @parsed-schema [:schema])) (range))))

(defn pig-storeEasy [file_name str_to_write]
  ;; stores pig output to jsonOut folder
  (spit file_name str_to_write :append true))

(defn alter-debug-command-counter
  [a]
  (dosync (alter a inc)))

(defn alter-ref-vector
  [a
   value]
  (dosync (alter a conj value)))
(defn alter-ref-vector3
  [a
   value]
  (dosync (alter a into value)))

(defn set-ref-vector [a
                      value]
  (dosync (ref-set a value)))

(defn replace-nonpig-symbols
  [inpit_str]
  (if (nil? inpit_str) "" (clojure.string/replace inpit_str #"\.|\-|\/|\\" {"." "_" "-" "_" "/" "_" "\\" "_"})
                       ))

(defn mySql-to-PIG-datatype
  [mysql_datatype]
  (try
    (
      case mysql_datatype
      ("bigint" "biginteger") "long"
      "float" "float"
      ("double" "decimal" "dec") "double"
      ("tinyint" "mediumint" "smallint" "int") "int"
      ("timestamp" "datetime" "date" "varchar" "char") "chararray"
      ("blob" "text") "bytearray"

      )
    (catch Exception e (str "bytearray")))
  )

(defn pig-notNull
  "docstring"
  [p_property_name
   p_property_datatype]

  (try
    ;  (
    ;    case p_property_datatype
    ;    ("long" "int" ) (str "("p_property_name " == '<NULL>'?0:("p_property_datatype ")" p_property_name ") as " p_property_name)
    ;    ("float" "double") (str "("p_property_name " == '<NULL>'?0.0:("p_property_datatype ")"p_property_name ") as " p_property_name)
    ;    ("bytearray" "chararray" "datetime")           (str "("p_property_name " == '<NULL>'?'':("p_property_datatype ")" p_property_name ") as " p_property_name )
    ;    )

    (
      case p_property_datatype
      ("long" "int") (str "(" p_property_name " == '<null>'?0:(" p_property_datatype ")" p_property_name ")")
      ("float" "double") (str "(" p_property_name " == '<null>'?0.0:(" p_property_datatype ")" p_property_name ")")
      ("bytearray" "chararray" "datetime") (str "(" p_property_name " == '<null>'?NULL:(" p_property_datatype ")" p_property_name ")")
      )

    ;(
    ;  case p_property_datatype
    ;  ("long" "int") (str "(UPPER(" p_property_name ") == '<NULL>'?0:(" p_property_datatype ")" p_property_name ")")
    ;  ("float" "double") (str "(UPPER(" p_property_name ") == '<NULL>'?0.0:(" p_property_datatype ")" p_property_name ")")
    ;  ("bytearray" "chararray" "datetime") (str "(UPPER(" p_property_name ") == '<NULL>'?NULL:(" p_property_datatype ")" p_property_name ")")
    ;  )
    (catch Exception e (str "NULL")))
  )

;(defn get-schema-immutable
;  "docstring"
;  [p_immutable_name]
;  (nth (for [x (vec (get-schema-seq))
;             :let [y (get-in @parsed-schema [:schema x])]
;             :when (if (= (get-in y [:name]) p_immutable_name) true)] y) 0)
;  )

;(defn get-schema-immutable-key
;  [p_immutable_name]
;  (nth (for [x (vec (get-schema-seq))
;             :let [y (get-in @parsed-schema [:schema x])]
;             :when (if (= (get-in y [:name]) p_immutable_name) true)] (get-in y [:schemaData :key])) 0))







(defn store-pig-script
  [
   p_pig_script_name_fp
   ]
  (with-open [w (clojure.java.io/writer p_pig_script_name_fp :append true)]
    (binding [*out* w]
      (doall (map (fn [x] (.write  *out* (str x "\n") ) )
                  @pig-command-collection
                  ))
      )
    )
  )


(defn get-immutable-list
  "docstring"
  []
  (let [immutable_list (into [] (distinct (remove nil? (select [:nodes ALL :commands ALL :immutable] @parsed-linkfile))))]
    immutable_list
    )

  )

(defn get-timestamps-property
  "docstring"
  [p_property_name]
  {:name (str "timestamp_" p_property_name) :dataType "long"}
  )

(defn get-timestamps-properties
  "docstring"
  [p_properties_list]
  (let [prop_list (into [] (map (fn [x] (get-timestamps-property x))
                                p_properties_list))]
    prop_list
    )
  )

(defn append-incremental?
  "docstring"
  [p_command]
  (if (contains? p_command :incremental) true false)

  )

(use '[clojure.java.shell :only [sh]])
(use 'com.rpl.specter)
(use 'com.rpl.specter.macros)

(def user-config (atom {}))

(defn load-cfg-file
  "Load a single config file into a config map. Returns a tuple of the
  given filename and the resulting config "
  [file]
  (try
    [file (edn/read-string {:readers *data-readers*} (slurp file))]
    (catch Throwable t
      (throw (ex-info (format "Error loading config file: %s" file)
                      {:file file} t)))))


(defn load-cfg
  [config_file_name]
  (let [a (load-cfg-file config_file_name)]
    (get-in a [1])
    )
  )

(defn get-cfg
  "Look up the given keys in the specified config. If the key is not
  present, throw an error explaining which key was missing."
  [cfg & keys]
  (let [v (get-in cfg keys ::none)]
    (if (= v ::none)
      ;(throw (ex-info (format "Attempted to read undefined configuration value %s" keys) {:keys keys}))
      nil
      v)))




(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn pig-set-commands
  []
  (let [
        v_pig_hadoop_params (get-cfg @user-config :pig_script_params)

        _ (println v_pig_hadoop_params)

        ]

    (if (some? v_pig_hadoop_params)
      (doseq [x v_pig_hadoop_params]
        (alter-ref-vector pig-command-collection (str "SET  " (get-in x [:parameter_name])
                                                      "  "
                                                      (get-in x [:value]))))

      )
    )

  )

(defn pig-register-commands
  []
  (let [
        v_pig_udf_register (get-cfg @user-config :pig_udf_register)

        ]
    (if (some? v_pig_udf_register)
      (doseq [x v_pig_udf_register]
        (alter-ref-vector pig-command-collection (str "REGISTER " (get-in x [:library_name])))
        )
      )

    )
  )

(defn pig-define-commands
  []
  (let [
        v_pig_udf_define_params (get-cfg @user-config :pig_udf_define)

        ]
    (if (some? v_pig_udf_define_params)
      (doseq [x v_pig_udf_define_params]
        (alter-ref-vector pig-command-collection (str "DEFINE " (get-in x [:pig_function_name])
                                                      "  " (get-in x [:java_udf_fuction]) ";"))
        )
      )
    )
  )

(defn pig-store-command
  [
   p_last_ds
   p_pig_res_folder_hdfs
   ]

  (let [
        ;v_pig_store_func (get-cfg @user-config :pig_store_function)
        v_inc_flag (get-cfg @user-config :incremental_flag)
        v_res_gr (get-cfg @user-config :result_grouping)
        ]

    (if (= v_inc_flag true)
      ;memgroupalias = FILTER memgroupalias  by NOT  codaincfilter(*);
      (alter-ref-vector pig-command-collection (str p_last_ds "= FILTER  " p_last_ds " by codaincfilter(*);"))
      )

    (if (= v_res_gr true)
      (alter-ref-vector pig-command-collection (str "group_tmp = GROUP  " p_last_ds " ALL;"
                                                    p_last_ds " = FOREACH group_tmp GENERATE '" p_last_ds "' as sbvname,'" (get-cfg @user-config :starttimestamp) "' as starttime,'" (get-cfg @user-config :endtimestamp) "' as endtimestamp, group_tmp." p_last_ds " as sbvdata,
                                                                true as MAXTIMSTAMP__SYSTEM__;"
                                                    ))
      )

    (if (= v_inc_flag true)
      ;memgroupalias = FILTER memgroupalias  by NOT  codaincfilter(*);
      (alter-ref-vector pig-command-collection (str "STORE " p_last_ds " INTO '" p_pig_res_folder_hdfs "' USING codajsonstorageinc();"))
      (alter-ref-vector pig-command-collection (str "STORE " p_last_ds " INTO '" p_pig_res_folder_hdfs "' USING codajsonstorage();"))
      )

    )
  )


;(pig-max-in-array ["b" "c" "d"] "a" 11111)

;(defn pig-max-in-array
;  "docstring"
;  [p_seq
;   p_pig_command
;   p_timestamp
;   ]
;  (if (empty? p_pig_command) "false"
;
;                             (if-not (empty? (first p_seq))
;                               (let [
;                                     v_first (first p_seq)
;                                     p_pig_command (str "(" p_pig_command ">" v_first "?" p_pig_command ":" v_first ")")
;                                     ]
;                                 (recur (rest p_seq) p_pig_command p_timestamp)
;
;                                 )
;                               (str "(" p_pig_command ">" p_timestamp "?(boolean)'true':(boolean)'false')")
;                               )
;
;                             )
;
;
;  )

(defn pig-filter-immutable-inc2
  [
   p_immutable_name
   p_command_counter
   ]
  (let [v_starttimestamp (get-cfg @user-config :starttimestamp)
        v_endtimestamp (get-cfg @user-config :endtimestamp)

        v_uds_name (str (if (nil? p_immutable_name) "" (replace-nonpig-symbols p_immutable_name)) p_command_counter)
        v_split_name1 (str v_uds_name "_split1")
        v_split_name2 (str v_uds_name "_split2")
        v_hist_name1 (str v_uds_name "_hist1")
        v_hist_name2 (str v_uds_name "_hist2")
        v_union_name (str v_uds_name "_union")
        ;
        ;SPLIT a INTO b1 IF (immutable_name == 'imm1' and (ts >=9 and ts < 11)),b2 IF (immutable_name == 'imm1' and (ts <9))

        split_command (str " SPLIT " data-uds-name " INTO " v_split_name1 " IF (immutable_name == '" p_immutable_name "' and ts >= " v_starttimestamp " ),"
                           v_split_name2 " IF (immutable_name == '" p_immutable_name "' and (ts <" v_starttimestamp "));")

        hist_filter_command1 (str v_hist_name1 "  = FOREACH (GROUP " v_split_name1 " by join_key )"
                                  "{
                                    byts1 = ORDER " v_split_name1 " BY ts DESC;
                                   res1 = LIMIT byts1 1;
                                   GENERATE FLATTEN(res1);
                                   };"
                                  )
        hist_filter_command2 (str v_hist_name2 "  = FOREACH (GROUP " v_split_name2 " by join_key )"
                                  "{
                                    byts2 = ORDER " v_split_name2 " BY ts DESC;
                                    newest2 = LIMIT byts2 1;
                                    res2 = FILTER newest2 by (value != '<deleted>');
                                    GENERATE FLATTEN(res2);
                                   };"
                                  )
        union_command (str v_union_name "= UNION " v_hist_name1 "," v_hist_name2 ";")

        ;g= FOREACH (GROUP e BY join_key) { f= ORDER e by ts DESC; l= LIMIT f 1; GENERATE FLATTEN(l);};

        final_hist_command (str v_uds_name "  = FOREACH (GROUP " v_union_name " by join_key )"
                                "{
                                  byts2 = ORDER " v_union_name " BY ts DESC;
                                                     res2 = LIMIT byts2 1;
                                                     GENERATE FLATTEN(res2);
                                                     };"
                                )


        ]
    (do
      (alter-ref-vector pig-command-collection split_command)
      (alter-ref-vector pig-command-collection hist_filter_command1)
      (alter-ref-vector pig-command-collection hist_filter_command2)
      (alter-ref-vector pig-command-collection union_command)
      (alter-ref-vector pig-command-collection final_hist_command)

      )
    ; (if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))

    v_uds_name))

(defn pig-filter-immutable
  [
   p_immutable_name
   p_command_counter
   ]
  (let [
        v_uds_name (str (if (nil? p_immutable_name) "" (replace-nonpig-symbols p_immutable_name)) p_command_counter)
        command (str v_uds_name " = FILTER " data-uds-name " BY  (immutable_name == '" p_immutable_name "' );")
        hist_filter_command (str v_uds_name "  = FOREACH (GROUP " v_uds_name " by join_key )"
                                 "{
                                   byts = ORDER " v_uds_name " BY ts DESC;
                                   newest = LIMIT byts 1;
                                   filtered = FILTER newest by (value != '<deleted>');
                                   GENERATE FLATTEN(filtered);
                                   };"
                                 )

        ]
    (if COGROUP_AFTER_FILTER
      (do
        (alter-ref-vector pig-command-collection command)
        (alter-ref-vector pig-command-collection hist_filter_command)
        )
      (alter-ref-vector pig-command-collection command)

      )


    ; (if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))

    v_uds_name))

(defn pig-filter-immutable-inc
  [
   p_immutable_name
   p_command_counter
   ]
  (let [
        v_starttimestamp (get-cfg @user-config :starttimestamp)
        v_endtimestamp (get-cfg @user-config :endtimestamp)
        v_uds_name (str (if (nil? p_immutable_name) "" (replace-nonpig-symbols p_immutable_name)) p_command_counter)
        ; command (str v_uds_name " = FILTER " data-uds-name " BY  (immutable_name == '" p_immutable_name "' and ts < " v_endtimestamp ");")
        command (str v_uds_name " = FILTER " data-uds-name " BY  (immutable_name == '" p_immutable_name "' );")
        ;hist_filter_command (str v_uds_name "  = FOREACH (GROUP " v_uds_name " by join_key )"
        ;                         "{byts = ORDER " v_uds_name " BY ts DESC;
        ;                           newest = LIMIT byts 1;
        ;                         filtered = FILTER newest by (value != '<deleted>' and ts<" v_starttimestamp ");
        ;                           GENERATE FLATTEN(filtered);
        ;                           };"
        ;                         )

        hist_filter_command (str v_uds_name "  = FOREACH (GROUP " v_uds_name " by join_key )"
                                 "{byts = ORDER " v_uds_name " BY ts DESC;
                                   newest = LIMIT byts 1;
                                   GENERATE FLATTEN(filtered);
                                   };"
                                 )

        ]
    (if COGROUP_AFTER_FILTER
      (do
        (alter-ref-vector pig-command-collection command)
        (alter-ref-vector pig-command-collection hist_filter_command)
        )
      (alter-ref-vector pig-command-collection command)

      )


    ; (if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))

    v_uds_name))

(defn pig-join
  [p_ds_name
   p_ds_name_left
   p_join_property_left
   p_ds_name_right
   p_join_property_right
   p_command_counter]
  (let [;p_immutable_name (clojure.string/replace p_immutable_name #"\." "")
        command (str p_ds_name "= JOIN " p_ds_name_left "  by " p_join_property_left " LEFT OUTER," p_ds_name_right " by " p_join_property_right ";")]
    (do
      (alter-ref-vector pig-command-collection command)
      )

    p_ds_name))


(defn pig-join-first
  [p_ds_name
   p_ds_name_left
   p_join_property_left
   ]
  (alter-ref-vector node-join-collection (str p_ds_name_left "  by (" p_join_property_left ")"))
  p_ds_name
  )

;(defn pig-join-rest
;  [p_ds_name
;   p_ds_name_right
;   p_join_property_right
;   ]
;  (alter-ref-vector node-join-collection (str p_ds_name_right "  by (" p_join_property_right ")"))
;  p_ds_name
;)
;
;
;(defn pig-multi-join-command
;  [ds_name
;   p_join_array
;  ]
;
;  (let [
;        command (str ds_name "= JOIN " (clojure.string/join ",\n" p_join_array) ";" )
;        ]
;    (alter-ref-vector pig-command-collection command)
;    )
;  )

(defn pig-cogroup-leftouter-first
  [p_ds_name
   p_ds_name_left
   p_join_property_left
   ]
  (let [
        command (str p_ds_name_left "  by (" p_join_property_left ")")
        flatten (str "FLATTEN(" p_ds_name_left ")")]

    (do
      (alter-ref-vector node-cogroup-collection command)
      (alter-ref-vector node-cogroup-flatten-collection flatten)
      )

    p_ds_name))

(defn pig-cogroup-leftouter-rest
  [p_ds_name
   p_ds_name_right
   p_join_property_right
   ]
  (let [
        command (str p_ds_name_right "  by (" p_join_property_right ")")
        flatten (str "FLATTEN(EmptyBagToNullFields(" p_ds_name_right "))")]

    (do
      (alter-ref-vector node-cogroup-collection command)
      (alter-ref-vector node-cogroup-flatten-collection flatten)
      )

    p_ds_name))





(defn pig-node-cogroup-command
  [ds_name
   p_cogroup_array
   p_flatten_array]

  (let [
        command (str ds_name "= FOREACH ( COGROUP \n" (clojure.string/join ",\n" p_cogroup_array)
                     ") GENERATE \n"
                     (clojure.string/join ",\n" p_flatten_array)
                     ";"
                     )
        ]
    (alter-ref-vector pig-command-collection command)
    )
  )

;(defn pig-node-join-command
;  [ds_name
;   p_cogroup_array
;]
;
;  (let [
;        command (str ds_name "= JOIN  " (clojure.string/join ",\n" (map (fn [x]
;                                                                          (str
;                                                                            (get-in x [:ds_name])
;                                                                            " by "
;                                                                            (get-in x [:join_property])
;
;                                                                            )
;                                                                          )
;                                                                        @p_cogroup_array
;                                                                        )
;
;                                                             )
;                     ";"
;
;
;                     )
;        ]
;    (alter-ref-vector pig-command-collection command)
;    )
;  )

(defn pig-cogroup
  [

   p_relation_name
   p_relation_property_name
   p_ds_name_left
   p_join_property_left
   p_ds_name_right
   p_join_property_right
   ]
  (let [;p_immutable_name (clojure.string/replace p_immutable_name #"\." "")
        command (str p_relation_name "= COGROUP " p_ds_name_left " by " p_join_property_left "," p_ds_name_right " by " p_join_property_right ";")
        flatten_command (str p_relation_name "= FOREACH " p_relation_name " GENERATE FLATTEN(" p_ds_name_left ")," p_ds_name_right " as " p_relation_property_name ";")
        v_property (create-property p_relation_property_name "COGROUP")
        ]

    (alter-ref-vector pig-branch-commands command)
    (alter-ref-vector pig-branch-commands flatten_command)
    (alter-ref-vector3 node-properties-collection v_property)
    p_relation_name))




(def left (ref []))
(def right (ref []))

(defn parse-branche-on
  [p_on]
  (set-ref-vector right [])
  (set-ref-vector left [])
  (dorun (map (fn [x]
                (alter-ref-vector left (get-in p_on [:on x :them]))
                (alter-ref-vector right (get-in p_on [:on x :me]))
                )
              (vec (take (count (get-in p_on [:on])) (range)))))

  )

(defn get-joinkey-destruct-clause
  "docstring"
  [p_command]
  (let [
        v_immutable_name (nth (select :immutable p_command) 0)
        v_schema_info (nth (select [:schema ALL VAL :name #(= v_immutable_name %)] @parsed-schema) 0)
        v_key (into [] (sort-by :ordinality (nth (select [ALL :schemaData :key] v_schema_info) 0)))
        ; v_immutable_timestamp (str (replace-nonpig-symbols v_immutable_name) "_SYSTEM_TS")
        prefix_str (str "GENERATE FLATTEN(REGEX_EXTRACT_ALL(join_key,'^"
                        (clojure.string/join "\\\\|" (repeat (count v_key) "(.*)")) "$')) as (")
        postfix_str (str "),FLATTEN(REGEX_EXTRACT_ALL(value,'^(.*)$')) as (value:chararray),ts as timestamp;")
        property_str (replace-nonpig-symbols (str (clojure.string/join ":chararray," (select [ALL :name] v_key)) ":chararray"))
        ]

    (str prefix_str property_str postfix_str)
    )

  )



(defn get-join-prop-list3

  [p_command]

  (let [
        v_immutable_name (nth (select :immutable p_command) 0)
        v_schema_info (nth (select [:schema ALL VAL :name #(= v_immutable_name %)] @parsed-schema) 0)
        v_key (into [] (sort-by :ordinality (nth (select [ALL :schemaData :key] v_schema_info) 0)))

        ]

    (into [] (map
               (fn [x]
                 (do
                   (assoc-in
                     (assoc-in {:name nil :dataType nil} [:name] (replace-nonpig-symbols (get-in x [:as])))
                     [:dataType] (mySql-to-PIG-datatype (get-in (
                                                                  select [ALL VAL :name #(= (get-in x [:key]) %)] v_key

                                                                         ) [0 0 :dataType]))
                     ))
                 )

               (select [:select ALL] p_command)

               )
          )

    )
  )

(defn get-join-prop-list3-mj

  [p_command]

  (let [
        v_immutable_name (nth (select :immutable p_command) 0)
        v_schema_info (nth (select [:schema ALL VAL :name #(= v_immutable_name %)] @parsed-schema) 0)
        v_key (into [] (sort-by :ordinality (nth (select [ALL :schemaData :key] v_schema_info) 0)))

        ]

    (into [] (map
               (fn [x]
                 (do
                   (assoc-in
                     (assoc-in {:name nil :dataType nil} [:name] (replace-nonpig-symbols (get-in x [:as])))
                     [:dataType] (mySql-to-PIG-datatype (get-in (
                                                                  select [ALL VAL :name #(= (get-in x [:key]) %)] v_key

                                                                         ) [0 0 :dataType]))
                     ))
                 )

               (select [:select ALL] p_command)

               )
          )

    )
  )

(defn get-join-prop-list

  [p_command]

  (let [
        v_immutable_name (nth (select :immutable p_command) 0)
        v_schema_info (nth (select [:schema ALL VAL :name #(= v_immutable_name %)] @parsed-schema) 0)
        v_key (into [] (sort-by :ordinality (nth (select [ALL :schemaData :key] v_schema_info) 0)))
        ]

    (into [] (map
               (fn [x]
                 (do
                   (assoc-in (assoc-in
                               (assoc-in {:name nil :as nil :dataType nil} [:as] (replace-nonpig-symbols (get-in x [:as])))
                               [:dataType] (mySql-to-PIG-datatype (get-in (
                                                                            select [ALL VAL :name #(= (get-in x [:key]) %)] v_key

                                                                                   ) [0 0 :dataType]))
                               )
                             [:name] (replace-nonpig-symbols (get-in x [:key]))
                             )
                   )
                 )

               (select [:select ALL] p_command)

               )
          )

    )
  )


(defn get-append-join-prop-list
  [p_command]
  (let [

        l (clojure.string/join "," (map (fn [x]
                                          (replace-nonpig-symbols (get-in p_command [:on x :property]))
                                          )
                                        (vec (take (count (get-in p_command [:on])) (range)))
                                        ))

        r (clojure.string/join "," (map (fn [x]
                                          (replace-nonpig-symbols (get-in p_command [:on x :for]))
                                          )
                                        (vec (take (count (get-in p_command [:on])) (range)))
                                        ))

        ]
    {:left l :right r}
    ))

(defn get-append-prop-list
  [p_command]

  (let [
        v_immutable_name (nth (select :immutable p_command) 0)
        v_schema_info (nth (select [:schema ALL VAL :name #(= v_immutable_name %)] @parsed-schema) 0)
        v_key (into [] (sort-by :ordinality (nth (select [ALL :schemaData :key] v_schema_info) 0)))
        ;_ (println v_key)
        ]

    (into [] (map
               (fn [x]
                 (do
                   (assoc-in
                     (assoc-in {:name nil :dataType nil} [:name] (replace-nonpig-symbols (get-in x [:for])))
                     [:dataType] (mySql-to-PIG-datatype (get-in (
                                                                  select [ALL VAL :name #(= (get-in x [:for]) %)] v_key

                                                                         ) [0 0 :dataType]))
                     ))
                 )

               (select [:on ALL] p_command)

               )
          )

    )

  )

(defn get-value-property

  [p_command
   ]

  (let [

        v_immutable_name (nth (select :immutable p_command) 0)
        v_schema_info (nth (select [:schema ALL VAL :name #(= v_immutable_name %)] @parsed-schema) 0)
        v_value_datatype (nth (select [ALL :schemaData :valueDataType] v_schema_info) 0)
        property_name (replace-nonpig-symbols (get-in p_command [:as]))
        ]
    [{:name property_name :dataType (mySql-to-PIG-datatype v_value_datatype)}]
    )
  )

;(defn get-value-property-inc
;
;  [p_command
;   ]
;
;  (let [
;
;        v_immutable_name (nth (select :immutable p_command) 0)
;        v_schema_info (nth (select [:schema ALL VAL :name #(= v_immutable_name %)] @parsed-schema) 0)
;        v_value_datatype (nth (select [ALL :schemaData :valueDataType] v_schema_info) 0)
;        property_name (replace-nonpig-symbols (get-in p_command [:as]))
;        ]
;    [{:name property_name :dataType (mySql-to-PIG-datatype v_value_datatype)} (get-timestamps-property property_name)]
;    )
;  )


(defn pig-load
  []
  (let [
        v_p_datafile_folder_hdfs (get-cfg @user-config :data_folder_hdfs)
        v_starttimestamp (get-cfg @user-config :starttimestamp)
        v_endtimestamp (get-cfg @user-config :endtimestamp)
        v_inc_mode     (get-cfg @user-config :incremental_flag)

        command (str "RAW_DATA =  LOAD '"
                     (str v_p_datafile_folder_hdfs)
                     "' using PigStorage('\\"
                     "t') AS (immutable_name:chararray,join_key:chararray,ts:double,value:chararray);")

        initilal_filtering  (str  "INPUT_DATA = FILTER  RAW_DATA  BY ts < (double)" v_endtimestamp " and  In2(immutable_name,'"
                                  (clojure.string/join "','" (get-immutable-list)) "','reference.referencedisplay');"
                                  )

        hist_filter_command_inc (str data-uds-name "  = FOREACH (GROUP " data-uds-name " BY (immutable_name,join_key) )"
                                     "{
                                       byts = ORDER " data-uds-name " BY ts DESC;
                                       filtered = LIMIT byts 1;
                                       res = FILTER filtered by ((value != '<deleted>' and ts <= "v_starttimestamp
                                     ") or (ts > " v_starttimestamp "));
                                   GENERATE FLATTEN(res);
                                   };"
                                     )

        hist_filter_command_full (str data-uds-name "  = FOREACH (GROUP " data-uds-name " BY (immutable_name,join_key) )"
                                      "{
                                        byts = ORDER " data-uds-name " BY ts DESC;
                                        filtered = LIMIT byts 1;
                                        res = FILTER filtered by ((value != '<deleted>');
                                    GENERATE FLATTEN(res);
                                    };"
                                      )
        distinct_input_data     (str data-uds-name " = DISTINCT " data-uds-name ";")


        ]

    (alter-ref-vector pig-command-collection command)
    (alter-ref-vector pig-command-collection initilal_filtering)
    (if DISTINCT_INPUT_DATA (alter-ref-vector pig-command-collection distinct_input_data))

    (if-not COGROUP_AFTER_FILTER
      (if (= v_inc_mode true)
        (alter-ref-vector pig-command-collection hist_filter_command_inc)
        (alter-ref-vector pig-command-collection hist_filter_command_full)
        )
      )



    )
  )

(def pig-cad-node-join-commands (ref []))



(defn cad-join

  [p_ds_name
   p_command
   p_count]

  (let [
        v_reference_name (replace-nonpig-symbols (get-in p_command [:referenceCode :referenceName]))
        v_ref_prop_name (replace-nonpig-symbols (get-in p_command [:referenceCode :as]))
        v_property_name (replace-nonpig-symbols (get-in p_command [:as]))
        filter_command (str v_ref_prop_name p_count " = FILTER reference BY  (reference_name == '" v_reference_name "');")
        projection_command (str v_ref_prop_name p_count " = FOREACH " v_ref_prop_name p_count " GENERATE reference_code, reference_displayname as " v_ref_prop_name ";")
        join_command (str p_ds_name "= JOIN " p_ds_name " by (" v_property_name
                          ") LEFT OUTER," v_ref_prop_name p_count " by (reference_code) USING 'replicated';")
        ]
    (alter-ref-vector3 node-properties-collection (create-property v_ref_prop_name "chararray"))

    (alter-ref-vector pig-cad-node-join-commands filter_command)
    (alter-ref-vector pig-cad-node-join-commands projection_command)
    (alter-ref-vector pig-cad-node-join-commands join_command)
    )

  )

(defn pig-join-foreach-final-inc
  [p_nodeId
   p_ds_name
   p_timestamp_property_list
   p_ts
   ]
  (let [

        v_property_list (clojure.string/join "," (map
                                                   (fn [x]
                                                     (get-in x [:name])
                                                     )
                                                   @node-properties-collection
                                                   ))

        command (str p_nodeId "= FOREACH " p_ds_name " GENERATE " v_property_list
                     (if-not (empty? p_timestamp_property_list)
                       (str ",codainctt((double)" p_ts "," (clojure.string/join "," p_timestamp_property_list) ")")
                       ",false"
                       )
                      " as MAXTIMSTAMP__SYSTEM__"
                     ";")]
    (alter-ref-vector pig-command-collection command)
    ))


(defn pig-join-select-inc
  [p_command
   p_command_counter]
  (let [
        v_immutable_name (replace-nonpig-symbols (get-immutable-name p_command))
        v_uds_name (str v_immutable_name p_command_counter)
        key_full_destruct (str v_uds_name "= FOREACH " v_immutable_name " " (get-joinkey-destruct-clause p_command))
        ;select_property_list (get-join-prop-list p_command)
        join_propperties (get-join-prop-list p_command)
        select_property_list (into []
                                   (map
                                     (fn [x] (str (if-not (= "COGROUP" (get-in x [:dataType]))
                                                    (str
                                                      (pig-notNull (replace-nonpig-symbols (get-in x [:name])) (get-in x [:dataType]))
                                                      )
                                                    (get-in x [:name])
                                                    )

                                                  " as " (get-in x [:as])))
                                     join_propperties)
                                   )

        v_timestamp_property_list (get-timestamps-properties (select [ALL :as] join_propperties))
        _ (println join_propperties)
         _ (println v_timestamp_property_list)
        command (str v_uds_name "= FOREACH " v_uds_name " GENERATE " (clojure.string/join "," select_property_list) ","
                     (clojure.string/join "," (map (fn [x] (str "timestamp as " x))
                                                   (select [ALL :name] v_timestamp_property_list))) ";")
        ]

    (alter-ref-vector3 node-properties-collection (get-join-prop-list3 p_command))
    (alter-ref-vector3 node-timestamp-properties-collection v_timestamp_property_list)

    (alter-ref-vector pig-command-collection key_full_destruct)

    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))

    (alter-ref-vector pig-command-collection command)

    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))
    ))

(defn pig-join-select2-inc
  [p_command
   p_node_name
   p_command_counter]
  (let [
        v_immutable_name (replace-nonpig-symbols (get-immutable-name p_command))
        v_uds_name (str v_immutable_name p_command_counter)
        key_full_destruct (str v_uds_name "= FOREACH " v_immutable_name " " (get-joinkey-destruct-clause p_command))
        join_propperties (get-join-prop-list p_command)

        select_property_list (into []
                                   (map
                                     (fn [x] (str (if-not (= "COGROUP" (get-in x [:dataType]))
                                                    (str
                                                      (pig-notNull (get-in x [:name]) (get-in x [:dataType]))
                                                      )
                                                    (get-in x [:name])
                                                    )

                                                  " as " (get-in x [:as])))
                                     join_propperties)
                                   )

        v_timestamp_property_list (get-timestamps-properties (select [ALL :as] join_propperties))
        _ (println join_propperties)
        _ (println v_timestamp_property_list)
        _ (println (append-incremental? p_command))
        _ (println p_command)
        command (str p_node_name "= FOREACH " v_uds_name " GENERATE " (clojure.string/join "," select_property_list) ","
                     (clojure.string/join "," (map (fn [x] (str "timestamp as " x))
                                                   (select [ALL :name] v_timestamp_property_list))) ";")
        ]

    (alter-ref-vector3 node-properties-collection (get-join-prop-list3 p_command))
        (if (append-incremental? p_command) (alter-ref-vector3 node-timestamp-properties-collection v_timestamp_property_list))
    ;(alter-ref-vector3 node-timestamp-properties-collection v_timestamp_property_list)
    (alter-ref-vector pig-command-collection key_full_destruct)

    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))

    (alter-ref-vector pig-command-collection command)

    ;(if (= (config :pig_script_debug) true)    (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))
    ))

(defn pig-append-select-inc
  [p_command
   p_command_counter
   ]
  (let [
        v_immutable_name (replace-nonpig-symbols (get-immutable-name p_command))
        v_uds_name (str v_immutable_name p_command_counter)
        key_full_destruct (str v_uds_name "= FOREACH " v_immutable_name " " (get-joinkey-destruct-clause p_command))


        key_property_list (into []
                                (map
                                  (fn [x] (str (if-not (= "COGROUP" (get-in x [:dataType]))
                                                 (str
                                                   (pig-notNull (get-in x [:name]) (get-in x [:dataType]))
                                                   )
                                                 (get-in x [:name])
                                                 )

                                               " as " (get-in x [:name])))
                                  (get-append-prop-list p_command))
                                )

        value_property (get-value-property p_command)
        value_timestamp_property [(get-timestamps-property (get-in value_property [0 :name]))]
        _ (println (str "*" value_timestamp_property))
        command (str v_uds_name "= FOREACH " v_uds_name " GENERATE " (if-not (= "COGROUP" (get-in value_property [0 :dataType]))
                                                                       (str
                                                                         (pig-notNull "value" (get-in value_property [0 :dataType]))
                                                                         )
                                                                       (get-in value_property [0 :name])
                                                                       )
                     " as " (get-in value_property [0 :name])
                     "," (clojure.string/join "," key_property_list) (str ",timestamp as " (get-in value_timestamp_property [0 :name])) ";")]

    (alter-ref-vector3 node-properties-collection value_property)
    (if (append-incremental? p_command) (alter-ref-vector3 node-timestamp-properties-collection value_timestamp_property))

    (alter-ref-vector pig-command-collection key_full_destruct)
    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))
    (alter-ref-vector pig-command-collection command)
    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))
    ))

(defn node-pig-inc [Node_name
                    command_seq
                    ds_name
                    command_counter
                    ]

  (if (empty? command_seq)

    (do
      (case JOINING_METHOD
        "COGROUP" (do
                    (pig-node-cogroup-command (replace-nonpig-symbols Node_name) @node-cogroup-collection @node-cogroup-flatten-collection)
                    (alter-ref-vector3 pig-command-collection @pig-cad-node-join-commands)
                    (alter-ref-vector3 pig-command-collection @pig-branch-commands)
                    )
        ;"IJM" (do
        ;               (pig-node-join-command (replace-nonpig-symbols Node_name) node-join-collection)
        ;               (alter-ref-vector3 pig-command-collection @pig-cad-node-join-commands)
        ;               (alter-ref-vector3 pig-command-collection @pig-branch-commands)
        ;               )
        "JOIN" (do
                 (alter-ref-vector3 pig-command-collection @pig-cad-node-join-commands)
                 (alter-ref-vector3 pig-command-collection @pig-branch-commands)
                 )

        )

      ; (pig-join-foreach-final (replace-nonpig-symbols Node_name) (replace-nonpig-symbols Node_name))

      )
    (do
      (let [
            immutable_name (get-immutable-name (first command_seq))

            command_type (get-in (first command_seq) [:type])

            on_left (get-in (get-append-join-prop-list (first command_seq)) [:left])

            on_right (get-in (get-append-join-prop-list (first command_seq)) [:right])

            ds_name_1 (replace-nonpig-symbols ds_name)
            Node_name (replace-nonpig-symbols Node_name)
            _ (println "command")
            _ (println (first command_seq))

            ]


        (if (= command_type "join")


          (do

            (println "-------------------------------------------")

            (println (str "MAKE KEY for " immutable_name))
            (pig-filter-immutable-inc immutable_name nil)

            (if-not (empty? (second command_seq))
              (do
                (pig-join-select-inc (first command_seq) command_counter)


                (recur Node_name (rest command_seq) (str immutable_name command_counter) (+ command_counter 1))
                )
              (pig-join-select2-inc (first command_seq) Node_name command_counter)
              )
            )
          (if (= command_type "append")
            (do
              (println (str "APPEND for " immutable_name))
              (pig-filter-immutable-inc immutable_name nil)
              (pig-append-select-inc (first command_seq) command_counter)
              ;  (println "after append select-inc")
              (case JOINING_METHOD

                "JOIN" (pig-join ds_name_1
                                 ds_name_1
                                 (str "(" on_left ")")
                                 (replace-nonpig-symbols (str immutable_name command_counter))
                                 (str "(" on_right ")") command_counter)
                "COGROUP" (do



                            (if (= @joining_flag 0)
                              (do
                                (pig-cogroup-leftouter-first Node_name
                                                             ds_name_1
                                                             on_left
                                                             )
                                (pig-cogroup-leftouter-rest Node_name
                                                            (replace-nonpig-symbols (str immutable_name command_counter))
                                                            on_right)
                                (set-ref-vector joining_flag 1)
                                )

                              (do
                                (pig-cogroup-leftouter-rest Node_name
                                                            (replace-nonpig-symbols (str immutable_name command_counter))
                                                            on_right)
                                )
                              )

                            )

                ;"IJM"                         (if (= @joining_flag 0)
                ;                                (do
                ;
                ;                                  (alter-ref-vector3 node-join-collection [{:ds_name ds_name_1 :join_property on_left }] )
                ;                                  (alter-ref-vector3 node-join-collection [{:ds_name (replace-nonpig-symbols (str immutable_name command_counter)) :join_property on_right }] )
                ;                                  (set-ref-vector joining_flag 1)
                ;                                  )
                ;
                ;                                (alter-ref-vector3 node-join-collection [{:ds_name (replace-nonpig-symbols (str immutable_name command_counter)) :join_property on_right }] )
                ;                                )

                )

              ;code as definintion


              (if (contains? (first command_seq) :referenceCode)
                (do
                  (cad-join Node_name (first command_seq) command_counter)
                  )
                )

              (recur Node_name (rest command_seq) ds_name (+ command_counter 1))
              )
            (if (= command_type "branch")
              (do
                (println (str "BRANCH for " (get-in (first command_seq) [:node])))
                (parse-branche-on (first command_seq))
                (pig-cogroup (replace-nonpig-symbols Node_name)
                             (get-in (first command_seq) [:as])
                             (replace-nonpig-symbols Node_name)
                             (str "(" (clojure.string/join "," @right) ")")
                             (get-in (first command_seq) [:node])
                             (str "(" (clojure.string/join "," @left) ")")
                             )

                (recur Node_name (rest command_seq) ds_name command_counter)
                )
              ;if the command type unknown
              (recur Node_name (rest command_seq) ds_name (+ command_counter 1))
              )

            )

          )

        )
      )
    )
  )

(defn pig-parse-node-inc
  [p_node
   ]
  (let [NodeId (get-in p_node [:nodeId])
        command_seq (get-in p_node [:commands])
        ;v_ts INCREMENTAL_TS
        v_ts (get-cfg @user-config :starttimestamp)
        ]
    (set-ref-vector node-properties-collection [])
    ;(set-ref-vector node-join-collection [])
    (set-ref-vector node-timestamp-properties-collection [])
    (set-ref-vector node-cogroup-collection [])
    (set-ref-vector node-cogroup-flatten-collection [])
    (set-ref-vector pig-cad-node-join-commands [])
    (set-ref-vector pig-branch-commands [])
    (set-ref-vector joining_flag 0)
    ; (println "debug pig-parse-node-inc")
    (node-pig-inc NodeId command_seq "" 0)
    (pig-join-foreach-final-inc (replace-nonpig-symbols NodeId) (replace-nonpig-symbols NodeId) (select [ALL :name] @node-timestamp-properties-collection) v_ts)
    )
  )

(defn pig-parse-all-nodes-inc
  [node_name
   node_seq
   ]
  (if-not (empty? node_seq)
    (do
      ;  (println "debug pig-parse-all-nodes-inc")
      (pig-parse-node-inc (first node_seq))
      (recur (get-in (first node_seq) [:nodeId]) (rest node_seq))
      )
    node_name
    )
  )
