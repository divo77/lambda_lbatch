(ns workspace.core
  (:gen-class)
  (:use clojure.test
        [clojure.java.shell :only [sh]])
  (:require
    [clojure.string :as str]
    [clojure.java.io :as io]
    [clojure.string :as string]
    [clj-time.core :as t]
    [clj-time.format :as f]
    [clj-time.coerce :as c]
    [clojure.tools.cli :refer [parse-opts]]
    [clojure.edn :as edn]
    [me.raynes.fs :as rfs]
    [workspace.gl.vars :refer :all]
    [workspace.lib.sbv :refer :all]
    [workspace.sbv-core :refer :all]
    )
  )

(use 'com.rpl.specter)
(use 'com.rpl.specter.macros)

(defn get-inc-file-lock-status
  "docstring"
  [p_inc_file]
  (let [
        timestamp_file_data (get-in (nth (load-cfg-file p_inc_file) 1) [:status])
        ]
    timestamp_file_data
    )

  )

(defn get-pig-ret-code-desc
  "docstring"
  [p_ret_code]

  (get-in (select [ALL (selected? :code #(= p_ret_code %))] pig_ret_codes) [0 :desc])
  )

(defn run-pig-script
  [
   p_script_file_name
   p_logfile_name_fp

   ]
  (let [
        v_pig_return_data (case (clojure.string/upper-case (get-cfg @user-config :pig_mode))
                            "LOCAL" (sh "pig" "-x" "local " " -file " p_script_file_name "-logfile" p_logfile_name_fp "-stop_on_failure")
                            "MAPREDUCE" (sh "pig" "-file" p_script_file_name "-logfile" p_logfile_name_fp "-stop_on_failure")
                            "default"
                            )
        ]

    v_pig_return_data

    )
  )

(defn store-pig_script
  "docstring"
  [p_script_name]

  (dorun (map (fn [x]
                (pig-storeEasy p_script_name (println-str (get @pig-command-collection x)))
                )
              (vec (take (count @pig-command-collection) (range))))
         )

  )

(def custom-formatter (f/formatter "yyyyMMdd_HH_MM_SS"))

(defn generate-pig-script
  [ ]
  (let [

        v_log_folder (get-cfg @user-config  :log_folder)
        v_temp_folder (get-cfg @user-config  :temp_folder)
        v_pig_output_folder_hdfs (get-cfg @user-config  :pig_output_folder_hdfs)
        v_mapping_file_name (get-cfg @user-config  :mapping_file_name)
        v_schema_file_name (get-cfg @user-config   :schema_file_name)
        _ (reset! parsed-linkfile (file-loader v_mapping_file_name))


        ;Start and END timestamps
        v_incremental_mode (get-cfg @user-config :incremental_flag)
        ;_ (println v_incremental_mode)

        v_immutable_list (into (get-immutable-list)  ["reference.referencedisplay"] )

        _ (reset! parsed-schema (assoc-in {:schema []} [:schema]
                                          (into []
                                                (filter (fn [x] (in? v_immutable_list (get-in x [:name])   ))
                                                        (get-in
                                                          (file-loader v_schema_file_name)
                                                          [:schema])
                                                        )
                                                )
                                          ))

        _ (set-ref-vector pig-command-collection [])
        _ (pig-register-commands)
        _ (pig-define-commands)
        _ (pig-set-commands)
        ;_ (if v_incremental_mode (pig-load-inc) (pig-load))
        _ (pig-load)
        _ (pig-parse-node reference-node)

        ; _ (println "before parsing nodes")
        last_ds (if v_incremental_mode
                  (pig-parse-all-nodes-inc "" (get-in @parsed-linkfile [:nodes]))
                  (pig-parse-all-nodes "" (get-in @parsed-linkfile [:nodes]))
                  )

        start_time (f/unparse custom-formatter (t/now))
        v_pig_res_folder (str last_ds "_" start_time)
        v_pig_res_folder_hdfs_fp (str v_pig_output_folder_hdfs v_pig_res_folder)
        v_pig_script_name_fp (str  v_temp_folder "pig_" start_time ".pig")
        _ (pig-store-command last_ds v_pig_res_folder_hdfs_fp)
        v_logfile_name_fp (str v_log_folder "pig_" start_time ".log")

        ]
    {:logfile_name v_logfile_name_fp :pig_script_name v_pig_script_name_fp :pig_output_folder_hdfs v_pig_res_folder_hdfs_fp}
    )
  )

;(defn cmd-format-date
;  [date-str]
;  (f/unparse (f/formatter "dd/MM/yyyy:kk:mm:ss") (f/parse custom-formatter date-str)))

(def cmd_custom_formater (f/formatter "dd/MM/y:kk:mm:ss"))

(def cli-options
  ;; An option with a required argument

  [

   [nil "--config CONFIG_FILENAME" "full path to the config file. if specified then only the config file can be used for specifing parameters"
    :default nil
    :parse-fn #(str %)
    ]
   [nil "--startdate STARTTIMESTAMP" "Start time in format \"MM/DD/YYYY:HH24:MI:SS\" for incremental subvert"
    :default nil
    :parse-fn #(f/parse cmd_custom_formater %)
    ; :validate [#(= % (f/unparse cmd_custom_formater (f/parse cmd_custom_formater %))) "Must be a date in format \"MM/DD/YYYY:HH24:MI:SS\""]
    ]
   [nil "--starttimestamp STARTTIMESTAMP" "Start time in format \"MM/DD/YYYY:HH24:MI:SS\" for incremental subvert"
    :default nil
    :parse-fn #(Integer/parseInt %)
    ; :validate [#(< 0 % 0x10000) "Must be a number of seconds between 0 and 65536 (18 hours)"]
    ]
   [nil "--timeincrement TIMEINCREMENT" "Increemnt time for compose the end time of the change period.
                                         Example --timeincrement=30 means you end time will be strttime + timeincrement"
    :default nil
    :parse-fn #(Integer/parseInt %)
    ;:validate [#(< 0 % 0x10000) "Must be a number of seconds between 0 and 65536 (18 hours)"]
    ]

   [nil "--incremental INCREMENTAL" "if sperror.logecified then only the config file can be used for specifing parameters"
    :default nil
    :parse-fn #(str %)
    ;:validate (#(or (= % "true") (= % "false")) "Can be only true or false")
    ]
   ["-h" "--help"]]

  )

(defn -main
  [& args]
  (let [

        ;taking the  application config

        v_coda_home (System/getenv "SBV_PARSER_HOME")
        v_f_tmp (str v_coda_home DEFAULT_CONFIG)
        v_default_config_file (if (rfs/readable? v_f_tmp) v_f_tmp (exit 0 (str "ERROR:default config file" v_f_tmp " isn't readable") ) )
        ; _ (println v_default_config_file)
        v_default_config (load-cfg v_default_config_file )
        ; _ (println v_default_config)
        v_run_pig_app (get-cfg v_default_config   :run_pig)
        v_pig_mode_app (get-cfg v_default_config   :pig_mode)
        ; v_pig_script_debug_app (get-cfg v_default_config   :pig_script_debug)
        v_pig_script_debug_mode_app (get-cfg v_default_config   :pig_script_debug_mode)
        v_pig_script_params_app (get-cfg v_default_config   :pig_hadoop_params)
        v_log_folder_app (get-cfg v_default_config   :log_folder)
        v_temp_folder_app (get-cfg v_default_config   :temp_folder)
        v_data_folder_hdfs_app (get-cfg v_default_config   :data_folder_hdfs)
        v_pig_output_folder_hdfs_app (get-cfg v_default_config   :pig_output_folder_hdfs)
        v_pig_output_folder_app (get-cfg v_default_config  :pig_output_folder)
        v_mapping_file_name_app (get-cfg v_default_config  :mapping_file_name)
        v_schema_file_name_app (get-cfg v_default_config   :schema_file_name)
        v_pig_udf_register_app (get-cfg v_default_config  :pig_udf_register)
        v_pig_udf_define_app (get-cfg v_default_config  :pig_udf_define)
        v_pig_store_function_app (get-cfg v_default_config  :pig_store_function)
        ;parameters for incremental

        v_time_gap_app (get-cfg v_default_config :time_gap)
        v_time_increment_app (get-cfg v_default_config :time_increment)
        v_incremental_flag_app (get-cfg v_default_config :incremental_flag)
        v_incremental_timstamp_file_app (get-cfg v_default_config :incremental_timstamp_file)
        ;Final grouping on/off

        v_result_grouping_app (get-cfg v_default_config :result_grouping)

        ;taking command line parameters
        ; _ (println "test 1")
        {:keys [options arguments errors summary]} (parse-opts args cli-options)
        ; _ (println "test 2")
        ]

    (if (some? (:config options) )

      (let [
            v_f_tmp (get-in options [:config ] )

            v_user_config (if (rfs/readable? v_f_tmp) (load-cfg v_f_tmp) (exit 1 (str "ERROR:user config file " v_f_tmp "isn't readable")))

            v_run_pig_user (get-cfg v_user_config :run_pig)
            v_pig_mode_user (get-cfg v_user_config  :pig_mode)
            ;  v_pig_script_debug_user (get-cfg v_user_config  :pig_script_debug)
            v_pig_script_debug_mode_user (get-cfg v_user_config  :pig_script_debug_mode)
            v_pig_script_params_user (get-cfg v_user_config  :pig_hadoop_params)
            v_log_folder_user (get-cfg v_user_config  :log_folder)
            v_temp_folder_user (get-cfg v_user_config  :temp_folder)
            v_data_folder_hdfs_user (get-cfg v_user_config  :data_folder_hdfs)
            v_pig_output_folder_hdfs_user (get-cfg v_user_config  :pig_output_folder_hdfs)
            v_pig_output_folder_user (get-cfg v_user_config  :pig_output_folder)
            v_mapping_file_name_user (get-cfg v_user_config  :mapping_file_name)
            v_schema_file_name_user (get-cfg v_user_config  :schema_file_name)
            v_pig_udf_register_user (get-cfg v_user_config  :pig_udf_register)
            v_pig_udf_define_user (get-cfg v_user_config  :pig_udf_define)
            v_pig_store_function_user (get-cfg v_user_config  :pig_store_function)

            ;parameters for incremental

            v_time_gap_user (get-cfg v_user_config :time_gap)
            v_time_increment_user (get-cfg v_user_config :time_increment)
            v_incremental_flag_user (get-cfg v_user_config :incremental_flag)
            v_incremental_timstamp_file_user (get-cfg v_user_config :incremental_timstamp_file)

            ;Final grouping on/off

            v_result_grouping_user (get-cfg v_user_config :result_grouping)

            v_result_grouping_final (if (some? v_result_grouping_user) v_result_grouping_user
                                                                       (if (some? v_result_grouping_app) v_result_grouping_app
                                                                                                         (exit 1 "ERROR :result_grouping parameter hasn't been defined in the default or local config")))

            v_time_gap_final (if (some? v_time_gap_user) v_time_gap_user
                                                         (if (some? v_time_gap_app) v_time_gap_app
                                                                                    (exit 1 "ERROR :time_gap parameter hasn't been defined in the default or local config")))
            v_time_increment_final (if (some? v_time_increment_user) v_time_increment_user
                                                                     (if (some? v_time_increment_app) v_time_increment_app
                                                                                                      (exit 1 "ERROR :time_increment parameter hasn't been defined in the default or local config")))
            v_incremental_flag_final (if (some? v_incremental_flag_user) v_incremental_flag_user
                                                                         (if (some? v_incremental_flag_app) v_incremental_flag_app
                                                                                                            (exit 1 "ERROR :incremental_flag parameter hasn't been defined in the default or local config")))
            v_incremental_timstamp_file_final (if (some? v_incremental_timstamp_file_user) v_incremental_timstamp_file_user
                                                                                           (if (some? v_incremental_timstamp_file_app) v_incremental_timstamp_file_app
                                                                                                                                       (exit 1 "ERROR :incremental_timstamp_file parameter hasn't been defined in the default or local config")))

            v_run_pig_final (if (some? v_run_pig_user) v_run_pig_user
                                                       (if (some? v_run_pig_app) v_run_pig_app
                                                                                 (exit 1 "ERROR :run_pig parameter hasn't been defined in the default or local config") ) )
            v_pig_mode_final (if (some? v_pig_mode_user) v_pig_mode_user
                                                         (if (some? v_pig_mode_app) v_pig_mode_app
                                                                                    (exit 1 "ERROR :pig_mode parameter hasn't been defined in the default or local config") ) )


            v_pig_script_debug_mode_final (if (some? v_pig_script_debug_mode_user) v_pig_script_debug_mode_user
                                                                                   (if (some? v_pig_script_debug_mode_app) v_pig_script_debug_mode_app
                                                                                                                           (exit 1 "ERROR :pig_script_debug_mode parameter hasn't been defined in the default or local config") ) )
            v_pig_script_params_final (if (some? v_pig_script_params_user) v_pig_script_params_user
                                                                           (if (some? v_pig_script_params_app) v_pig_script_params_app
                                                                                                               (exit 1 "ERROR :pig_script_params parameter hasn't been defined in the default or local config") ) )

            v_log_folder_final (if (some? v_log_folder_user) v_log_folder_user
                                                             (if (some? v_log_folder_app) v_log_folder_app
                                                                                          (exit 1 "ERROR :log_folder parameter hasn't been defined in the default or local config") ) )

            v_temp_folder_final (if (some? v_temp_folder_user) v_temp_folder_user
                                                               (if (some? v_temp_folder_app) v_temp_folder_app
                                                                                             (exit 1 "ERROR :temp_folder parameter hasn't been defined in the default or local config") ) )
            v_data_folder_hdfs_final (if (some? v_data_folder_hdfs_user) v_data_folder_hdfs_user
                                                                         (if (some? v_data_folder_hdfs_app) v_data_folder_hdfs_app
                                                                                                            (exit 1 "ERROR :data_folder_hdfs parameter hasn't been defined in the default or local config") ) )

            v_pig_output_folder_hdfs_final (if (some? v_pig_output_folder_hdfs_user) v_pig_output_folder_hdfs_user
                                                                                     (if (some? v_pig_output_folder_hdfs_app) v_pig_output_folder_hdfs_app
                                                                                                                              (exit 1 "ERROR :pig_output_folder_hdfs parameter hasn't been defined in the default or local config") ) )

            v_pig_output_folder_final (if (some? v_pig_output_folder_user) v_pig_output_folder_user
                                                                           (if (some? v_pig_output_folder_app) v_pig_output_folder_app
                                                                                                               (exit 1 "ERROR :pig_output_folder parameter hasn't been defined in the default or local config") ) )

            v_mapping_file_name_final (if (some? v_mapping_file_name_user) v_mapping_file_name_user
                                                                           (if (some? v_mapping_file_name_app) v_mapping_file_name_app
                                                                                                               (exit 1 "ERROR :mapping_file_name parameter hasn't been defined in the default or local config") ) )

            v_schema_file_name_final (if (some? v_schema_file_name_user) v_schema_file_name_user
                                                                         (if (some? v_schema_file_name_app) v_schema_file_name_app
                                                                                                            (exit 1 "ERROR :schema_file_name parameter hasn't been defined in the default or local config") ) )
            v_pig_udf_register_final (if (some? v_pig_udf_register_user) v_pig_udf_register_user
                                                                         (if (some? v_pig_udf_register_app) v_pig_udf_register_app
                                                                                                            (println "WARNING :schema_file_name parameter hasn't been defined in the default or local config") ) )

            v_pig_udf_define_final (if (some? v_pig_udf_define_user) v_pig_udf_define_user
                                                                     (if (some? v_pig_udf_define_app) v_pig_udf_define_app
                                                                                                      (println "WARNING :schema_file_name parameter hasn't been defined in the default or local config") ) )


            v_pig_store_function_final (if (some? v_pig_store_function_user) v_pig_store_function_user
                                                                             (if (some? v_pig_store_function_app) v_pig_store_function_app
                                                                                                                  (println "WARNING :pig_store_function parameter hasn't been defined in the default or local config") ) )

            ]

        (do


          (reset! user-config {:run_pig                   v_run_pig_final
                               :pig_mode                  v_pig_mode_final
                               :pig_script_debug_mode     v_pig_script_debug_mode_final
                               :pig_script_params         v_pig_script_params_final
                               :log_folder                v_log_folder_final
                               :temp_folder               v_temp_folder_final
                               :data_folder_hdfs          v_data_folder_hdfs_final
                               :pig_output_folder_hdfs    v_pig_output_folder_hdfs_final
                               :pig_output_folder         v_pig_output_folder_final
                               :mapping_file_name         v_mapping_file_name_final
                               :schema_file_name          v_schema_file_name_final
                               :pig_udf_register          v_pig_udf_register_final
                               :pig_udf_define            v_pig_udf_define_final
                               :pig_store_function        v_pig_store_function_final
                               :time_gap                  v_time_gap_final
                               :time_increment            v_time_increment_final
                               :incremental_flag          v_incremental_flag_final
                               :incremental_timstamp_file v_incremental_timstamp_file_final
                               :starttimestamp            922337203685477580
                               :endtimestamp              922337203685477580
                               :result_grouping           v_result_grouping_final
                               }
                  )

          )
        )

      (let [
            v_result_grouping_final (if (some? v_result_grouping_app) v_result_grouping_app
                                                                      (exit 1 "ERROR :time_gap parameter hasn't been defined in the default or local config"))

            v_time_gap_final (if (some? v_time_gap_app) v_time_gap_app
                                                        (exit 1 "ERROR :time_gap parameter hasn't been defined in the default or local config"))

            v_time_increment_final (if (some? v_time_increment_app) v_time_increment_app
                                                                    (exit 1 "ERROR :time_increment parameter hasn't been defined in the default or local config"))

            v_incremental_flag_final (if (some? v_incremental_flag_app) v_incremental_flag_app
                                                                        (exit 1 "ERROR :incremental_flag parameter hasn't been defined in the default or local config"))

            v_incremental_timstamp_file_final (if (some? v_incremental_timstamp_file_app) v_incremental_timstamp_file_app
                                                                                          (exit 1 "ERROR :incremental_timstamp_file parameter hasn't been defined in the default or local config"))

            v_run_pig_final (if (some? v_run_pig_app) v_run_pig_app
                                                      (exit 1 "ERROR :run_pig parameter hasn't been defined in the default or local config") )
            v_pig_mode_final (if (some? v_pig_mode_app) v_pig_mode_app
                                                        (exit 1 "ERROR :pig_mode parameter hasn't been defined in the default or local config") )
            v_pig_script_debug_mode_final (if (some? v_pig_script_debug_mode_app) v_pig_script_debug_mode_app
                                                                                  (exit 1 "ERROR :pig_script_debug_mode parameter hasn't been defined in the default or local config") )

            v_pig_script_params_final (if (some? v_pig_script_params_app) v_pig_script_params_app
                                                                          (exit 1 "ERROR :pig_script_params parameter hasn't been defined in the default or local config") )

            v_log_folder_final  (if (some? v_log_folder_app) v_log_folder_app
                                                             (exit 1 "ERROR :log_folder parameter hasn't been defined in the default or local config") )
            v_temp_folder_final (if (some? v_temp_folder_app) v_temp_folder_app
                                                              (exit 1 "ERROR :temp_folder parameter hasn't been defined in the default or local config") )
            v_data_folder_hdfs_final  (if (some? v_data_folder_hdfs_app) v_data_folder_hdfs_app
                                                                         (exit 1 "ERROR :data_folder_hdfs parameter hasn't been defined in the default or local config") )

            v_pig_output_folder_hdfs_final (if (some? v_pig_output_folder_hdfs_app) v_pig_output_folder_hdfs_app
                                                                                    (exit 1 "ERROR :pig_output_folder_hdfs parameter hasn't been defined in the default or local config") )

            v_pig_output_folder_final  (if (some? v_pig_output_folder_app) v_pig_output_folder_app
                                                                           (exit 1 "ERROR :pig_output_folder parameter hasn't been defined in the default or local config") )

            v_mapping_file_name_final (if (some? v_mapping_file_name_app) v_mapping_file_name_app
                                                                          (exit 1 "ERROR :mapping_file_name parameter hasn't been defined in the default or local config") )

            v_schema_file_name_final (if (some? v_schema_file_name_app) v_schema_file_name_app
                                                                        (exit 1 "ERROR :schema_file_name parameter hasn't been defined in the default or local config") )
            v_pig_udf_register_final (if (some? v_pig_udf_register_app) v_pig_udf_register_app
                                                                        (println "WARNING :v_pig_udf_register_user parameter hasn't been defined in the default or local config") )
            v_pig_udf_define_final (if (some? v_pig_udf_define_app) v_pig_udf_define_app
                                                                    (println "WARNING :schema_file_name parameter hasn't been defined in the default or local config") )

            v_pig_store_function_final (if (some? v_pig_store_function_app) v_pig_store_function_app
                                                                            (println "WARNING :pig_store_function parameter hasn't been defined in the default or local config") )

            ]

        (do

          (reset! user-config {:run_pig                   v_run_pig_final
                               :pig_mode                  v_pig_mode_final
                               :pig_script_debug_mode     v_pig_script_debug_mode_final
                               :pig_script_params         v_pig_script_params_final
                               :log_folder                v_log_folder_final
                               :temp_folder               v_temp_folder_final
                               :data_folder_hdfs          v_data_folder_hdfs_final
                               :pig_output_folder_hdfs    v_pig_output_folder_hdfs_final
                               :pig_output_folder         v_pig_output_folder_final
                               :mapping_file_name         v_mapping_file_name_final
                               :schema_file_name          v_schema_file_name_final
                               :pig_udf_register          v_pig_udf_register_final
                               :pig_udf_define            v_pig_udf_define_final
                               :pig_store_function        v_pig_store_function_final

                               :time_gap                  v_time_gap_final
                               :time_increment            v_time_increment_final
                               :incremental_flag          v_incremental_flag_final
                               :incremental_timstamp_file v_incremental_timstamp_file_final
                               :starttimestamp            922337203685477580
                               :endtimestamp              922337203685477580
                               :result_grouping           v_result_grouping_final
                               })

          ;(println "test 4")

          )
        )
      )


    ;-----------------------------------------------------------------

    ;(println "test 5")

    (let
      [v_incremental (get-in options [:incremental])
       res (if-not (nil? v_incremental)
             (case v_incremental
               "true" (do
                        ;Override the config parameters and set up incremental_flag=true
                        ; (println "incrmental=true")
                        ; (println options)
                        (reset! user-config (assoc-in @user-config [:incremental_flag] true))

                        (let [v_starttime (get-in options [:starttime])
                              v_timeincrement (get-in options [:timeincrement])
                              v_st_timestamp (c/to-long v_starttime)
                              ]
                          (if-not (or
                                    (nil? v_starttime)
                                    (nil? v_timeincrement)
                                    )
                            (do
                              (println "correct timestamps")
                              (println (f/unparse cmd_custom_formater v_starttime))
                              (println v_st_timestamp)
                              (println (+ (c/to-long v_starttime) v_timeincrement))
                              (println (c/to-long v_starttime))
                              (reset! user-config (assoc-in @user-config [:endtimestamp] (+ v_st_timestamp v_timeincrement)))
                              (reset! user-config (assoc-in @user-config [:starttimestamp] v_st_timestamp))
                              (reset! user-config (assoc-in @user-config [:time_increment] v_timeincrement))
                              )


                            (do
                              (println "need both parameters")
                              (exit 1 "ERROR : Both :starttime and :timeincrement should be specified ")
                              )

                            )
                          )

                        )

               "false" (do
                         ;(println "incremental=false")
                         (reset! user-config (assoc-in @user-config [:incremental_flag] false))
                         )

               "wrong_inc_value")
             (do                                              ;no command line incremental parameter use config file

               (println "no CMD parameters for incremetal run use configuration parameters")

               (if (or
                     (nil? (get-cfg @user-config :time_gap))
                     (nil? (get-cfg @user-config :time_increment))
                     )
                 (exit 1 "ERROR :time_gap and :time_increment parameters mustn't be nil. Set up the range between 0 and N ")
                 )


               (let [
                     v_current_timestamp (quot (System/currentTimeMillis) 1000)
                     v_nf_start_timestamp (- v_current_timestamp (+ (get-cfg @user-config :time_gap) (get-cfg @user-config :time_increment)))
                     v_tmp_inc_file_name (get-cfg @user-config :incremental_timstamp_file)
                     v_timeincrement (get-in options [:timeincrement])

                     ]

                 (if-not (rfs/exists? v_tmp_inc_file_name)
                   (do                                        ;timestamp file doesn't exist

                     (println (str "ERROR:default incremental file " v_tmp_inc_file_name " doesn't exist"))
                     (println (str "Creating ...  " v_tmp_inc_file_name))

                     ; if the timestamp file doesn't exist then recreate it

                     (create-timestamp-file v_tmp_inc_file_name v_nf_start_timestamp "UNLOCK")
                     (reset! user-config (assoc-in @user-config [:starttimestamp] v_nf_start_timestamp))
                     (println v_nf_start_timestamp)
                     (println v_timeincrement)
                     (reset! user-config (assoc-in @user-config [:endtimestamp] (+ v_nf_start_timestamp v_timeincrement)))
                     )


                   (do                                        ;timestamp file exist

                     (let [timestamp_file_data (nth (load-cfg-file v_tmp_inc_file_name) 1)
                           _ (println timestamp_file_data)
                           v_inc_file_start_timestamp (get-in timestamp_file_data [:timestamp])
                           v_timeincrement (get-cfg @user-config :time_increment)
                           ]

                       (do
                         ; (println 1)
                         (println v_inc_file_start_timestamp)
                         (if (some? v_inc_file_start_timestamp)
                           (do
                             ;(println 2)
                             (println v_inc_file_start_timestamp)
                             (reset! user-config (assoc-in @user-config [:starttimestamp] v_inc_file_start_timestamp))
                             (reset! user-config (assoc-in @user-config [:endtimestamp] (+ v_inc_file_start_timestamp v_timeincrement)))

                             )
                           (do
                             ; if the file timestamp parameter  is empty recreate it
                             ;(println 3)
                             (create-timestamp-file v_tmp_inc_file_name v_nf_start_timestamp "UNLOCK")
                             (reset! user-config (assoc-in @user-config [:starttimestamp] v_nf_start_timestamp))
                             (reset! user-config (assoc-in @user-config [:endtimestamp] (+ v_nf_start_timestamp v_timeincrement)))

                             )
                           )

                         )

                       )
                     )

                   )
                 )

               )
             )

       ]

      (if (= res "wrong_inc_value") (exit 1 "ERROR : wrong parameter for --incremental .USe true or false"))

      )



    (do                                                       ;there is no CMD incremental parameter use the parameters configured in config file



      (let [
            v_run_pig (get-cfg @user-config :run_pig)
            v_script_info (generate-pig-script)
            v_pig_script_name_fp (get-in v_script_info [:pig_script_name])
            v_logfile_name_fp (get-in v_script_info [:logfile_name])
            v_pig_res_folder_hdfs_fp (get-in v_script_info [:pig_output_folder_hdfs])
            v_timestamp_file (get-cfg @user-config :incremental_timstamp_file)
            v_new_starttimestamp (+ (get-cfg @user-config :endtimestamp) 1)
            _ (if-not (empty? @pig-command-collection) (store-pig-script v_pig_script_name_fp) (exit 1 "ERROR: PIG SCRIPT IS EMPTY. EXIT:1"))
            ]

        (if (= v_run_pig true)

          (dorun

            (map (fn [x]
                   (if (= (get-inc-file-lock-status v_timestamp_file) "UNLOCK")
                     (

                       (println "pig_script_name = " v_pig_script_name_fp)
                       (println "PIG result folder: " (str "/mapr" v_pig_res_folder_hdfs_fp "/"))
                       (println "PIG log file folder: " v_logfile_name_fp)
                       (println "RUNNING PIG SCRIPT ....: ")

                       (let [
                             v_pig_ret_info (run-pig-script v_pig_script_name_fp v_logfile_name_fp)
                             v_pig_ret_code (get-in v_pig_ret_info [:exit])
                             ]


                         (if (= v_pig_ret_code 0)
                           (do
                             (println (str "RETURN CODE:" v_pig_ret_code ": " (get-pig-ret-code-desc v_pig_ret_code)))
                             (create-timestamp-file v_timestamp_file v_new_starttimestamp "UNLOCK")
                             (println "<<<<SUCCESS>>>>")
                             (exit 0 "EXIT:0")


                             )
                           (do
                             (println (str "<<<<ERROR : RETURN CODE:" v_pig_ret_code ": " (get-pig-ret-code-desc v_pig_ret_code)))
                             (exit 1 "EXIT:1")

                             )
                           )
                         )


                       )

                     (do
                       (println (str "inc file is LOCKED waiting for " WAIT_ON_LOCK_TIME))
                       (Thread/sleep (* WAIT_ON_LOCK_TIME 1000))
                       )

                     )
                   ) (take WAIT_ON_LOCK_RUN_ATTEMPTS (repeat 1)))
            )

          (do
            (println "pig_script_name = " v_pig_script_name_fp)
            (println "PIG result folder: " (str "/mapr" v_pig_res_folder_hdfs_fp "/"))
            (println "PIG log file folder: " v_logfile_name_fp)
            (println "FINISHED without PIG script run")
            {}
            )
          )

        )


      (exit 0 "EXIT:0")
      )


    )
  )



