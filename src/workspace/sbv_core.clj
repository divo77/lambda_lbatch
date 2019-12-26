(ns workspace.sbv-core
  (:gen-class)
  (:use clojure.test)
  (:require
    [cheshire.core :refer :all]
    ;[clojure.string :as str]
    ;[clojure.java.io :as io]
    ;[clojure.string :as string]
    [clojure.java.shell :only [sh]]
    [clj-time.core :as t]
    [clj-time.format :as f]
    ;  [clojure.spec :as spec]
    [clojure.tools.cli :refer [parse-opts]]
    [clojure.edn :as edn]
    ;[clojure.tools.logging :as log]
    [me.raynes.fs :as rfs]
    [workspace.gl.vars :refer :all]
    [workspace.lib.sbv :refer :all]
    )
  )
;(use '[conf-er])
(use '[clojure.java.shell :only [sh]])
(use 'com.rpl.specter)
(use 'com.rpl.specter.macros)


(defn pig-join-foreach-final
  [p_nodeId
   p_ds_name
   ]
  (let [

        v_property_list (clojure.string/join "," (map
                                                   (fn [x]
                                                     (get-in x [:name])
                                                     )
                                                   @node-properties-collection
                                                   ))
        ;  _ (println v_property_list)
        command (str p_nodeId "= FOREACH " p_ds_name " GENERATE " v_property_list ";")]
    (alter-ref-vector pig-command-collection command)
    ))


(defn pig-join-select
  [p_command
   p_command_counter]
  (let [
        v_immutable_name (replace-nonpig-symbols (get-immutable-name p_command))
        v_uds_name (str v_immutable_name p_command_counter)
        key_full_destruct (str v_uds_name "= FOREACH " v_immutable_name " " (get-joinkey-destruct-clause p_command))
        ;select_property_list (get-join-prop-list p_command)
        select_property_list (into []
                                   (map
                                     (fn [x] (str (if-not (= "COGROUP" (get-in x [:dataType]))
                                                    (str
                                                      (pig-notNull (replace-nonpig-symbols (get-in x [:name])) (get-in x [:dataType]))
                                                      )
                                                    (get-in x [:name])
                                                    )

                                                  " as " (get-in x [:as])))
                                     (get-join-prop-list p_command))
                                   )



        command (str v_uds_name "= FOREACH " v_uds_name " GENERATE " (clojure.string/join "," select_property_list) ";")
        ]

    (alter-ref-vector3 node-properties-collection (get-join-prop-list3 p_command))

    (alter-ref-vector pig-command-collection key_full_destruct)

    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))

    (alter-ref-vector pig-command-collection command)

    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))
    ))


(defn pig-join-select-mj
  [p_command
   p_command_counter]
  (let [
        v_immutable_name (replace-nonpig-symbols (get-immutable-name p_command))
        v_uds_name (str v_immutable_name p_command_counter)
        key_full_destruct (str v_uds_name "= FOREACH " v_immutable_name " " (get-joinkey-destruct-clause p_command))
        ;select_property_list (get-join-prop-list p_command)
        select_property_list (into []
                                   (map
                                     (fn [x] (str (if-not (= "COGROUP" (get-in x [:dataType]))
                                                    (str
                                                      (pig-notNull (replace-nonpig-symbols (get-in x [:name])) (get-in x [:dataType]))
                                                      )
                                                    (get-in x [:name])
                                                    )

                                                  " as " (get-in x [:as])))
                                     (get-join-prop-list p_command))
                                   )

        command (str v_uds_name "= FOREACH " v_uds_name " GENERATE " (clojure.string/join "," select_property_list) ";")
        ]

    (alter-ref-vector3 node-properties-collection (get-join-prop-list3 p_command))

    (alter-ref-vector pig-command-collection key_full_destruct)

    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))

    (alter-ref-vector pig-command-collection command)

    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))
    ))


(defn pig-join-select2
  [p_command
   p_node_name
   p_command_counter]
  (let [
        v_immutable_name (replace-nonpig-symbols (get-immutable-name p_command))
        v_uds_name (str v_immutable_name p_command_counter)
        key_full_destruct (str v_uds_name "= FOREACH " v_immutable_name " " (get-joinkey-destruct-clause p_command))
        ;select_property_list (get-join-prop-list p_command)

        select_property_list (into []
                                   (map
                                     (fn [x] (str (if-not (= "COGROUP" (get-in x [:dataType]))
                                                    (str
                                                      (pig-notNull (get-in x [:name]) (get-in x [:dataType]))
                                                      )
                                                    (get-in x [:name])
                                                    )

                                                  " as " (get-in x [:as])))
                                     (get-join-prop-list p_command))
                                   )

        command (str p_node_name "= FOREACH " v_uds_name " GENERATE " (clojure.string/join "," select_property_list) ";")
        ]

    (alter-ref-vector3 node-properties-collection (get-join-prop-list3 p_command))
    (alter-ref-vector pig-command-collection key_full_destruct)

    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))

    (alter-ref-vector pig-command-collection command)

    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))
    ))

(defn pig-append-select
  [p_command
   p_command_counter
   ]
  (let [
        ;v_uds_name (replace-nonpig-symbols (get-immutable-name p_command))
        v_immutable_name (replace-nonpig-symbols (get-immutable-name p_command))
        v_uds_name (str v_immutable_name p_command_counter)
        key_full_destruct (str v_uds_name "= FOREACH " v_immutable_name " " (get-joinkey-destruct-clause p_command))
        ;key_property_list (get-append-prop-list p_command)
        ;key_property_list (into []
        ;              (map
        ;              (fn [x] (str "(" (get-in x [:dataType]) ")" (get-in x [:name] ) " as " (get-in x [:as])) )
        ;              (get-join-prop-list p_command))
        ;                  )

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
        ;value_property_name (get-in p_command [:as])
        value_property (get-value-property p_command)
        ; key_name      (get-in p_command [:on ])
        command (str v_uds_name "= FOREACH " v_uds_name " GENERATE " (if-not (= "COGROUP" (get-in value_property [0 :dataType]))
                                                                       (str
                                                                         (pig-notNull "value" (get-in value_property [0 :dataType]))
                                                                         )
                                                                       (get-in value_property [0 :name])
                                                                       )
                     " as " (get-in value_property [0 :name])
                     "," (clojure.string/join "," key_property_list) ";")]

    (alter-ref-vector3 node-properties-collection value_property)
    (alter-ref-vector pig-command-collection key_full_destruct)
    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))
    (alter-ref-vector pig-command-collection command)
    ;(if (= (config :pig_script_debug) true)  (if (= (config :pig_script_debug_mode)  "DEBUG2") (debug-store-command v_uds_name) ))
    ))

(defn node-pig [Node_name
                command_seq
                ds_name
                command_counter
                ]

  (if (empty? command_seq)

    (do
      (pig-node-cogroup-command (replace-nonpig-symbols Node_name) @node-cogroup-collection @node-cogroup-flatten-collection)
      (alter-ref-vector3 pig-command-collection @pig-cad-node-join-commands)
      (alter-ref-vector3 pig-command-collection @pig-branch-commands)

      )
    (do
      (let [
            immutable_name (get-immutable-name (first command_seq))
            ;immutable_name (str (get-immutable-name (first command_seq)) (alter-debug-command-counter pig-debug-command-counter))

            command_type (get-in (first command_seq) [:type])

            on_left (get-in (get-append-join-prop-list (first command_seq)) [:left])

            on_right (get-in (get-append-join-prop-list (first command_seq)) [:right])

            ds_name_1 (replace-nonpig-symbols ds_name)
            Node_name (replace-nonpig-symbols Node_name)

            ]


        (if (= command_type "join")
          ;case command_type
          ;"join"

          (do

            (println "-------------------------------------------")

            (println (str "MAKE KEY for " immutable_name))
            (pig-filter-immutable immutable_name nil)
            (if-not (empty? (second command_seq))
              (do
                (pig-join-select (first command_seq) command_counter)


                (recur Node_name (rest command_seq) (str immutable_name command_counter) (+ command_counter 1))
                )
              (pig-join-select2 (first command_seq) Node_name command_counter)
              )
            )
          (if (= command_type "append")
            (do
              (println (str "APPEND for " immutable_name))
              (pig-filter-immutable immutable_name nil)
              (pig-append-select (first command_seq) command_counter)
              (if (= (get-cfg @user-config :pig_script_debug_mode) "DEBUG3")

                (pig-join ds_name_1
                          ds_name_1
                          (str "(" on_left ")")
                          (replace-nonpig-symbols (str immutable_name command_counter))
                          (str "(" on_right ")") command_counter)

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

                  (pig-cogroup-leftouter-rest Node_name
                                              (replace-nonpig-symbols (str immutable_name command_counter))
                                              on_right)
                  )
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

(defn node-pig-mj-append [Node_name
                          command_seq
                          ds_name
                          command_counter
                          ]

  (if (empty? command_seq)

    (do
      (pig-node-cogroup-command (replace-nonpig-symbols Node_name) @node-cogroup-collection @node-cogroup-flatten-collection)
      (alter-ref-vector3 pig-command-collection @pig-cad-node-join-commands)
      (alter-ref-vector3 pig-command-collection @pig-branch-commands)

      )
    (do
      (let [
            immutable_name (get-immutable-name (first command_seq))
            ;immutable_name (str (get-immutable-name (first command_seq)) (alter-debug-command-counter pig-debug-command-counter))

            command_type (get-in (first command_seq) [:type])

            on_left (get-in (get-append-join-prop-list (first command_seq)) [:left])

            on_right (get-in (get-append-join-prop-list (first command_seq)) [:right])

            ds_name_1 (replace-nonpig-symbols ds_name)
            Node_name (replace-nonpig-symbols Node_name)

            ]

        (if (= command_type "append")
          (do
            (println (str "APPEND for " immutable_name))
            (pig-filter-immutable immutable_name nil)
            (pig-append-select (first command_seq) command_counter)
            (if (= (get-cfg @user-config :pig_script_debug_mode) "DEBUG3")

              (pig-join ds_name_1
                        ds_name_1
                        (str "(" on_left ")")
                        (replace-nonpig-symbols (str immutable_name command_counter))
                        (str "(" on_right ")") command_counter)

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

                (pig-cogroup-leftouter-rest Node_name
                                            (replace-nonpig-symbols (str immutable_name command_counter))
                                            on_right)
                )
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

(defn node-pig-build-subbasckets [Node_name
                                  command_seq
                                  ds_name
                                  command_counter
                                  ]

  (if (empty? command_seq)

    (do
      ;(pig-node-cogroup-command (replace-nonpig-symbols Node_name) @node-cogroup-collection @node-cogroup-flatten-collection)
      ;(alter-ref-vector3 pig-command-collection @pig-cad-node-join-commands)
      ;(alter-ref-vector3 pig-command-collection @pig-branch-commands)

      )
    (do
      (let [
            immutable_name (get-immutable-name (first command_seq))
            ;immutable_name (str (get-immutable-name (first command_seq)) (alter-debug-command-counter pig-debug-command-counter))

            command_type (get-in (first command_seq) [:type])

            ds_name_1 (replace-nonpig-symbols ds_name)
            Node_name (replace-nonpig-symbols Node_name)

            ]


        (if (= command_type "join")
          ;case command_type
          ;"join"

          (do

            (println "-------------------------------------------")

            (println (str "MAKE KEY for " immutable_name))
            (pig-filter-immutable immutable_name nil)
            (if-not (empty? (second command_seq))
              (do
                (pig-join-select-mj (first command_seq) command_counter)


                (recur Node_name (rest command_seq) (str immutable_name command_counter) (+ command_counter 1))
                )
              (pig-join-select-mj (first command_seq) Node_name command_counter)
              )
            )
          )

        )
      )
    )
  )

(defn pig-parse-node
  [p_node
   ]
  (let [NodeId (get-in p_node [:nodeId])
        command_seq (get-in p_node [:commands])
        v_ts INCREMENTAL_TS
        ]
    (set-ref-vector node-properties-collection [])
    (set-ref-vector node-timestamp-properties-collection [])
    (set-ref-vector node-cogroup-collection [])
    (set-ref-vector node-cogroup-flatten-collection [])
    (set-ref-vector pig-cad-node-join-commands [])
    (set-ref-vector pig-branch-commands [])
    (set-ref-vector joining_flag 0)
    (node-pig NodeId command_seq "" 0)
    (pig-join-foreach-final (replace-nonpig-symbols NodeId) (replace-nonpig-symbols NodeId))

    )
  )

(defn pig-parse-node-mj
  [p_node
   ]
  (let [NodeId (get-in p_node [:nodeId])
        command_seq (get-in p_node [:commands])
        v_ts INCREMENTAL_TS
        ]
    (set-ref-vector node-properties-collection [])
    (set-ref-vector node-timestamp-properties-collection [])
    (set-ref-vector node-cogroup-collection [])
    (set-ref-vector node-cogroup-flatten-collection [])
    (set-ref-vector pig-cad-node-join-commands [])
    (set-ref-vector pig-branch-commands [])
    (set-ref-vector joining_flag 0)
    (node-pig-build-subbasckets NodeId command_seq "" 0)
    (node-pig NodeId command_seq "" 0)
    (pig-join-foreach-final (replace-nonpig-symbols NodeId) (replace-nonpig-symbols NodeId))

    )
  )

(defn pig-parse-all-nodes
  [node_name
   node_seq
   ]
  (if-not (empty? node_seq)
    (do
      (pig-parse-node (first node_seq))
      (recur (get-in (first node_seq) [:nodeId]) (rest node_seq))
      )
    node_name
    )
  )