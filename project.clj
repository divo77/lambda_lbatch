(defproject workspace "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha12"]
		 [org.clojure/data.json "0.2.6"]
		 [org.clojure/data.csv "0.1.3"]
                 [cheshire "5.6.1"]
                 [conf-er "1.0.1"]
                 [clj-time "0.12.0"]
                 [org.clojure/tools.cli "0.3.5"]
                 [com.rpl/specter "0.12.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [me.raynes/fs "1.4.6"]
                 ;  [environ "0.5.0"]
                 [instaparse "1.4.3"]
                 ;  [com.rpl/specter "0.13.2"]

                 ]
  :plugins [[lein-cljfmt "0.5.3"]
            ;[jonase/eastwood "0.2.1"]
            [lein-kibit "0.1.2"]]
  :main  workspace.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
  :dev {:dependencies [[org.apache.pig/pig "0.13.0"]
                                                                    [org.apache.hadoop/hadoop-core "1.1.2"]]}
})
