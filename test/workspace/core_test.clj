(ns workspace.core-test
  (:require [clojure.test :refer :all]
            [workspace.core :refer :all]))

;; test if file-loader returns anything
(deftest load-test
  (testing "sample link file load operation successful - checks that file-loader function executes"
    (file-loader "branchLink.json")))

;; tests if file-loader function returns an content of the branchLink.json reformated for PIG  
(deftest correctLoad-test
  (testing "sample link file loaded correctly"
    (is (= [{"parentCName" "memGroupHoldTree", "childCName" "holdCodeTree", "cJsonTageName" "HoldCode", "parentIdName" "holdCode"}]
           (file-loader "branchLink.json")))))

;; test the same as previous example for file "compositeLink.json"

(deftest compositeLoad-test
  (testing "sample link file with composite key loads correctly"
    (is (= [{"parentCName" "providerLocationAffiliationTree", "childCName" "providerLocationPhoneTree", "cJsonTageName" "Phones", "parentIdName" ["providerLocationAffiliationID" "providerID"], "isComposite" true}]
           (file-loader "compositeLink.json")))))
;; 
;; test if function true? returns true (finds the flat=true in json) for "branchLink.json"
;;

(deftest branch-test
  (testing "detect flat operator as branch (Note: 0 parameter refers to first line of link file)"
    (is (= "no" (flat? 0 "branchLink.json")))))
;; 
;;test if function true? returns true (finds the flat=true in json) for "joinLink.json"
;;

(deftest join-test
  (testing "detect flat operator as join (Note: 0 parameter refers to first line of link file)"
    (is (= true (flat? 0 "joinLink.json")))))

;; test if function config-easy-pig returns correct PIG command for "branchLink.json"
;;
(deftest wroteBranch-test
  (testing "clojure writes correct branch operation"
    (is (= (config-easy-pig (first (file-loader "branchLink.json")) 0 "branchLink.json") "HoldCode = FOREACH holdCode GENERATE *;\n\nmemGroupHold = COGROUP memGroupHold by holdCode, HoldCode by holdCode;\n\nmemGroupHold = FOREACH memGroupHold GENERATE FLATTEN(memGroupHold), HoldCode;\n\n"))))

;; test if function config-easy-pig returns correct PIG command for "joinLink.json"
;;
(deftest wroteJoin-test
  (testing "clojure writes correct join operation"
    (is (= (config-easy-pig (first (file-loader "joinLink.json")) 0 "joinLink.json")
           "ProviderOrgLocations = FOREACH providerOrgLocation GENERATE *;\n\nproviderLocationAffiliation = JOIN providerLocationAffiliation by providerOrgLocationID, ProviderOrgLocations by providerOrgLocationID;\n\n"))))
;;
;; test if function config-easy-pig returns correct PIG command for "compositeBranchLink.json"
;;
(deftest wroteCompositeBranch-test
  (testing "clojure writes correct composite branch operation"
    (is (= (config-easy-pig (first (file-loader "compositeBranchLink.json")) 0 "compositeBranchLink.json")
           "Phones = FOREACH providerLocationPhone GENERATE *;\n\nproviderLocationAffiliation = COGROUP providerLocationAffiliation by (providerLocationAffiliationID, providerID), Phones by (providerLocationAffiliationID, providerID);\n\nproviderLocationAffiliation = FOREACH providerLocationAffiliation GENERATE FLATTEN(providerLocationAffiliation), Phones;\n\n"))))
;;
;; if config-link-looper function executes it means files created ?????
;;
(deftest wroteFile-test
  (testing "clojure writes a file of pig code - checks that config-link-looper function executes"
    (config-link-looper "branchLink.json" "testSubvert.pig" "providerSchema.txt")))
;;
;; if no tag name in the "noTagLink.json" then nil. test passes for the link file without the tag name
;;
(deftest tagNil-test
  (testing "checks get-tag returns nil if there is no tag name in link file"
    (is (= nil (get-tag (file-loader "noTagLink.json"))))))
;;
;; The same as previous test but for "joinLink.json"
;;
(deftest tagExists-test
  (testing "checks a link file for the existence of a tag name parameter on a branch relation, expects no nil values"
    (is (not-any? nil? (map (fn [x]
                              (get-tag x))
                            (file-loader "joinLink.json"))))))

;;
;; ???????????????????? if you testing it and it works where are the functions ?
;;

(deftest checkTagPos-test
  (testing "checks a join link file for the tag parameter, expects no tag
            THIS FUNCTION IS NOT CURRENTLY SUPPORTED IN MAIN SCRIPT"
    (is (= true (checkTag "noTagJoinLink.json")))))
;;
;;
(deftest checkTagNeg-test
  (testing "checks a join link file for the tag parameter, expects a tag
            THIS FUNCTION IS NOT CURRENTLY SUPPORTED IN MAIN SCRIPT"
    (is (= false (checkTag "tagJoinLink.json")))))


(comment --DEPRECATED?

         (deftest checkTagPos-test
           (testing "checks a join link file for the tag parameter, expects no tag - expected to pass
            THIS FUNCTION IS NOT CURRENTLY SUPPORTED IN MAIN SCRIPT"
             (not-any? false?
                       (map (fn [x y]
                              (cond
                                (and (= true (flat? x "tagJoinLink.json")) (not= nil (get-tag y))) false))
                            (gen-number-array "tagJoinLink.json")
                            (file-loader "tagJoinLink.json")))))

         (deftest checkTagNeg-test
           (testing "checks a join link file for the tag parameter, expects no tag - expected to fail
            THIS FUNCTION IS NOT CURRENTLY SUPPORTED IN MAIN SCRIPT"
             (not-any? false?
                       (map (fn [x y]
                              (cond
                                (and (= true (flat? x "noTagJoinLink.json")) (not= nil (get-tag y))) false))
                            (gen-number-array "noTagJoinLink.json")
                            (file-loader "noTagJoinLink.json")))))
         )


(deftest isCompValidPos-test
  (testing "checks a link file  for the existence of two keys when isCompsite = true - expected to pass
            THIS FUNCTION IS NOT CURRENTLY SUPPORTED IN MAIN SCRIPT"
    (is (not-any? nil?
                  (map (fn [x]
                         (cond
                           (and (= 2 (count (flatten (get-link x)))) (isComposite? x)) "valid"))
                       (file-loader "compositeLink.json"))))))


(comment
  (deftest isCompValidNeg-test
    (testing "checks a link file  for the existence of two keys when isCompsite = true - expected to fail
            THIS FUNCTION IS NOT CURRENTLY SUPPORTED IN MAIN SCRIPT"
      (is (not-any? nil?
                    (map (fn [x]
                           (cond
                             (and (= 2 (count (flatten (get-link x)))) (isComposite? x)) "valid"))
                         (file-loader "compositeFailLink.json"))))))

  (deftest keyMismatch-test
    (testing "checks that the parentID column exists in both parent and child - CURRENTLY NOT SUPPORTED, expected to fail"
      (is (= (config-easy-pig (first (file-loader "mismatchLink.json")) 0 "mismatchLink.json")
             "ERROR: this parentID does not exist in both parent and child tables"))))

  (deftest deepLink-test
    (testing "ability to execute a deep link - CURRENTLY NOT SUPPORTED, expected to fail"
      (is (= (deep-link "countries.csv" "departments.csv")
             "countries = COGROUP countries by region_id, regions by region_id;
              countries = FOREACH countries GENERATE FLATTEN(countries), regions;

        locations = COGROUP locations by country_id, countries by country_id;
        locations = FOREACH locations GENERATE FLATTEN(locations), countries;

        departments = COGROUP departments by location_id, locations by location_id;
        departments = FOREACH departments GENERATE FLATTEN(departments), locations;"))))


  (deftest referenceCode-test
    (testing "ability to insert reference code values into a subvertical -> CURRENTLY NOT SUPPORTED, expected to fail"
      (is (= (referenceCode "refOut.json" "refCode.json") {"aarActionLengthType" "Indefinite"}))))

  (deftest refCodeExclusion-test
    (testing "ability to exclude a column from the reference code table -> CURRENTLY NOT SUPPORTED, expected to fail"
      (is (= (referenceCode "refExclOut.json" "refCode.json") {"aarActionLengthType" "X"}))))



  (deftest columnExclusion-test
    (testing "ability to exclude a column from a table -> CURRENTLY NOT SUPPORTED, expected to fail"
      (is (= (config-easy-pig (first (file-loader "excludeLink.json")) 0 "excludeLink.json")
             "countries = FOREACH countries GENERATE country_id, region_id;
        countries = COGROUP countries by region_id, regions by region_id;
        countries = FOREACH countries GENERATE FLATTEN(countries), regions;"))))

  )                                                         ;;end comment












