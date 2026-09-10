(ns resolve-tick-test
  (:require [cljs.test :refer [deftest is testing run-tests]]
            [resolver.core :as core]))

(deftest parse-tranco-csv-test
  (testing "rank,domain rows, no header"
    (is (= [{:rank 1 :domain "google.com"} {:rank 2 :domain "youtube.com"}]
           (core/parse-tranco-csv "1,google.com\n2,youtube.com\n"))))
  (testing "blank lines are skipped, not counted as rows"
    (is (= 1 (count (core/parse-tranco-csv "1,example.com\n\n\n")))))
  (testing "domain is lower-cased"
    (is (= "example.com" (:domain (first (core/parse-tranco-csv "1,EXAMPLE.com\n"))))))
  (testing "0 input rows -> 0 output rows, not an error (caller decides what 0 means)"
    (is (= [] (core/parse-tranco-csv "")))))

(deftest cc-vertices-test
  (testing "TLD-first reversed notation is undone back to normal domain order"
    (is (= "www.wikipedia.org" (core/cc-reversed->domain "org.wikipedia.www")))
    (is (= "example.com" (core/cc-reversed->domain "com.example"))))
  (testing "one well-formed line -> {:domain}"
    (is (= {:domain "aaa.aaa"} (core/parse-cc-vertices-line "4\taaa.aaa\t5"))))
  (testing "malformed lines (wrong field count) are skipped, not thrown"
    (is (nil? (core/parse-cc-vertices-line "not-enough-fields")))
    (is (nil? (core/parse-cc-vertices-line "a\tb\tc\td")))))

(deftest resolution-rows-test
  (let [ctx {:source "tranco-top-1m" :source-date "2026-08-28" :tick-id "tick-1"
             :resolved-at "2026-08-28T00:00:00.000Z"}]
    (testing "one row per (type, answer) pair"
      (let [rows (core/resolution-rows ctx
                   {:domain "example.com" :rank 1
                    :results [{:type "A" :answer [{:data "93.184.216.34" :TTL 300}]}
                              {:type "NS" :answer [{:data "a.iana-servers.net."}
                                                    {:data "b.iana-servers.net."}]}]})]
        (is (= 3 (count rows)))
        (is (every? #(= "example.com" (:resolution/domain %)) rows))
        (testing "trailing dot on FQDN rdata is stripped"
          (is (some #(= "a.iana-servers.net" (:resolution/value %)) rows)))
        (testing "TTL carried when present"
          (is (some #(= 300 (:resolution/ttl %)) rows)))
        (testing "source/source-date come from ctx, not hardcoded"
          (is (every? #(= "tranco-top-1m" (:resolution/source %)) rows))
          (is (every? #(= "2026-08-28" (:resolution/source-date %)) rows)))))
    (testing "zero answers across all types -> exactly one NONE row, not zero rows"
      (let [rows (core/resolution-rows ctx
                   {:domain "nxdomain.example" :rank 999
                    :results [{:type "A" :answer []} {:type "AAAA" :answer []}
                              {:type "MX" :answer []} {:type "NS" :answer []}]})]
        (is (= 1 (count rows)))
        (is (= "NONE" (:resolution/record-type (first rows))))))
    (testing "row ids are stable/deterministic for the same (domain,type,value)"
      (let [mk #(core/resolution-rows ctx {:domain "example.com" :rank 1
                                           :results [{:type "A" :answer [{:data "1.2.3.4"}]}]})]
        (is (= (map :resolution/id (mk)) (map :resolution/id (mk))))))
    (testing "blank rdata values are dropped (an empty string is not evidence)"
      (let [rows (core/resolution-rows ctx
                   {:domain "example.com" :rank 1
                    :results [{:type "A" :answer [{:data ""} {:data "1.2.3.4"}]}]})]
        (is (= 1 (count rows)))))))

(run-tests)
