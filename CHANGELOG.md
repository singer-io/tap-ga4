# Changelog

## v0.3.6
  * Update `google-analytics-data` library from `0.14.0` to `0.20.0` [#122](https://github.com/singer-io/tap-ga4/pull/122)

## v0.3.5
  * Update cached field exclusions to match changes made in the GA4 Data API [#117](https://github.com/singer-io/tap-ga4/pull/117)

## v0.3.4
  * Bump dependency versions for twistlock compliance [#113](https://github.com/singer-io/tap-ga4/pull/113)

## v0.3.3
  * Dependabot update [#112](https://github.com/singer-io/tap-ga4/pull/112)

## v0.3.2
  * Update cached field exclusions to match changes made in the GA4 Data API [#108](https://github.com/singer-io/tap-ga4/pull/108)

## v0.3.1
  * Update deprecated metrics in premade reports [#110](https://github.com/singer-io/tap-ga4/pull/110)

## v0.3.0
  * Make `report_definitions` config property optional [#109](https://github.com/singer-io/tap-ga4/pull/109)

## v0.2.0
  * Allow `report_definitions` config property to be input as a list or json-encoded string [#107](https://github.com/singer-io/tap-ga4/pull/107)

## v0.1.6
  * Fix `400 The dimensions and metrics are incompatible` error [#106](https://github.com/singer-io/tap-ga4/pull/106)
  * Update pylint exclusions [#105](https://github.com/singer-io/tap-ga4/pull/105)

## v0.1.5
  * Fix `400 Cannot have filter_partition dimension without specifying filter-partitions in the request` error [#104](https://github.com/singer-io/tap-ga4/pull/104)

## v0.1.4
  * Add `NUMBER` data type in the catalog for the `INTEGER` type metric field. [#102](https://github.com/singer-io/tap-ga4/pull/102)
## v0.1.3
  * Update cached field exclusions to match changes made in the GA4 Data API [#101](https://github.com/singer-io/tap-ga4/pull/101)

## v0.1.2
  * Integer dimensions are sometimes returned as the string (other) [#37](https://github.com/singer-io/tap-ga4/pull/37)
## v0.1.1
  * Update cached field exclusions to match changes made in the GA4 Data API [#100](https://github.com/singer-io/tap-ga4/pull/100)
## v0.1.0
  * Update libraries to run on python 3.11.7 [#99](https://github.com/singer-io/tap-ga4/pull/99)
## v0.0.32
  * Update cached field exclusions to match changes made in the GA4 Data API [#97](https://github.com/singer-io/tap-ga4/pull/97)
## v0.0.31
  * Update cached field exclusions to match changes made in the GA4 Data API [#92](https://github.com/singer-io/tap-ga4/pull/92)
## v0.0.30
  * Update cached field exclusions to match changes made in the GA4 Data API [#77](https://github.com/singer-io/tap-ga4/pull/77)
## v0.0.29
  * Update cached field exclusions to match changes made in the GA4 Data API [#75](https://github.com/singer-io/tap-ga4/pull/75)
## v0.0.28
  * Update metric fields for the ecommerce report [#67](https://github.com/singer-io/tap-ga4/pull/71)
## v0.0.27
  * Update cached field exclusions to match changes made in the GA4 Data API [#67](https://github.com/singer-io/tap-ga4/pull/67)
## v0.0.26
  * Update cached field exclusions to match changes made in the GA4 Data API [#65](https://github.com/singer-io/tap-ga4/pull/65)
## v0.0.25
  * Update cached field exclusions to match changes made in the GA4 Data API [#62](https://github.com/singer-io/tap-ga4/pull/62)
## v0.0.24
  * Update cached field exclusions to match changes made in the GA4 Data API [#58](https://github.com/singer-io/tap-ga4/pull/58)
## v0.0.23
  * Update structure of replication method metadata for testing [#54](https://github.com/singer-io/tap-ga4/pull/54)
## v0.0.22
  * Update cached field exclusions to match changes made in the GA4 Data API [#49](https://github.com/singer-io/tap-ga4/pull/49)
## v0.0.21
  * Update cached field exclusions to match changes made in the GA4 Data API [#48](https://github.com/singer-io/tap-ga4/pull/48)
## v0.0.20
  * Update cached field exclusions to match changes made in the GA4 Data API [#44](https://github.com/singer-io/tap-ga4/pull/44)
## v0.0.19
  * Update wait time after quota limit is reached to one hour [#42](https://github.com/singer-io/tap-ga4/pull/42)
## v0.0.18
  * Update cached field exclusions to match changes made in the GA4 Data API [#41](https://github.com/singer-io/tap-ga4/pull/41)
## v0.0.17
  * Update cached field exclusions to match changes made in the GA4 Data API [#39](https://github.com/singer-io/tap-ga4/pull/39)
## v0.0.16
  * Update cached field exclusions to match changes made in the GA4 Data API [#38](https://github.com/singer-io/tap-ga4/pull/38)
## v0.0.15
  * Adds two premade reports with filters, conversions_report and in_app_purchases [#31](https://github.com/singer-io/tap-ga4/pull/31)
## v0.0.14
  * Update cached field exclusions to match changes made in the GA4 Data API [#33](https://github.com/singer-io/tap-ga4/pull/33)
## v0.0.13
  * Mark metrics with non-ascii non-alphanumeric characters as unsupported [#28](https://github.com/singer-io/tap-ga4/pull/28)
## v0.0.12
  * Update premade report names to match GA4 Data API changes [#27](https://github.com/singer-io/tap-ga4/pull/27)
## v0.0.11
  * Update field names to match GA4 Data API changes [#26](https://github.com/singer-io/tap-ga4/pull/26)
## v0.0.10
  * Update datetime formats to account for canonicalization of datetime dimensions  [#22](https://github.com/singer-io/tap-ga4/pull/22)
## v0.0.9
  * Canonicalize response to match schema [#21](https://github.com/singer-io/tap-ga4/pull/21)
## v0.0.8
  * Fixes canonicalization for custom dimensions/metrics [#20](https://github.com/singer-io/tap-ga4/pull/20)
## v0.0.7
  * Fixes error reading field_exclusions.json [#18](https://github.com/singer-io/tap-ga4/pull/18)
## v0.0.6
  * Fixes error reading field_exclusions.json [#17](https://github.com/singer-io/tap-ga4/pull/17)
## v0.0.5
  * Adds pre-made reports [#14](https://github.com/singer-io/tap-ga4/pull/14)
  * Converts field names to lowecase and snake_case to match destination formatting [#16](https://github.com/singer-io/tap-ga4/pull/16)
## v0.0.4
  * Update cached field exclusions for acheivementId [#15](https://github.com/singer-io/tap-ga4/pull/15)
## v0.0.3
  * Add adjustable conversion window [#13](https://github.com/singer-io/tap-ga4/pull/13)
## v0.0.2
  * Cache standard dimensions and metrics [#12](https://github.com/singer-io/tap-ga4/pull/12)
## v0.0.1
  * Alpha release [#11](https://github.com/singer-io/tap-ga4/pull/11)
