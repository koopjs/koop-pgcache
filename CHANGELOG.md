# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

## Unreleased
### Added
* `pgCache.createExportStream` returns a stream of features either as strings or JSON directly from the DB

### Fixed
* Fix geoservices parsing when there is no where clause

## [1.5.1] - 2012-12-04
### Fixed
* Test ensures success of idFilter

## [1.5.0] - 2015-12-03
### Added
* New method `addIndexes` allows indexes to be added at any time
* Index creation at table creation time can be disable

### Changed
* Methods for querying, geoservices parsing, table creation/insertion and indexing moved to seperate mdules
* Inserts used more efficient multi-row method
* All SQL statements are logged at debug level

## [1.4.3] - 2015-11-30
### Added
* Log full error object in case of query failure

## [1.4.2] - 2015-11-11
### Fixed
* Remove two potential causes of unhandled exceptions in `select`

## [1.4.1] - 2015-11-02
### Fixed
* Don't call back with 'Not Found' in the error field when a specific query is empty

## [1.4.0] - 2015-10-12
### Fixed
* Aborted transactions are ended
* Cleaned up some broken docs

### Changed
* Removed third parameter from `_insertFeature` as it was not being used for the id
* Info passed in with geojson is stored as a flat object
* Indices set for each field can be turned off at insert time

### Deprecated
* Info passed in with geojson is no longer stored in the info doc as info.info

### Removed
* deleted outdated docs folder (should only be committed to gh-pages)

## [1.3.1] - 2015-09-15
### Fixed
* Insert WKT with proper quote escaping

## [1.3.0] - 2015-09-15
### Added
* New methods `getWKT` & `insertWKT` for storing and retrieving well known text strings of spatial reference systems

## [1.2.0] - 2015-09-10
### Added
* order_by option accepts array of {field: order} e.g. {'total_precip': 'ASC'}

### Changed
* Stats are requested as float
* Request data from the DB as JSON instead of strings
* Remove size limitation from `host` varchar table column ([#26](https://github.com/koopjs/koop-pgcache/pull/26))

### Fixed
* Intermittent off-by-one errors in tests are no more

## [1.1.0] - 2014-09-20
### Added
* name, type, and version are now included in exports

### Changed
* Errors are actual error objects
* Upgrade to Standard 5

### Fixed
* Style rewrites for standard bump

## [1.0.1] - 2015-06-15
### Fixed
* Geometry objects passed in as strings were not correctly being parsed in the parseGeometry method.

### Added
* better test coverage for parseGeometry

## [1.0.0] - 2015-06-15
### Added
* Created getExtent method that returns the extent of all features in the DB.

## [0.2.2] - 2015-06-14
### Fixed
* BBOX geometries from tile requests were not being handled correctly, fixed else statement by defining bbox as the input geometry.

## [0.2.1] - 2015-06-12
### Fixed
* Broken sql due to missing semicolons

### Added
* Continuous integration support via https://travis-ci.org
* Function-level documentation and doc generation via http://documentation.js.org

## [0.2.0] - 2015-06-11
### Added
* getStat supports getting a statistic from the db directly
* tests for get stat

## [0.1.6] - 2015-06-11
### Changed
* Update URLs in package.json for org change
* Improve documentation in README.md

## [0.1.5] - 2015-06-09
### Changed
* Refactored to fit [Standard](https://github.com/feross/standard) style

## [0.1.4] - 2015-06-08
### Changed
* Needed a better check on the value of the geohash precision when reducing precision in the DB

## [0.1.3] - 2015-05-21
### Changed
* When inserting data we now check for a fields array, if it exists we create an index on that field. Helps make queries faster.

## [0.1.2] - 2015-05-20
### Changed
* Fixed OR filters to not append a problematic 'AND' to queries

## [0.1.1] - 2015-05-07
### Changed
* Using a more optimized recursive query to get a count of distinct geohash cells at a given substring

### Added
* a series of substring indexes get placed on tables in the DB to support geohashing

## [0.1.0] - 2015-04-30
### Added
* Added support for retrieving geohash aggregations directly from the DB.

## [0.0.6] - 2015-04-28
### Changed
* Fixed the creation of geospatial indexes for socrata tables with dashed in the name and using the geojson geometry in each feature

## [0.0.5] - 2015-04-16
### Changed
* fixed an issue where geometry filters and idFilters were in conflict in the generated sql

## [0.0.4] - 2015-04-13
### Added
* Support for providers to filter queries with an idFilter param. This optional param is appended to the where clause and allows the query to filter on table id instead of property values.

## [0.0.3] - 2015-04-11
### Changed
* Better logic for `<` or `>` operators in where clauses

## [0.0.2] - 2015-04-03
### Added
* Created a new method for applying coded value domain from Esri services: applyCodedDomains - called when filtering data with fields that contain coded value domains

[1.5.1]: https://github.com/Esri/koop-pgcache/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/Esri/koop-pgcache/compare/v1.4.3...v1.5.0
[1.4.3]: https://github.com/Esri/koop-pgcache/compare/v1.4.2...v1.4.3
[1.4.2]: https://github.com/Esri/koop-pgcache/compare/v1.4.1...v1.4.2
[1.4.1]: https://github.com/Esri/koop-pgcache/compare/v1.4.0...v1.4.1
[1.4.0]: https://github.com/Esri/koop-pgcache/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/Esri/koop-pgcache/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/Esri/koop-pgcache/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/Esri/koop-pgcache/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/Esri/koop-pgcache/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/Esri/koop-pgcache/compare/v0.2.2...v1.0.0
[0.2.2]: https://github.com/Esri/koop-pgcache/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/Esri/koop-pgcache/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/Esri/koop-pgcache/compare/v0.1.6...v0.2.0
[0.1.6]: https://github.com/Esri/koop-pgcache/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/Esri/koop-pgcache/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/Esri/koop-pgcache/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/Esri/koop-pgcache/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/Esri/koop-pgcache/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/Esri/koop-pgcache/compare/v0.1.1...v0.1.1
[0.1.0]: https://github.com/Esri/koop-pgcache/compare/v0.0.6...v0.1.0
[0.0.6]: https://github.com/Esri/koop-pgcache/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/Esri/koop-pgcache/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/Esri/koop-pgcache/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/Esri/koop-pgcache/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/Esri/koop-pgcache/compare/v0.0.1...v0.0.2
