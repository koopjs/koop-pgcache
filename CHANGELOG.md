# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

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

[0.1.3]: https://github.com/Esri/koop-pgcache/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/Esri/koop-pgcache/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/Esri/koop-pgcache/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/Esri/koop-pgcache/compare/v0.1.1...v0.1.1
[0.1.0]: https://github.com/Esri/koop-pgcache/compare/v0.0.6...v0.1.0
[0.0.6]: https://github.com/Esri/koop-pgcache/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/Esri/koop-pgcache/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/Esri/koop-pgcache/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/Esri/koop-pgcache/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/Esri/koop-pgcache/compare/v0.0.1...v0.0.2
