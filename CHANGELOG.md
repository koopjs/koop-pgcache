# Change Log
All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning](http://semver.org/).

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

[0.0.6]: https://github.com/Esri/koop-pgcache/compare/v0.0.5...v0.0.6
[0.0.5]: https://github.com/Esri/koop-pgcache/compare/v0.0.4...v0.0.5
[0.0.4]: https://github.com/Esri/koop-pgcache/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/Esri/koop-pgcache/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/Esri/koop-pgcache/compare/v0.0.1...v0.0.2
