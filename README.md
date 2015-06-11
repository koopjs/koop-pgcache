# koop-pgcache

[![npm][npm-img]][npm-url]

[npm-img]: https://img.shields.io/npm/v/koop-pgcache.svg?style=flat-square
[npm-url]: https://www.npmjs.com/package/koop-pgcache

A PostGIS data cache for [Koop](https://github.com/Esri/koop).

Koop's data caching is by default a local, in-memory object. `koop-pgcache` allows you to use PostGIS to cache data retrieved from requests more efficiently and minimize round trips and rate limiting from third party data providers.

## Install

```
npm install koop-pgcache
```

## Usage

To use a PostGIS cache, you need to have a PostgreSQL database with PostGIS enabled.

Detailed installation guides for PostgreSQL are available [here](https://wiki.postgresql.org/wiki/Detailed_installation_guides).

Instructions for enabling PostGIS on an existing PostgreSQL database are available [here](http://postgis.net/install/).

Once you have a PostGIS database for Koop to use as a cache, add the postgres address of the database to your Koop configuration and register the PostGIS cache like so:

```js
var config = {
  'db': {
    'conn': 'postgres://localhost/koopdev'
  }
}

var koop = require('koop')(config)
var koopPg = require('koop-pgcache')

koop.registerCache(koopPg)
```

If everything was configured correctly, Koop should now be using your PostGIS database to cache data from providers.

## Resources

* [Koop](https://github.com/Esri/koop)
* [PostgreSQL](http://www.postgresql.org/)
* [PostGIS](http://postgis.net/)
* [ArcGIS for Developers](http://developers.arcgis.com)
* [ArcGIS REST API Documentation](http://resources.arcgis.com/en/help/arcgis-rest-api/)
* [@esri](http://twitter.com/esri)

## Issues

Find a bug or want to request a new feature?  Please let us know by submitting an issue.

## Contributing

Esri welcomes contributions from anyone and everyone. Please see our [guidelines for contributing](https://github.com/esri/contributing).

## License

Copyright 2015 Esri

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

> http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

A copy of the license is available in the repository's [license.txt](license.txt) file.

[](Esri Tags: ArcGIS Web Mapping GeoJson FeatureServices)
[](Esri Language: JavaScript)
