from datapackage_pipelines.utilities.resources import PROP_STREAMING
from datapackage_pipelines.wrapper import ingest, spew
import requests, logging, datetime, os
import geocoder
from sqlalchemy import create_engine, MetaData, or_
from sqlalchemy.orm import sessionmaker
from time import sleep
from copy import deepcopy


# attributes in entity details that might contain the full address in a single string
LOCATION_ADDRESS_FIELDS = ["address"]

# simple mapping of fields to their possible sources in the data
# we will take the first item from the list of possible sources that has some data
LOCATION_FIELDS_MAP = {"street": ["street"],
                       "house_number": ["street_number", "house_number"],
                       "city": ["city"],
                       "country": ["country"],
                       "po_box": ["pob", "zipcode", "postal_code", "pob_postal_code"],}

class GeoCodeEntities(object):

    PROVIDER_DEFAULT_PARAMS = {
        "timeout": 5,
        "sleep_seconds": 1
    }

    # the processor will try each provider that has the env_vars in order up to the limit
    PROVIDERS = [
        # google seems to provide the best results, so we try to use it first
        # https://developers.google.com/maps/documentation/geocoding/usage-limits
        # $0.50 USD / 1000 up to 100000
        {
            "provider": "google",
            "env_vars": ["GOOGLE_API_KEY"],
            "limit": 50000
        },
        {
            "provider": "google",
            "env_vars": ["GOOGLE_CLIENT", "GOOGLE_CLIENT_SECRET"],
            "limit": 50000
        },
        # 1M limit of free geocode entities per year
        {
            "provider": "bing",
            "env_vars": ["BING_API_KEY"],
            "limit": 50000
        },
        # up 2500 daily free requests which it's worth to use
        # we could analyze the data later and decide which providers to use (we store the provider along with the lat / lng)
        {
            "provider": "google",
            "limit": 1000
        },
        # free and unlimited, but seems to provide lower quality results
        {
            "provider": "osm",
            "limit": 1000
        }
    ]

    def __init__(self, parameters, datapackage, resources):
        self.parameters = parameters
        self.datapackage = datapackage
        self.resources = resources
        self.locations_cache = {}
        # we will append providers that failed during this pipeline run here - to prevent retrying them again
        self.blacklist_providers = parameters.get("blacklist-providers", [])
        self.provider_request_counts = {provider["provider"]: 0 for provider in self.PROVIDERS}

    @staticmethod
    def get_schema():
        return {"fields": [{"name": "entity_id", "type": "string"},
                           {"name": "location", "type": "string"},
                           {"name": "lat", "type": "number"},
                           {"name": "lng", "type": "number"},
                           {"name": "provider", "type": "string"},
                           {"name": "geojson", "type": "object"}],
                "primaryKey": ["entity_id"]}

    def initialize_db_session(self):
        connection_string = os.environ.get("DPP_DB_ENGINE")
        assert connection_string is not None, \
            "Couldn't connect to DB - " \
            "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
        engine = create_engine(connection_string)
        return sessionmaker(bind=engine)()

    # returns a location string to geocode by
    # returns None if no suitable location string is found
    def get_location_string(self, entity_details):
        for field in LOCATION_ADDRESS_FIELDS:
            if entity_details.get(field) and len(entity_details[field]) > 5:
                # for this type of fields, the address is the best match
                return entity_details[field]
        address = {}
        for field, possible_sources in LOCATION_FIELDS_MAP.items():
            address[field] = ""
            for source in possible_sources:
                if entity_details.get(source) and len(entity_details[source]) > 0:
                    address[field] = entity_details[source]
                    break
        location = "{street} {house_number}, {city}, {country}, {po_box}".format(**address)
        return location if len(location) > 15 else None

    def filter_resources(self):
        for resource_descriptor, resource in zip(self.datapackage["resources"], self.resources):
            logging.info(resource_descriptor)
            if resource_descriptor["name"] == self.parameters["output-resource"]:
                yield self.filter_resource(resource)
            else:
                yield resource

    def geocoder_get(self, location, provider, timeout):
        return geocoder.get(location, provider=provider, session=self.requests_session, timeout=timeout)

    def get_entity_row(self, entity_id):
        if self.db_table is not None:
            rows = self.db_session.query(self.db_table).filter(self.db_table.c.entity_id==entity_id).all()
            if len(rows) > 0:
                return rows[0]
        return None

    def get_location_row(self, entity_id, entity_location):
        if self.db_table is not None:
            rows = self.db_session.query(self.db_table).filter(self.db_table.c.entity_id!=entity_id,
                                                               self.db_table.c.location==entity_location,
                                                               self.db_table.c.lat!=None,
                                                               self.db_table.c.lng!=None).all()
            if len(rows) > 0:
                return rows[0]
        return None

    def is_update_needed(self, entity_row, location):
        if entity_row is None:
            # new entity - not previously geocoded, will be inserted
            return True
        else:
            # entity matched by entity_id to existing row in entities_geocode table
            # need to determine if we should update this entity's geocode data or not
            if entity_row.location and not location:
                # previously had a location, now doesn't - should update it in DB
                return True
            elif entity_row.location != location:
                # location changed for this entity
                return True
            else:
                # no change is needed
                return False

    def warn_once(self, msg):
        if not hasattr(self, "_warn_once_msgs"):
            self._warn_once_msgs = []
        if msg not in self._warn_once_msgs:
            self._warn_once_msgs.append(msg)
            logging.warning(msg)

    def geocode(self, location):
        for provider_params in self.PROVIDERS:
            provider = deepcopy(self.PROVIDER_DEFAULT_PARAMS)
            provider.update(provider_params)
            geocode_provider = provider["provider"]
            if geocode_provider in self.blacklist_providers:
                self.warn_once("provider {} is blacklisted".format(geocode_provider))
            elif self.provider_request_counts[geocode_provider] >= provider["limit"]:
                self.warn_once("provider {} reached limit".format(geocode_provider))
            elif "" in [os.environ.get(env_var, "") for env_var in provider.get("env_vars", [])]:
                self.warn_once("provider {} requires environment variables {}".format(geocode_provider, provider["env_vars"]))
            else:
                # valid provider
                if provider["sleep_seconds"] > 0:
                    sleep(provider["sleep_seconds"])
                self.provider_request_counts[geocode_provider] += 1
                try:
                    g = self.geocoder_get(location, geocode_provider, provider["timeout"])
                except Exception:
                    logging.exception("geocoding exception, blacklist this provider")
                    self.blacklist_providers.append(geocode_provider)
                else:
                    if g.ok and g.confidence > 0:
                        # got valid geo data
                        return g.lat, g.lng, geocode_provider, g.geojson
                    else:
                        self.warn_once("couldn't get any geo data for provider {}, will not try again".format(geocode_provider))
                        # return the response and provider to allow to inspect it later in DB
                        return None, None, geocode_provider, g.geojson
        self.warn_once("exhausted all providers, couldn't find any geo data")
        return None

    def filter_resource(self, resource):
        self.db_session = self.initialize_db_session()
        self.db_meta = MetaData(bind=self.db_session.connection())
        self.db_meta.reflect()
        self.db_table = self.db_meta.tables.get(self.parameters["geo-table"])
        self.requests_session = requests.session()
        for row in resource:
            entity_id = row["id"]
            entity_location = self.get_location_string(row["details"])
            if entity_location:
                entity_row = self.get_entity_row(entity_id)
                if self.is_update_needed(entity_row, entity_location):
                    row = self.get_row(entity_id, entity_location)
                    if row:
                        yield row
                else:
                    logging.info("no update needed: {}".format(entity_id))
        self.requests_session.close()

    def get_row(self, entity_id, entity_location):
        # get DB row for this entity id and location
        # does the geocoding if needed
        location_cache = self.locations_cache.get(entity_location)
        if location_cache:
            # location was already processed in current run, we can reuse the result
            lat, lng, provider, geojson = location_cache
        else:
            lat, lng, provider, geojson = None, None, None, {}
            # check if there is another entity with the same location in DB
            # in that case we might not need to geocode again
            location_row = self.get_location_row(entity_id, entity_location)
            if location_row is not None:
                lat, lng = location_row.lat, location_row.lng
                provider, geojson = location_row.provider, location_row.geojson
            else:
                # need to geocode
                res = self.geocode(entity_location)
                if res is None:
                    # this means all providers were exhausted, we skip those rows and don't cache or yield them
                    return None
                else:
                    lat, lng, provider, geojson = res
            # store the result in internal cache - whether it's from geocode or from DB
            self.locations_cache[entity_location] = lat, lng, provider, geojson
        return {"entity_id": entity_id, "location": entity_location,
               "lat": lat, "lng": lng, "provider": provider,
               "geojson": geojson}


if __name__ == "__main__":
    parameters, datapackage, resources = ingest()
    for i, resource in enumerate(datapackage["resources"]):
        if resource["name"] == parameters["input-resource"]:
            datapackage["resources"][i] = {"name": parameters["output-resource"],
                                           "path": parameters["output-resource"]+".csv",
                                           "schema": GeoCodeEntities.get_schema(),
                                           PROP_STREAMING: True}
    spew(datapackage, GeoCodeEntities(parameters, datapackage, resources).filter_resources())
