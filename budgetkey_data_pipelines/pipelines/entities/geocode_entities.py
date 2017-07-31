from datapackage_pipelines.wrapper import ingest, spew
import requests, logging, datetime, os
import geocoder
from sqlalchemy import create_engine, MetaData, or_
from sqlalchemy.orm import sessionmaker


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

    def __init__(self, parameters, datapackage, resources):
        self.parameters = parameters
        self.datapackage = datapackage
        self.resources = resources
        self.locations_cache = {}

    @staticmethod
    def get_schema():
        return {"fields": [{"name": "entity_id", "type": "string"},
                           {"name": "location", "type": "string"},
                           {"name": "lat", "type": "number"},
                           {"name": "lng", "type": "number"}],
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

    def geocoder_google(self, location):
        return geocoder.google(location)

    def get_entity_location_details_from_db(self, session, table, entity_id, location):
        got_entity_row, old_location, location_lat, location_lng = False, None, None, None
        if table is not None:
            if location:
                rows = session.query(table).filter(or_(table.c.entity_id == entity_id, table.c.location == location))
            else:
                rows = session.query(table).filter(or_(table.c.entity_id == entity_id))
            got_location_row = False
            for row in rows:
                if row.entity_id == entity_id:
                    old_location = row.location
                    got_entity_row = True
                if row.location == location:
                    location_lat, location_lng = row.lat, row.lng
                    got_location_row = True
                if not got_entity_row and not got_location_row:
                    raise Exception("Unexpected row: {}".format(row))
        return got_entity_row, old_location, location_lat, location_lng

    def filter_resource(self, resource):
        session = self.initialize_db_session()
        meta = MetaData(bind=session.connection())
        meta.reflect()
        table = meta.tables.get(self.parameters["geo-table"])
        for row in resource:
            entity_id, location = row["id"], self.get_location_string(row["details"])
            has_row, old_location, lat, lng = self.get_entity_location_details_from_db(session, table, entity_id, location)
            if (not has_row  # new entity - not geocoded, will be inserted
                or (has_row and not location and old_location)  # previously had a location, now doesn't
                or (has_row and location != old_location)):  # location changed
                # need to update DB
                if location and (not lat or not lng):
                    if location in self.locations_cache:
                        lat, lng = self.locations_cache[location]
                    else:
                        # new location, need to geocode
                        g = self.geocoder_google(location)
                        if g.ok:
                            lat, lng = g.latlng
                        else:
                            lat, lng = None, None
                        self.locations_cache[location] = lat, lng
                # only yield items which need to be updated in DB
                yield {"entity_id": entity_id, "location": location,
                       "lat": lat, "lng": lng}


if __name__ == "__main__":
    parameters, datapackage, resources = ingest()
    for i, resource in enumerate(datapackage["resources"]):
        if resource["name"] == parameters["input-resource"]:
            datapackage["resources"][i] = {"name": parameters["output-resource"],
                                           "path": parameters["output-resource"]+".csv",
                                           "schema": GeoCodeEntities.get_schema()}
    spew(datapackage, GeoCodeEntities(parameters, datapackage, resources).filter_resources())
