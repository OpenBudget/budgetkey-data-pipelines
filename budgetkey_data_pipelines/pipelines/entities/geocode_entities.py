from datapackage_pipelines.wrapper import ingest, spew
import requests, logging, datetime, os
import geocoder
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker


parameters, datapackage, resources = ingest()


# attributes in entity details that might contain the full address in a single string
LOCATION_ADDRESS_FIELDS = ["address"]

# simple mapping of fields to their possible sources in the data
# we will take the first item from the list of possible sources that has some data
LOCATION_FIELDS_MAP = {"street": ["street"],
                       "house_number": ["street_number", "house_number"],
                       "city": ["city"],
                       "country": ["country"],
                       "po_box": ["pob", "zipcode", "postal_code", "pob_postal_code"],}


def initialize_db_session():
    connection_string = os.environ.get("DPP_DB_ENGINE")
    assert connection_string is not None, \
        "Couldn't connect to DB - " \
        "Please set your '%s' environment variable" % "DPP_DB_ENGINE"
    engine = create_engine(connection_string)
    return sessionmaker(bind=engine)()


# returns a location string to geocode by
# returns None if no suitable location string is found
def get_location_string(entity_details):
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


def get_schema():
    return {"fields": [{"name": "entity_id", "type": "string"},
                       {"name": "location", "type": "string"},
                       {"name": "lat", "type": "number"},
                       {"name": "lng", "type": "number"}],
            "primaryKey": ["entity_id"]}


def get_all_geocoded_entity_ids():
    session = initialize_db_session()
    meta = MetaData(bind=session.connection())
    meta.reflect()
    table = meta.tables.get(parameters["geo-table"])
    if table is None:
        res = []
    else:
        res = [row.entity_id for row in session.query(table.c.entity_id).all()]
    return res


def filter_resource(resource):
    geocoded_entity_ids = get_all_geocoded_entity_ids()
    logging.info(geocoded_entity_ids)
    for row in resource:
        entity_id = row["id"]
        if entity_id not in geocoded_entity_ids:
            lat, lng = None, None
            location = get_location_string(row["details"])
            if location:
                logging.info("geocoding location: '{}'".format(location))
                g = geocoder.google(location)
                if g.ok:
                    lat, lng = g.latlng
            yield {"entity_id": entity_id, "location": location,
                   "lat": lat, "lng": lng}


def filter_resources(datapackage, resources):
    for resource_descriptor, resource in zip(datapackage["resources"], resources):
        logging.info(resource_descriptor)
        if resource_descriptor["name"] == parameters["output-resource"]:
            yield filter_resource(resource)
        else:
            yield resource


for i, resource in enumerate(datapackage["resources"]):
    if resource["name"] == parameters["input-resource"]:
        datapackage["resources"][i] = {"name": parameters["output-resource"],
                                       "path": parameters["output-resource"]+".csv",
                                       "schema": get_schema()}

spew(datapackage, filter_resources(datapackage, resources))
