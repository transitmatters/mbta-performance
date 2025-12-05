# ARCGIS_IDS for each year, rapid transit data
ARCGIS_IDS = {
    "2016": "3e892be850fe4cc4a15d6450de4bd318",
    "2017": "cde60045db904ad299922f4f8759dcad",
    "2018": "25c3086e9826407e9f59dd9844f6c975",
    "2019": "11bbb87f8fb245c2b87ed3c8a099b95f",
    "2020": "cb4cf52bafb1402b9b978a424ed4dd78",
    "2021": "611b8c77f30245a0af0c62e2859e8b49",
    "2022": "99094a0c59e443cdbdaefa071c6df609",
    "2023": "9a7f5634db72459ab731b6a9b274a1d4",
    "2024": "0711756aa5e1400891e79b984a94b495",
    "2025": "e2344a2297004b36b82f57772926ed1a",
}

# ARCGIS_IDS for each year, bus data
# 2018 & 2019 data is no longer available from the ArcGIS Hub
BUS_ARCGIS_IDS = {
    "2020": "4c1293151c6c4a069d49e6b85ee68ea4",
    "2021": "2d415555f63b431597721151a7e07a3e",
    "2022": "ef464a75666349f481353f16514c06d0",
    "2023": "b7b36fdb7b3a4728af2fccc78c2ca5b7",
    "2024": "96c77138c3144906bce93d0257531b6a",
    "2025": "924df13d845f4907bb6a6c3ed380d57a",
}


HISTORIC_COLUMNS_PRE_LAMP = [
    "service_date",
    "route_id",
    "trip_id",
    "direction_id",
    "stop_id",
    "stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "event_type",
    "event_time_sec",
]

HISTORIC_COLUMNS_LAMP = [
    "service_date",
    "route_id",
    "trip_id",
    "direction_id",
    "stop_id",
    "sync_stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "event_type",
    "event_time_sec",
]


#  Ferry Section
FERRY_UPDATE_CACHE_URL = "https://hub.arcgis.com/api/download/v1/items/ae21643bbe60488db8520cc694f882aa/csv?redirect=false&layers=0&updateCache=true"
FERRY_RIDERSHIP_ARCGIS_URL = "https://hub.arcgis.com/api/v3/datasets/ae21643bbe60488db8520cc694f882aa_0/downloads/data?format=csv&spatialRefId=4326&where=1%3D1"


CSV_FIELDS = [
    "service_date",
    "route_id",
    "trip_id",
    "direction_id",
    "stop_id",
    "stop_sequence",
    "vehicle_id",
    "vehicle_label",
    "event_type",
    "event_time",
    "vehicle_consist",
]


unofficial_ferry_labels_map = {
    "F1": "Boat-F1",
    "F2H": "Boat-F1",
    "F3": "Boat-EastBoston",
    "F4": "Boat-F4",
    "F5": "Boat-Lynn",
    "F6": "Boat-F6",
    "F7": "Boat-F7",
    "F8": "Boat-F8",
}

inbound_outbound = {"From Boston": 0, "To Boston": 1}

example_field_mapping = {
    "service_date": "service_date",
    "route_id": "route_id",
    "trip_id": "trip_id",
    "direction_id": "travel_direction",
    "stop_id": "stop_id",
    "stop_sequence": None,
    "vehicle_id": "vessel_time_slot",
    "vehicle_label": None,
    "event_type": None,
    "event_time": "actual_arrival",
    "scheduled_tt": None,
    "vehicle_consist": None,
}

arrival_field_mapping = {
    "service_date": "service_date",
    "route_id": "route_id",
    "trip_id": "trip_id",
    "travel_direction": "direction_id",
    "arrival_terminal": "stop_id",
    "vessel_time_slot": "vehicle_id",
    "actual_arrival": "event_time",
}

departure_field_mapping = {
    "service_date": "service_date",
    "route_id": "route_id",
    "trip_id": "trip_id",
    "travel_direction": "direction_id",
    "departure_terminal": "stop_id",
    "vessel_time_slot": "vehicle_id",
    "actual_departure": "event_time",
}

# For these I used context clues from the CSV and then matched up using the MBTA Website to find Stop IDs
station_mapping = {
    "Aquarium": "Boat-Aquarium",
    "Boston": "Boat-Long",
    "Central Whf": "Boat-Aquarium",
    "Georges": "Boat-George",
    "Hingham": "Boat-Hingham",
    "Hull": "Boat-Hull",
    "Lewis": "Boat-Lewis",
    "Logan": "Boat-Logan",
    "Long Wharf N": "Boat-Long",
    "Long Wharf S": "Boat-Long-South",
    "Lynn": "Boat-Blossom",
    "Navy Yard": "Boat-Charlestown",
    "Quincy": "Boat-Quincy",
    "Rowes": "Boat-Rowes",
    "Rowes Wharf": "Boat-Rowes",
    "Seaport": "Boat-Fan",
    "Winthrop": "Boat-Winthrop",
    "HULL": "Boat-Hull",
    "LOGAN": "Boat-Logan",
}
