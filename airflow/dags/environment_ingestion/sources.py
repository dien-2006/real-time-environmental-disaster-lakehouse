
SOURCES = {
    "usgs": ("api.usgs", "USGSClient", "* * * * *"),
    "open_meteo": ("api.open_meteo", "OpenMeteoClient", "*/15 * * * *"),
    "openaq": ("api.openaq", "OpenAQClient", "*/15 * * * *"),
    "nasa_firms": ("api.nasa_firms", "NASAFirmsClient", "*/30 * * * *"),
}

