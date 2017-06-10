# coding=utf-8

import pkg_resources

def load_geoip():
    import pygeoip
    from pygeoip.const import MEMORY_CACHE
    
    resource_package = __name__
    filename = pkg_resources.resource_filename(resource_package, 'GeoIP.dat')
    return pygeoip.GeoIP(filename, flags=MEMORY_CACHE)
