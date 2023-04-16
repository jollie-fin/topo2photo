import utils.misc as misc
import os
import shutil
from tqdm.std import tqdm
import urllib.request

gmt_servers = misc.dotdict(
    oceania=misc.dotdict(
        name    ="Oceania (Master)",
        url     ="https://oceania.generic-mapping-tools.org",
        location="SOEST, U of Hawaii, USA"),
    brasil=misc.dotdict(
        name    ="Brasil",
        url     ="http://brasil.generic-mapping-tools.org",
        location="IAG-USP, U of Sao Paulo, Brazil"),
    australia=misc.dotdict(
        name    ="Australia",
        url     ="http://australia.generic-mapping-tools.org",
        location="EarthByte Group, Sydney U, Australia"),
    china=misc.dotdict(
        name    ="China",
        url     ="http://china.generic-mapping-tools.org",
        location="U of Sci. &amp; Tech. of China, China"),
    usaw=misc.dotdict(
        name    ="sdsc-opentopography misc.dotdict(US West Coast)",
        url     ="http://sdsc-opentopography.generic-mapping-tools.org",
        location="OpenTopography at San Diego Supercomputing Center"),
    usae=misc.dotdict(
        name    ="NOAA misc.dotdict(US East Coast)",
        url     ="http://noaa.generic-mapping-tools.org",
        location="Lab for Satellite Altimetry, NOAA, USA"),
    portugal=misc.dotdict(
        name    ="Portugal",
        url     ="http://portugal.generic-mapping-tools.org",
        location="U of Algarve, Portugal"),
    singapore=misc.dotdict(
        name    ="Singapore",
        url     ="http://singapore.generic-mapping-tools.org",
        location="National U of Singapore, Singapore"),
    southafrica=misc.dotdict(
        name    ="South Africa",
        url     ="http://south-africa.generic-mapping-tools.org",
        location="TENET, Tertiary Education &amp; Research Networks, South Africa",)
    )

def getBestGMTServer():
    return gmt_servers.oceania.url

def fetchFromInternet(path, url, desc=None):
    desc = os.path.basename(path) if desc is None else desc
    with urllib.request.urlopen(url) as response, open(path, 'wb') as f:
        with tqdm.wrapattr(
                f,
                "write",
                total=response.length,
                desc=desc,
            ) as file_obj:
            shutil.copyfileobj(response, file_obj)
