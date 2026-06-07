"""Pydantic schemas and section registry for AIP AD 2.X sections."""

from pydantic import BaseModel

from .ad_2_1 import LocationAndName
from .ad_2_2 import GeographicAndAdminData
from .ad_2_3 import OperationalHours
from .ad_2_4 import HandlingServices
from .ad_2_12 import RunwayPhysicalCharacteristicsSection
from .ad_2_13 import DeclaredDistances
from .ad_2_18 import ATSCommunicationFrequencies
from .ad_2_19 import NavigationAids

SECTION_REGISTRY: dict[str, type[BaseModel]] = {
    "AD 2.1": LocationAndName,
    "AD 2.2": GeographicAndAdminData,
    "AD 2.3": OperationalHours,
    "AD 2.4": HandlingServices,
    "AD 2.12": RunwayPhysicalCharacteristicsSection,
    "AD 2.13": DeclaredDistances,
    "AD 2.18": ATSCommunicationFrequencies,
    "AD 2.19": NavigationAids,
}

SUPPORTED_SECTIONS: tuple[str, ...] = tuple(SECTION_REGISTRY.keys())

__all__ = [
    "SECTION_REGISTRY",
    "SUPPORTED_SECTIONS",
    "LocationAndName",
    "GeographicAndAdminData",
    "OperationalHours",
    "HandlingServices",
    "RunwayPhysicalCharacteristicsSection",
    "DeclaredDistances",
    "ATSCommunicationFrequencies",
    "NavigationAids",
]
