from enum import StrEnum


class NeonMembershipType(StrEnum):
    MONTHLY = "MONTH"
    ANNUAL = "YEAR"


class NeonMembershipStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    CANCELED = "CANCELED"
    REFUNDED = "REFUNDED"
    FAILED = "FAILED"
    DEFERRED = "DEFERRED"
    PENDING = "PENDING"


class NeonEventRegistrationStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    CANCELED = "CANCELED"
    FAILED = "FAILED"
    REFUNDED = "REFUNDED"
    DEFERRED = "DEFERRED"
    PENDING = "PENDING"


class NeonEventCategory(StrEnum):
    WOODWORKING = "Woodworking"
    WOODSHOP_SAFETY = "Woodshop Safety"
    METALWORKING = "Metalworking"
    MACHINING = "Machining"
    PRINTING_3D = "_3D Printing"
    LASERS = "Laser Cutting"
    ELECTRONICS = "Electronics"
    TEXTILES = "Textiles"
    CNC = "CNC Router"
    MISC = "Miscellaneous"
    PRIVATE = "Private"
    ORIENTATION = "Orientation"
    FACILITY_AND_SAFETY_TOUR = "Facility and Safety Tour"
    WOODSHOP_MENTOR_SERIES = "Woodshop Mentor Series"
    TOOL_SHARPENING = "Tool Sharpening"
    NONE = "None"


class Attended(StrEnum):
    WSS = "WSS"
    MSS = "MSS"
    CNC = "CNC"
    LASERS = "lasers"
    PRINTING_3D = "3dp"


class AccountCurrentMembershipStatus(StrEnum):
    ACTIVE = "Active"
    FUTURE = "Future"
    INACTIVE = "Inactive"
