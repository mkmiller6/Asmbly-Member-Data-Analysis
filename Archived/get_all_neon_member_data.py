"""
Get full Neon data for all Neon account holders at Asmbly.

For each constituent, get:
    - First Name
    - Last Name
    - email
    - Neon ID
    - OpenPathID (if any)
    - DiscourseId (if any)
    - # of Discourse posts (if any)
    - Total Discourse read time (if any)
    - Total # of Skedda bookings (if any)
    - Skedda bookings by category (if any)
    - Address (if any)
    - City (if any)
    - State (if any)
    - Zip (if any)
    - Gender (if any)
    - Phone (if any)
    - Birthdate (if any)
    - Classes attended (boolean for each class)
    - Total classes registered
    - Total classes attended
    - Total dollars spent (memberships and events only)
    - Referral Source
    - Family Membership (boolean)
    - Waiver Signed (boolean)
    - Orientation attended (boolean)
    - First active membership start date
    - Last active membership expiration date
    - Memberships count (x12 if annual)
"""

from pprint import pprint
import base64
import datetime
import time
from enum import StrEnum
from dataclasses import dataclass
import asyncio
import aiohttp
import pandas as pd

from config import N_APIkey, N_APIuser

# Neon Account Info
N_AUTH = f"{N_APIuser}:{N_APIkey}"
N_BASE_URL = "https://api.neoncrm.com"
N_SIGNATURE = base64.b64encode(bytearray(N_AUTH.encode())).decode()
N_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {N_SIGNATURE}",
}


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
    _3DPRINTING = "_3D Printing"
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


@dataclass
class NeonEventType:
    name: str
    category: NeonEventCategory


@dataclass
class NeonMembership:
    price: float
    start_date: datetime.date
    end_date: datetime.date
    status: NeonMembershipStatus
    type: NeonMembershipType


@dataclass
class NeonEventRegistration:
    event_type: NeonEventType
    event_id: str
    registration_date: datetime.date
    event_date: datetime.date
    registration_status: NeonEventRegistrationStatus
    registration_amount: float


@dataclass
class NeonAccount:
    neon_id: str
    first_name: str
    last_name: str
    email: str
    address: str | None
    city: str | None
    state: str | None
    zip: str | None
    phone: str | None
    birthdate: str | None
    gender: str | None
    openpath_id: str | None
    discourse_id: str | None
    referral_source: str | None
    family_membership: bool
    waiver_date: datetime.date | None
    orientation_date: datetime.date | None
    memberships: list[NeonMembership] | None
    event_registrations: list[NeonEventRegistration] | None

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"

    @property
    def membership_count(self) -> int:
        count = 0
        if self.memberships:
            for m in self.memberships:
                if (
                    m.type == NeonMembershipType.ANNUAL
                    and m.status == NeonMembershipStatus.SUCCEEDED
                ):
                    count += 12
                elif (
                    m.type == NeonMembershipType.MONTHLY
                    and m.status == NeonMembershipStatus.SUCCEEDED
                ):
                    count += 1
        return count

    @property
    def event_registration_count(self) -> int:
        return 0 if not self.event_registrations else len(self.event_registrations)

    @property
    def event_attended_count(self) -> int:
        if not self.event_registrations:
            return 0
        return len(
            [
                r
                for r in self.event_registrations
                if r.registration_status == NeonEventRegistrationStatus.SUCCEEDED
            ]
        )

    @property
    def total_dollars_spent(self) -> float:
        mem_sum, event_sum = 0, 0
        if self.memberships:
            mem_sum = sum(
                m.price
                for m in self.memberships
                if m.status == NeonMembershipStatus.SUCCEEDED
            )
        if self.event_registrations:
            event_sum = sum(
                r.registration_amount
                for r in self.event_registrations
                if r.registration_status == NeonEventRegistrationStatus.SUCCEEDED
            )
        return mem_sum + event_sum

    @property
    def first_membership_start_date(self) -> datetime.date | None:
        if self.memberships:
            return self.memberships[0].start_date
        return None

    @property
    def latest_membership_end_date(self) -> datetime.date | None:
        if self.memberships:
            return self.memberships[-1].end_date
        return None


def backoff_time(retry_count: int) -> float:
    """
    Calculates the backoff time for retrying an operation. The backoff time increases exponentially.

    Parameters:
        retry_count (int): The number of times the operation has been retried.

    Returns:
        wait_time (float): The calculated backoff time in seconds.
    """
    initial_delay = 200 / 1000
    wait_time = (2**retry_count) * initial_delay
    return wait_time


async def get_all_accounts(aio_session: aiohttp.ClientSession):
    """
    Asynchronously retrieves all Neon accounts from the Neon API.

    Parameters:
        aioSession (aiohttp.ClientSession): The aiohttp client session to use for making
        HTTP requests.

    Returns:
        accounts (list): A list of all Neon accounts.
    """
    resource_path = "/v2/accounts"
    max_retries = 10
    page = 0

    while True:

        params = {"currentPage": page, "pageSize": 200, "userType": "INDIVIDUAL"}

        for i in range(max_retries):
            async with aio_session.get(resource_path, params=params) as accounts:
                if accounts.status == 200:
                    accounts_json = await accounts.json()
                    break
                if accounts.status in set([429, 502]):
                    await asyncio.sleep(backoff_time(i))
                else:
                    print(accounts.status)

        yield accounts_json
        page += 1


async def get_acct_event_registrations(
    aio_session: aiohttp.ClientSession, neon_id: str
) -> list[NeonEventRegistration]:
    """
    Asynchronously retrieves all Neon event registrations for an account from the Neon API.

    Parameters:
        aioSession (aiohttp.ClientSession): The aiohttp client session to use for making
        HTTP requests.
        neon_id (str): The Neon ID of the account to retrieve event registrations for.

    Returns:
        event_registrations (list[NeonEventRegistration]): A list of all Neon event registrations
        for the account.
    """
    resource_path = f"/v2/accounts/{neon_id}/eventRegistrations"
    max_retries = 10

    params = {
        "currentPage": 0,
        "pageSize": 200,
        "sortColumn": "registrationDateTime",
        "sortDirection": "ASC",
    }

    for i in range(max_retries):
        async with aio_session.get(resource_path, params=params) as event_registrations:
            if event_registrations.status == 200:
                event_registrations_json = await event_registrations.json()
                event_registrations_json = event_registrations_json.get(
                    "eventRegistrations"
                )
                break
            if event_registrations.status in set([429, 502]):
                await asyncio.sleep(backoff_time(i))
            else:
                print(event_registrations.status)
                return None

    if not event_registrations_json:
        return None
    all_registrations = []
    for event in event_registrations_json:

        registration_date = datetime.datetime.strptime(
            event["registrationDateTime"], "%Y-%m-%dT%H:%M:%SZ"
        ).date()
        event_id = event["eventId"]
        registration_amount = event["registrationAmount"]
        status = event["tickets"][0]["attendees"][0]["registrationStatus"]

        for i in range(max_retries):
            async with aio_session.get(f"/v2/events/{event_id}") as event:
                if event.status == 200:
                    event_json = await event.json()
                    break
                if event.status in set([429, 502]):
                    await asyncio.sleep(backoff_time(i))
                else:
                    print(event.status)
                    return None

        event_name = event_json["name"].split(" w/")[0]
        event_date = datetime.datetime.strptime(
            event_json["eventDates"]["startDate"], "%Y-%m-%d"
        ).date()
        event_category = "None"
        if category := event_json["category"]:
            event_category = category.get("name")

        neon_event_type = NeonEventType(
            name=event_name,
            category=NeonEventCategory(event_category),
        )

        all_registrations.append(
            NeonEventRegistration(
                registration_date=registration_date,
                event_id=event_id,
                registration_status=NeonEventRegistrationStatus(status),
                event_type=neon_event_type,
                event_date=event_date,
                registration_amount=registration_amount,
            )
        )

    return all_registrations


async def get_acct_membership_data(
    aio_session: aiohttp.ClientSession, neon_id: str
) -> list[NeonMembership]:
    """
    Asynchronously retrieves all Neon memberships for an account from the Neon API.

    Parameters:
        aioSession (aiohttp.ClientSession): The aiohttp client session to use for making
        HTTP requests.
        neon_id (str): The Neon ID of the account to retrieve memberships for.

    Returns:
        memberships (list[NeonMembership]): A list of all Neon memberships for the account.
    """
    resource_path = f"/v2/accounts/{neon_id}/memberships"
    params = {
        "currentPage": 0,
        "pageSize": 200,
        "sortColumn": "date",
        "sortDirection": "ASC",
    }
    max_retries = 10

    for i in range(max_retries):
        async with aio_session.get(resource_path, params=params) as memberships:
            if memberships.status == 200:
                memberships_json = await memberships.json()
                memberships_json = memberships_json.get("memberships")
                break
            if memberships.status in set([429, 502]):
                await asyncio.sleep(backoff_time(i))
            else:
                print(memberships.status)

    if not memberships_json:
        return None

    all_memberships = []
    for membership in memberships_json:
        start_date = datetime.datetime.strptime(
            membership["termStartDate"], "%Y-%m-%d"
        ).date()
        end_date = datetime.datetime.strptime(
            membership["termEndDate"], "%Y-%m-%d"
        ).date()
        all_memberships.append(
            NeonMembership(
                price=membership["fee"],
                start_date=start_date,
                end_date=end_date,
                type=NeonMembershipType(membership["termUnit"]),
                status=NeonMembershipStatus(membership["status"]),
            )
        )

    return all_memberships


async def get_individual_account(
    aio_session: aiohttp.ClientSession, neon_id: int
) -> NeonAccount:
    """
    Asynchronously retrieves a single Neon account from the Neon API.

    Parameters:
        aioSession (aiohttp.ClientSession): The aiohttp client session to use for making
        HTTP requests.
        neon_id (str): The Neon ID of the account to retrieve.

    Returns:
        account (dict): The Neon account with the specified Neon ID.
    """
    resource_path = f"/v2/accounts/{neon_id}"
    max_retries = 10

    for i in range(max_retries):
        async with aio_session.get(resource_path) as account:
            if account.status == 200:
                account_json = await account.json()
                break
            if account.status in set([429, 502]):
                await asyncio.sleep(backoff_time(i))
            else:
                print(account.status)
                return None

    try:
        first_name = account_json["individualAccount"]["primaryContact"].get(
            "firstName"
        )
    except TypeError:
        pprint(account_json)
    last_name = account_json["individualAccount"]["primaryContact"].get("lastName")
    email = account_json["individualAccount"]["primaryContact"].get("email1")
    gender = account_json["individualAccount"]["primaryContact"].get("gender")
    if gender:
        gender = gender.get("name")
    birthdate = account_json["individualAccount"]["primaryContact"].get("dob")
    if birthdate:
        try:
            birthdate = datetime.datetime.strptime(
                birthdate["year"] + "-" + birthdate["month"] + "-" + birthdate["day"],
                "%Y-%m-%d",
            ).date()
        except ValueError:
            birthdate = None
            pprint(account_json)

    addresses = account_json["individualAccount"]["primaryContact"].get("addresses")

    address = None
    if addresses:
        for i in addresses:
            if i.get("isPrimaryAddress") is True:
                address = i

    street, city, state, zip_code, phone = None, None, None, None, None
    if address:
        street = address.get("addressLine1")
        city = address.get("city")
        state = address.get("stateProvince")
        if state:
            state = state.get("code")
        zip_code = address.get("zipCode")
        phone = address.get("phone1")

    custom_fields = {
        field.get("name"): [field.get("value"), field.get("optionValues")]
        for field in account_json["individualAccount"].get("accountCustomFields")
    }

    openpath_id = custom_fields.get("OpenPathID")
    if openpath_id:
        openpath_id = openpath_id[0]
    discourse_id = custom_fields.get("DiscourseID")
    if discourse_id:
        discourse_id = discourse_id[0]

    family_membership = False
    if sub := custom_fields.get("Family Group Sub Member"):
        for i in sub[1]:
            if i.get("name") == "Yes":
                family_membership = True
                break
    if not family_membership:
        if primary := custom_fields.get("FamilyGroupPrimaryMember"):
            for i in primary[1]:
                if i.get("name") == "Family Group Primary Member":
                    family_membership = True
                    break

    waiver_date = custom_fields.get("WaiverDate")
    if waiver_date:
        waiver_date = datetime.datetime.strptime(
            waiver_date[0],
            "%m/%d/%Y",
        ).date()

    orientation_date = custom_fields.get("FacilityTourDate")
    if orientation_date:
        orientation_date = datetime.datetime.strptime(
            orientation_date[0], "%m/%d/%Y"
        ).date()

    referral_source = None
    if referral := custom_fields.get("Referral Source"):
        referral_source = referral[1][0].get("name")

    async with asyncio.TaskGroup() as tg:
        memberships = tg.create_task(get_acct_membership_data(aio_session, neon_id))
        event_registrations = tg.create_task(
            get_acct_event_registrations(aio_session, neon_id)
        )

    return NeonAccount(
        neon_id=neon_id,
        first_name=first_name,
        last_name=last_name,
        email=email,
        gender=gender,
        birthdate=birthdate,
        city=city,
        state=state,
        zip=zip_code,
        phone=phone,
        address=street,
        referral_source=referral_source,
        openpath_id=openpath_id,
        discourse_id=discourse_id,
        family_membership=family_membership,
        waiver_date=waiver_date,
        orientation_date=orientation_date,
        memberships=memberships.result(),
        event_registrations=event_registrations.result(),
    )


async def test_generator():
    for i in [14, 1743, 1332]:
        yield {"accounts": [{"accountId": i}]}


async def main():
    async with aiohttp.ClientSession(
        headers=N_HEADERS, base_url=N_BASE_URL
    ) as aio_session:
        frames = []
        count = 0

        async for page in get_all_accounts(aio_session):
            if page["pagination"]["currentPage"] < page["pagination"]["totalPages"]:
                accts = page["accounts"]
            else:
                break

            # async for page in test_generator():
            # accts = page["accounts"]

            for i in accts:
                if i["userType"] != "INDIVIDUAL":
                    continue
                acct = await get_individual_account(aio_session, i["accountId"])

                member_df = pd.DataFrame.from_records([vars(acct)])
                member_df.drop(
                    ["memberships", "event_registrations"], axis=1, inplace=True
                )
                member_df["membership_count"] = acct.membership_count
                member_df["first_membership_start"] = acct.first_membership_start_date
                member_df["last_membership_end"] = acct.latest_membership_end_date
                member_df["event_registration_count"] = acct.event_registration_count
                member_df["event_attended_count"] = acct.event_attended_count
                member_df["total_dollars_spent"] = acct.total_dollars_spent
                if acct.event_registrations is not None:
                    for event in acct.event_registrations:
                        member_df[event.event_type.name] = (
                            event.registration_status
                            == NeonEventRegistrationStatus.SUCCEEDED
                        )

                frames.append(member_df)

                if len(frames) % 50 == 0:
                    count += 50
                    print(f"--- {count} ---")

        return pd.concat(frames, ignore_index=True, sort=False)


if __name__ == "__main__":
    startTime = time.time()

    final = asyncio.run(main())

    final.to_csv("all_neon_members.csv", index=False)

    print(f"--- {(time.time() - startTime)} seconds ---")
