import datetime
import asyncio
from pprint import pprint
from typing import AsyncIterator, AsyncGenerator

import aiohttp

from helpers.api_exponential_backoff import backoff_time
from helpers.enums import (
    NeonEventCategory,
    NeonEventRegistrationStatus,
    NeonMembershipStatus,
    NeonMembershipType,
    AccountCurrentMembershipStatus,
)

from helpers.neon_dataclasses import (
    Donation,
    NeonEventType,
    StoredNeonEvent,
    NeonMembership,
    NeonEventRegistration,
    NeonAccount,
)

stored_events: dict[int, StoredNeonEvent] = {}


async def get_all_accounts(
    aio_session: aiohttp.ClientSession, search_fields: dict, output_fields: list
) -> AsyncIterator[AsyncGenerator[dict, None]]:
    """
    Asynchronously retrieves all Neon accounts from the Neon API matching the search criteria.
    Output fields are determined by the output_fields variable.

    Parameters:
        aio_session (aiohttp.ClientSession): The aiohttp client session to use for making
        HTTP requests.
        search_fields: A dict of search criteria to filter Neon accounts.
        output_fields: A list of output fields desired.

    Returns:
        AsyncIterator[AsyncGenerator[dict]]: Async generator of dicts of Neon accounts
        matching the search fields.
    """
    resource_path = "/v2/accounts/search"
    max_retries = 10
    page = 0

    while True:

        data = {
            "searchFields": search_fields,
            "outputFields": output_fields,
            "pagination": {"currentPage": page, "pageSize": 200},
        }

        for i in range(max_retries):
            async with aio_session.post(resource_path, json=data) as accounts:
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
    aio_session: aiohttp.ClientSession, neon_id: str | int
) -> list[NeonEventRegistration]:
    """
    Asynchronously retrieves all Neon event registrations for an account from the Neon API.

    Parameters:
        aioSession (aiohttp.ClientSession): The aiohttp client session to use for making
        HTTP requests.
        neon_id (str | int): The Neon ID of the account to retrieve event registrations for.

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
        return []
    all_registrations = []
    for event in event_registrations_json:

        status = event["tickets"][0]["attendees"][0]["registrationStatus"]
        status = NeonEventRegistrationStatus(status)
        if status != NeonEventRegistrationStatus.SUCCEEDED:
            continue

        # registration_date = datetime.datetime.strptime(
        #    event["registrationDateTime"], "%Y-%m-%dT%H:%M:%SZ"
        # ).date()
        event_id = event["eventId"]
        registration_amount = event["registrationAmount"]

        if not (stored_event := stored_events.get(event_id)):

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

            stored_event = StoredNeonEvent(
                event_name=event_name,
                event_date=event_date,
                event_type=neon_event_type,
                category=NeonEventCategory(event_category),
            )

            stored_events[event_id] = stored_event

        else:
            neon_event_type = stored_event.event_type
            event_date = stored_event.event_date

        all_registrations.append(
            NeonEventRegistration(
                event_id=event_id,
                registration_status=status,
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
    Asynchronously retrieves all Neon memberships for an account from the Neon API with a status
    of successful.

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
        return []

    all_memberships = []
    for membership in memberships_json:

        status = NeonMembershipStatus(membership["status"])

        if status != NeonMembershipStatus.SUCCEEDED:
            continue

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
                status=status,
            )
        )

    return all_memberships


async def get_acct_donation_data(
    aio_session: aiohttp.ClientSession, neon_id: str
) -> list[Donation]:
    """
    Asynchronously retrieves all Neon donations for an account from the Neon API.

    Parameters:
        aioSession (aiohttp.ClientSession): The aiohttp client session to use for making
        HTTP requests.
        neon_id (str): The Neon ID of the account to retrieve donations for.

    Returns:
        donations (list[Donation]): A list of all Neon donations for the account.
    """
    resource_path = f"/v2/accounts/{neon_id}/donations"
    params = {
        "currentPage": 0,
        "pageSize": 200,
        "sortColumn": "date",
        "sortDirection": "ASC",
    }
    max_retries = 10

    for i in range(max_retries):
        async with aio_session.get(resource_path, params=params) as donations:
            if donations.status == 200:
                donations_json = await donations.json()
                donations_json = donations_json.get("donations")
                break
            if donations.status in set([429, 502]):
                await asyncio.sleep(backoff_time(i))
            else:
                print(donations.status)

    if not donations_json:
        return []

    all_donations = []
    for donation in donations_json:

        date = datetime.datetime.strptime(donation["date"], "%Y-%m-%d").date()

        amount = donation["amount"]

        all_donations.append(
            Donation(
                amount=amount,
                date=date,
            )
        )

    return all_donations


async def get_individual_account(
    aio_session: aiohttp.ClientSession, neon_id: int, current_membership_status: str
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

    membership_status = AccountCurrentMembershipStatus(current_membership_status)

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

    if types := account_json["individualAccount"].get("individualTypes"):
        teacher = any(_type["name"] == "Instructor" for _type in types)
        steward = any(
            _type["name"] == "Steward" or _type["name"] == "Super Steward"
            for _type in types
        )
        volunteer = any(_type["name"] == "Volunteer" for _type in types)
    else:
        teacher, steward, volunteer = False, False, False

    async with asyncio.TaskGroup() as tg:
        memberships = tg.create_task(get_acct_membership_data(aio_session, neon_id))
        event_registrations = tg.create_task(
            get_acct_event_registrations(aio_session, neon_id)
        )
        donations = tg.create_task(get_acct_donation_data(aio_session, neon_id))

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
        teacher=teacher,
        steward=steward,
        volunteer=volunteer,
        current_membership_status=membership_status,
        memberships=memberships.result(),
        event_registrations=event_registrations.result(),
        donations=donations.result(),
    )
