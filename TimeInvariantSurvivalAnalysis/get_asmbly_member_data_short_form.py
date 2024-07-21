"""
Get full Neon data for all Asmbly members with at least one membership.

This module will return data suitable for time-invariant covariate 
survival regression analysis where each row will correspond to 1 member
with the following pieces of data for each:
    - email
    - Neon ID
    - First name
    - Last name
    - Has Discourse ID (boolean)
    - Has OP ID (boolean)
    - Time from Asmbly (np.nan if address unavailable)
    - Gender (np.nan if unavailable)
    - Age (np.nan if unavailable)
    - Referral Source (np.nan if unavailable)
    - Family Membership (boolean)
    - Membership Cancelled (boolean)
    - Membership Type (monthly or annual)
    - Total classes attended before first membership
    - Total classes attended
    - Waiver Signed (boolean)
    - Orientation attended (boolean)
    - Woodshop Safety attended (boolean)
    - Metal Shop Safety attended (boolean)
    - CNC Router class attended (boolean)
    - Laser class attended (boolean)
    - 3DP class attended (boolean)
    - Steward (boolean)
    - Teacher (boolean)

"""

import base64
import time
import asyncio
import aiohttp
import requests
import googlemaps
import pandas as pd

from helpers.get_neon_data import get_all_accounts, get_individual_account
from helpers.enums import (
    AccountCurrentMembershipStatus,
)
from helpers.neon_dataclasses import Attended

from config import N_APIkey, N_APIuser, GOOGLE_MAPS_API_KEY

# Neon Account Info
N_AUTH = f"{N_APIuser}:{N_APIkey}"
N_BASE_URL = "https://api.neoncrm.com"
N_SIGNATURE = base64.b64encode(bytearray(N_AUTH.encode())).decode()
N_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {N_SIGNATURE}",
}


async def test_generator():
    for i in [14, 1743, 1332]:
        yield {"accounts": [{"accountId": i}]}


async def main():
    session = requests.Session()
    gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY, requests_session=session)
    asmbly_geocode = gmaps.geocode("9701 Dessau Rd Ste 304, Austin, TX 78754")[0][
        "geometry"
    ]["location"]

    async with aiohttp.ClientSession(
        headers=N_HEADERS, base_url=N_BASE_URL
    ) as aio_session:
        frames = []

        default_params = pd.DataFrame(
            {
                "neon_id": [None],
                "email": [None],
                "first_name": [None],
                "last_name": [None],
                "has_op_id": [False],
                "has_discourse_id": [False],
                "time_from_asmbly": [None],
                "age": [None],
                "gender": [None],
                "referral_source": [None],
                "family_membership": [False],
                "membership_cancelled": [False],
                "annual_membership": [False],
                "waiver_signed": [False],
                "orientation_attended": [False],
                "taken_MSS": [False],
                "taken_WSS": [False],
                "taken_cnc_class": [False],
                "taken_lasers_class": [False],
                "taken_3dp_class": [False],
                "teacher": [False],
                "steward": [False],
                "num_classes_before_joining": [None],
                "num_classes_attended": [None],
                "total_dollars_spent": [None],
                "duration": [None],
            }
        )

        search_fields = [
            {"field": "Account Type", "operator": "EQUAL", "value": "Individual"},
            {"field": "First Membership Enrollment Date", "operator": "NOT_BLANK"},
            # {
            #    "field": "Account ID",
            #    "operator": "IN_RANGE",
            #    "valueList": ["1743", "14"],
            # },
        ]

        output_fields = ["Account ID", "Account Current Membership Status"]

        async for page in get_all_accounts(aio_session, search_fields, output_fields):
            if page["pagination"]["currentPage"] < page["pagination"]["totalPages"]:
                print(
                    f"Page {page['pagination']['currentPage'] + 1}",
                    "of",
                    page["pagination"]["totalPages"],
                )
                accts = page["searchResults"]
            else:
                break

            # async for page in test_generator():
            # accts = page["accounts"]

            for i in accts:
                acct = await get_individual_account(
                    aio_session, i["Account ID"], i["Account Current Membership Status"]
                )

                attended = acct.has_taken_classes()
                distances = acct.get_distance_from_asmbly(gmaps, asmbly_geocode)

                member_df = default_params.copy(deep=True)

                member_df["neon_id"] = acct.neon_id
                member_df["email"] = acct.email
                member_df["first_name"] = acct.first_name
                member_df["last_name"] = acct.last_name
                member_df["has_op_id"] = acct.openpath_id is not None
                member_df["has_discourse_id"] = acct.discourse_id is not None
                member_df["age"] = acct.age
                member_df["gender"] = acct.gender
                member_df["referral_source"] = acct.referral_source
                member_df["family_membership"] = acct.family_membership
                member_df["waiver_signed"] = acct.waiver_date is not None
                member_df["orientation_attended"] = acct.orientation_date is not None
                member_df["taken_MSS"] = attended[Attended.MSS]
                member_df["taken_WSS"] = attended[Attended.WSS]
                member_df["taken_cnc_class"] = attended[Attended.CNC]
                member_df["taken_lasers_class"] = attended[Attended.LASERS]
                member_df["taken_3dp_class"] = attended[Attended.PRINTING_3D]
                member_df["teacher"] = acct.teacher
                member_df["steward"] = acct.steward
                member_df["num_classes_before_joining"] = (
                    acct.get_classes_before_first_membership()
                )
                member_df["num_classes_attended"] = acct.total_classes_attended
                member_df["total_dollars_spent"] = acct.total_dollars_spent
                member_df["time_from_asmbly"] = distances["time"]

                cancelled = (
                    acct.current_membership_status
                    == AccountCurrentMembershipStatus.INACTIVE
                )

                member_df["membership_cancelled"] = cancelled
                member_df["annual_membership"] = acct.has_annual_membership
                member_df["duration"] = acct.membership_duration

                frames.append(member_df)

                # Some basic progress tracking
                if (count := len(frames)) % 50 == 0:
                    print(f"--- {count} ---")

        return pd.concat(frames, ignore_index=True, sort=False)


if __name__ == "__main__":
    startTime = time.time()

    final = asyncio.run(main())

    final.to_csv("all_members_short_form.csv", index=False)

    print(f"--- {(time.time() - startTime)} seconds ---")
