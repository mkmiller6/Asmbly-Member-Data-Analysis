"""
Get full Neon data for all Asmbly members with at least one membership.

This module will return data in the "Long form" suitable for time-varying covariate 
survival regression analysis where each row will correspond to 1 membership period 
(monthly or annual) and the following pieces of data for each period:
    - email
    - Neon ID
    - DiscourseID (None if none)
    - Has Discourse ID (time invariant boolean)
    - Has OP ID (time invariant boolean)
    - Distance from Asmbly (np.nan if address unavailable)
    - Time from Asmbly (np.nan if address unavailable)
    - Gender (np.nan if unavailable)
    - Age (np.nan if unavailable)
    - Referral Source (np.nan if unavailable)
    - Family Membership (time invariant boolean)
    - Membership Cancelled (boolean)
    - Membership Type (monthly or annual)
    - Start interval (0, 1, 2, ...)
    - Stop interval (1, 2, 3, ...)
    - Start date of period (3/15/2022)
    - Stop date of period (4/14/2022)
    - Total classes attended this period
    - Total classes attended before first membership (time invariant)
    - Total dollars spent this period (memberships, events, and donations only)
    - Waiver Signed (time invariant boolean)
    - Orientation attended (time invariant boolean)
    - Woodshop Safety attended (time invariant boolean)
    - Metal Shop Safety attended (time invariant boolean)
    - CNC Router class attended (time invariant boolean)
    - Laser class attended (time invariant boolean)
    - Steward (time invariant boolean)
    - Teacher (time invariant boolean)
    - Volunteer (time invariant boolean)
    - # Woodshop classes attended this period
    - # Metal Shop classes attended this period
    - # Lasers classes attended this period
    - # Textiles classes attended this period
    - # Electronics classes attended this period
    - # 3dp classes attended this period

Later the following data will be merged:
    - Skedda booking count this period (0 if none)
    - Skedda booking minutes this period (0 if none)
    - Discourse read time this period (0 if none)
    - Discourse posts read this period (0 if none)

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
    NeonMembershipType,
    NeonEventCategory,
    AccountCurrentMembershipStatus,
)
from helpers.neon_dataclasses import NeonAccount, NeonMembership, Attended

from config import N_APIkey, N_APIuser, GOOGLE_MAPS_API_KEY

# Neon Account Info
N_AUTH = f"{N_APIuser}:{N_APIkey}"
N_BASE_URL = "https://api.neoncrm.com"
N_SIGNATURE = base64.b64encode(bytearray(N_AUTH.encode())).decode()
N_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Basic {N_SIGNATURE}",
}


def add_row(
    row_list: list,
    df: pd.DataFrame,
    acct: NeonAccount,
    interval: int,
    membership: NeonMembership,
) -> None:
    class_counter = acct.classes_by_category_in_period(
        membership.start_date, membership.end_date
    )

    df_params = df.copy(deep=True)

    df_params.loc[0, "start"] = interval - 1
    df_params.loc[0, "stop"] = interval
    df_params.loc[0, "membership_type"] = membership.type
    df_params.loc[0, "start_date"] = membership.start_date
    df_params.loc[0, "end_date"] = membership.end_date
    df_params.loc[0, "dollars_spent"] = acct.dollars_spent_in_period(membership)
    df_params.loc[0, "num_classes_attended"] = (
        0 if not class_counter else class_counter.total()
    )
    df_params.loc[0, "woodshop_classes"] = (
        0
        if not class_counter
        else (
            class_counter[NeonEventCategory.WOODWORKING]
            + class_counter[NeonEventCategory.WOODSHOP_SAFETY]
            + class_counter[NeonEventCategory.CNC]
        )
    )
    df_params.loc[0, "metal_shop_classes"] = (
        0
        if not class_counter
        else (
            class_counter[NeonEventCategory.METALWORKING]
            + class_counter[NeonEventCategory.MACHINING]
        )
    )
    df_params.loc[0, "lasers_classes"] = (
        0 if not class_counter else class_counter[NeonEventCategory.LASERS]
    )

    df_params.loc[0, "textiles_classes"] = (
        0 if not class_counter else class_counter[NeonEventCategory.TEXTILES]
    )

    df_params.loc[0, "electronics_classes"] = (
        0 if not class_counter else class_counter[NeonEventCategory.ELECTRONICS]
    )

    df_params.loc[0, "3dp_classes"] = (
        0 if not class_counter else class_counter[NeonEventCategory.PRINTING_3D]
    )

    row_list.append(df_params)


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
                "discourse_id": [None],
                "distance_from_asmbly": [None],
                "time_from_asmbly": [None],
                "age": [None],
                "gender": [None],
                "referral_source": [None],
                "family_membership": [False],
                "membership_cancelled": [False],
                "membership_type": [None],
                "start": [None],
                "stop": [None],
                "start_date": [None],
                "end_date": [None],
                "waiver_signed": [False],
                "orientation_attended": [False],
                "metal_shop_safety": [False],
                "woodshop_safety": [False],
                "cnc_router": [False],
                "lasers": [False],
                "teacher": [False],
                "steward": [False],
                "volunteer": [False],
                "num_classes_before_joining": [None],
                "num_classes_attended": [None],
                "dollars_spent": [None],
                "woodshop_classes": [None],
                "metal_shop_classes": [None],
                "lasers_classes": [None],
                "textiles_classes": [None],
                "electronics_classes": [None],
                "3dp_classes": [None],
            }
        )

        search_fields = [
            {"field": "Account Type", "operator": "EQUAL", "value": "Individual"},
            {"field": "First Membership Enrollment Date", "operator": "NOT_BLANK"},
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

                df_params = default_params.copy(deep=True)

                df_params["neon_id"] = acct.neon_id
                df_params["email"] = acct.email
                df_params["first_name"] = acct.first_name
                df_params["last_name"] = acct.last_name
                df_params["has_op_id"] = acct.openpath_id is not None
                df_params["has_discourse_id"] = acct.discourse_id is not None
                df_params["discourse_id"] = acct.discourse_id
                df_params["age"] = acct.age
                df_params["gender"] = acct.gender
                df_params["referral_source"] = acct.referral_source
                df_params["family_membership"] = acct.family_membership
                df_params["waiver_signed"] = acct.waiver_date is not None
                df_params["orientation_attended"] = acct.orientation_date is not None
                df_params["metal_shop_safety"] = attended[Attended.MSS]
                df_params["woodshop_safety"] = attended[Attended.WSS]
                df_params["cnc_router"] = attended[Attended.CNC]
                df_params["lasers"] = attended[Attended.LASERS]
                df_params["teacher"] = acct.teacher
                df_params["steward"] = acct.steward
                df_params["volunteer"] = acct.volunteer
                df_params["num_classes_before_joining"] = (
                    acct.get_classes_before_first_membership()
                )
                df_params["distance_from_asmbly"] = distances["distance"]
                df_params["time_from_asmbly"] = distances["time"]

                row_list = []
                mem_intervals = acct.get_membership_periods()
                if annuals := mem_intervals.get(NeonMembershipType.ANNUAL):
                    for interval, membership in annuals.items():
                        add_row(row_list, df_params, acct, interval, membership)

                if monthlies := mem_intervals.get(NeonMembershipType.MONTHLY):
                    for interval, membership in monthlies.items():
                        add_row(row_list, df_params, acct, interval, membership)

                member_df = pd.concat(row_list, ignore_index=True, sort=False)

                last_mem_date = acct.memberships[-1].end_date
                cancelled = (
                    acct.current_membership_status
                    == AccountCurrentMembershipStatus.INACTIVE
                )
                member_df.loc[
                    member_df["end_date"] == last_mem_date, "membership_cancelled"
                ] = cancelled

                frames.append(member_df)

                # Some basic progress tracking
                if (count := len(frames)) % 50 == 0:
                    print(f"--- {count} ---")

        return pd.concat(frames, ignore_index=True, sort=False)


if __name__ == "__main__":
    startTime = time.time()

    final = asyncio.run(main())

    final.to_csv("all_members_long_form.csv", index=False)

    print(f"--- {(time.time() - startTime)} seconds ---")
