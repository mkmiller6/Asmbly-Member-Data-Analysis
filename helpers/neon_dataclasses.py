from dataclasses import dataclass
from collections import Counter
import datetime
import googlemaps
import numpy as np

from helpers.enums import (
    NeonEventCategory,
    NeonEventRegistrationStatus,
    NeonMembershipStatus,
    NeonMembershipType,
    Attended,
    AccountCurrentMembershipStatus,
)


@dataclass
class Donation:
    date: datetime.date
    amount: float


@dataclass
class NeonEventType:
    name: str
    category: NeonEventCategory


@dataclass
class StoredNeonEvent:
    event_name: str
    event_date: datetime.date
    event_type: NeonEventType
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
    birthdate: datetime.date | None
    gender: str | None
    openpath_id: str | None
    discourse_id: str | None
    referral_source: str | None
    family_membership: bool
    waiver_date: datetime.date | None
    orientation_date: datetime.date | None
    teacher: bool
    steward: bool
    volunteer: bool
    current_membership_status: AccountCurrentMembershipStatus
    memberships: list[NeonMembership]
    event_registrations: list[NeonEventRegistration]
    donations: list[Donation]

    @property
    def full_name(self) -> str:
        return f"{self.first_name} {self.last_name}"

    @property
    def age(self) -> int:
        today = datetime.date.today()

        age = (today - self.birthdate).days // 365 if self.birthdate else None

        return age

    @property
    def total_dollars_spent(self) -> float:
        mem_sum, event_sum, donation_sum = 0, 0, 0
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
        if self.donations:
            donation_sum = sum(d.amount for d in self.donations)
        return mem_sum + event_sum + donation_sum

    @property
    def total_classes_attended(self) -> int:
        return len(self.event_registrations)

    @property
    def first_membership_start_date(self) -> datetime.date | None:
        if self.memberships:
            return self.memberships[0].start_date
        return None

    @property
    def has_annual_membership(self) -> bool:
        return any(m.type == NeonMembershipType.ANNUAL for m in self.memberships)

    @property
    def membership_duration(self) -> int:
        length = 0
        for m in self.memberships:
            if m.type == NeonMembershipType.MONTHLY:
                length += 1
                continue
            if m.type == NeonMembershipType.ANNUAL:
                length += 12

        return length

    def get_classes_before_first_membership(self) -> int:
        """
        Return a count of all classes attended before the start of the first membership.

        If acct has no memberships, return a count of all classes attended.
        """
        first_mem = self.first_membership_start_date

        if not first_mem:
            return len(self.event_registrations)

        events_attended = list(
            filter(lambda x: x.event_date < first_mem, self.event_registrations)
        )

        return len(events_attended)

    def get_classes_for_interval(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> list[NeonEventRegistration] | None:
        """
        For the interval determined by start_date and end_date,
        find the number of classes of each category attended
        """
        events_in_interval = list(
            filter(
                lambda x: x.event_date >= start_date and x.event_date <= end_date,
                self.event_registrations,
            )
        )

        if len(events_in_interval) == 0:
            return None

        return events_in_interval

    def get_donations_for_interval(
        self, start_date: datetime.date, end_date: datetime.date
    ) -> list[Donation] | None:
        """
        For the interval determined by start_date and end_date,
        find all donations made by this account.
        """
        donations_in_interval = list(
            filter(
                lambda x: x.date >= start_date and x.date <= end_date,
                self.donations,
            )
        )

        if len(donations_in_interval) == 0:
            return None

        return donations_in_interval

    def get_membership_periods(
        self,
    ) -> dict[NeonMembershipType, dict[int, NeonMembership]]:
        """
        Find the intervals of all membership periods for the account for both monthly and annual
        membership types.

        Return value is of the form:

        {
            Annual: {
                1: NeonMembership
                2: NeonMembership
            },
            Monthly: {
                1: NeonMembership
                2: NeonMembership
                3: NeonMembership
            }
        }

        Where either annual or monthly could be None.
        """
        intervals = {}

        for mem_type in NeonMembershipType:
            filtered = [
                membership
                for membership in self.memberships
                if membership.type == mem_type
            ]

            if len(filtered) > 0:
                for i, membership in enumerate(filtered, 1):
                    if not intervals.get(membership.type):
                        intervals[membership.type] = {}
                    intervals[membership.type][i] = membership

        return intervals

    def has_taken_classes(self) -> dict[Attended, bool]:
        """
        Determine if the user has taken the following classes:
        - Woodshop Safety
        - Metal Shop Safety
        - Any CNC class
        - Any lasers class
        - Any 3dp class
        """
        attended = {
            Attended.WSS: False,
            Attended.MSS: False,
            Attended.CNC: False,
            Attended.LASERS: False,
            Attended.PRINTING_3D: False,
        }
        if len(self.event_registrations) == 0:
            return attended

        for event in self.event_registrations:
            cat = event.event_type.category
            if cat == NeonEventCategory.WOODSHOP_SAFETY:
                attended[Attended.WSS] = True
            elif cat == NeonEventCategory.CNC:
                attended[Attended.CNC] = True
            elif cat == NeonEventCategory.LASERS:
                attended[Attended.LASERS] = True
            elif cat == NeonEventCategory.PRINTING_3D:
                attended[Attended.PRINTING_3D] = True
            elif "metal shop safety" in event.event_type.name.lower():
                attended[Attended.MSS] = True

        return attended

    def get_distance_from_asmbly(
        self, gmaps: googlemaps.Client, asmbly_geocode
    ) -> dict[str, float] | dict[str, type[np.nan]]:
        """
        Call the Google Maps API to calculate the distance and time from Asmbly based on the
        address of the account.
        """
        if (
            self.address is None
            or self.city is None
            or self.state is None
            or self.zip is None
        ):
            return {"distance": np.nan, "time": np.nan}
        try:
            routes = gmaps.directions(
                origin=f"{self.address}, {self.city}, {self.state} {self.zip}",
                destination=asmbly_geocode,
                mode="driving",
                avoid="tolls",
                # departure_time=datetime.datetime.now(),
                region="US",
                language="en",
                units="metric",
                # traffic_model="best_guess",
            )
        except googlemaps.exceptions.ApiError as e:
            print(e)
            return {"distance": np.nan, "time": np.nan}

        try:
            routes = routes[0]
        except IndexError:
            return {"distance": np.nan, "time": np.nan}

        distance = routes["legs"][0]["distance"]["value"]
        # try:
        # time = routes["legs"][0]["duration_in_traffic"]["value"]
        # except KeyError:
        time = routes["legs"][0]["duration"]["value"]

        return {"distance": distance, "time": time}

    def dollars_spent_in_period(self, membership: NeonMembership) -> float:
        """Find the total dollar value of events, memberships, and donations in the period"""
        start_date = membership.start_date
        end_date = membership.end_date

        events = self.get_classes_for_interval(start_date, end_date)
        donations = self.get_donations_for_interval(start_date, end_date)

        total = float(0)

        if events is not None:
            for event in events:
                total += event.registration_amount

        if donations is not None:
            for donation in donations:
                total += donation.amount

        total += membership.price

        return total

    def classes_by_category_in_period(
        self,
        start_date: datetime.date,
        end_date: datetime.date,
    ) -> Counter | None:
        """Count number of classes taken in each class category for the period."""

        events = self.get_classes_for_interval(start_date, end_date)

        if events is None:
            return None

        events_by_category = Counter()

        cats = [event.event_type.category for event in events]
        events_by_category.update(cats)

        return events_by_category
