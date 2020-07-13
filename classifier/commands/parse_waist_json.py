"""
Classify loops through all the ads and save the scores to the database.
"""
import click
import dill
from classifier.utilities import (confs, DB)
import json
import spacy
import requests
from os import environ
# DATABASE_URL="postgres:///facebook_ads" pipenv run ./classify parse_waist_json --every
from datetime import datetime


AGE_OFFSET = 12 # Facebook seems to reflect ages as the age in years minus twelve. So, 65 is reflected in JSON as 53. Weird.

GENDER_CROSSWALK = {
    "MALE": "men",
    "FEMALE": "women"
}
TABLE_NAME = "fbpac_ads"

@click.command("parse_waist_json")
# @click.option("--newest/--every",
#               default=True,
#               help="Classify all of the records")
@click.pass_context
def parse_waist_json(ctx):
    """
    Classify the ads in the database at $DATABASE_URL.
    """

    # takes 8s locally
    start_time = datetime.now()    
    query = "select * from " + TABLE_NAME + " where targets = '[]' and targeting is not null and targeting ilike '{%'"

# to fix something, 
# update fbpac_ads set targets = '[]' where targeting like '%WAISTUIWorkEmployerType%';
# update fbpac_ads set targets = '[]' where targeting like '%WAISTUIRelationshipType%';
# update fbpac_ads set targets = '[]' where targeting like '%WAISTUIJobTitleType%';

    total = "select count(*) as length from ({}) as t1;"
    length = DB.query(total.format(query))[0]["length"]
    records = DB.query(query)
    print("found {} ads".format(length))
    updates = []
    query = "update " + TABLE_NAME + " set targets=:targets where id=:id"
    idx = 0
    for record in records:
        idx += 1
        record_lang = "en-US" if record["lang"] == "en-IE" else record["lang"]
        
        advertiser = record["advertiser"]

        created_at = record["created_at"]

        data = json.loads(record["targeting"])
        if record["targeting"][0] == '{' and "waist_targeting_data" in data:
            targeting = data["waist_targeting_data"] # this is necessary for all post Nov 13 - Dec 6 data.
        elif record["targeting"][0] == '{' and "data" in data:
            targeting = data["data"]["waist_targeting_data"] # this is necessary for all post Jan 29 (ATI code) data
        else:
            targeting = data
        if not advertiser and "waist_advertiser_info" in data:
            advertiser = data["waist_advertiser_info"]["name"]

        targets = parse_one_waist_json(targeting)

        # it appears there are never multiple distinct JSONs for one ad (besides diff profile_picture_url query strings and diff date formats)
        # TODO: once I have examples, implement unimplemented 

        update = {
            "id": record["id"],
            "targets": json.dumps(targets)
        }
        updates.append(update)
        out = "Parsed {pid[id]} ({info[idx]} of {info[length]}) with {pid[targets]}"
        # print(out.format(pid=update, info={"length": length, "idx": idx}))

        if len(updates) >= 100 and True:
            DB.bulk_query(query, updates)
            updates = []

    if updates and True:
        DB.bulk_query(query, updates)

    job_query = f"insert into job_runs(start_time, end_time, success, job_id, created_at, updated_at) select '{start_time}' start_time, now() end_time, true success, jobs.id job_id, now() created_at, now() updated_at from jobs where name = 'fbpac-waist-parser';"
    DB.query(job_query)
    # requests.post(environ.get("SLACKWH", 'example.com'), data=json.dumps({"text": f"(2/6): parsed WAIST JSON from {idx} ads"}), headers={"Content-Type": "application/json"})


# I've now actually seen these.
# Unknown WAIST type WAISTUIDPAType
# Unknown WAIST type WAISTUIActionableInsightsType


def parse_one_waist_json(targeting):
    targets = []
    for elem in targeting:
        if elem["__typename"] == "WAISTUICustomAudienceType":
            if elem["waist_ui_type"] == "CUSTOM_AUDIENCES_WEBSITE":
                targets += [["Website", "people who have visited their website or used one of their apps"]]
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_ENGAGEMENT_PAGE":
                targets += [["Activity on the Facebook Family", "fb page"]] # https://www.facebook.com/business/help/221146184973131?id=2469097953376494
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_ENGAGEMENT_IG":
                targets += [["Activity on the Facebook Family", "instagram business page"]] # https://www.facebook.com/business/help/214981095688584
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_ENGAGEMENT_LEAD_GEN":
                targets += [["Activity on the Facebook Family", "interacted with lead generation ads"]]
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_ENGAGEMENT_EVENT":
                targets += [["Activity on the Facebook Family", "event"]]
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_ENGAGEMENT_CANVAS":
                targets += [["Activity on the Facebook Family", "interacted with a 'Instant Experience' ad"]] # https://www.facebook.com/business/help/1056178197835021?id=2469097953376494
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_LOOKALIKE":
                targets += [["Retargeting", "people who may be similar to their customers"]]
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_MOBILE_APP":
                targets += [["Website", "activity in a non-Facebook mobile app"]]
                # mobile_ca_data app_name
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_DATAFILE":
                targets += [["List", ""]]
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_ENGAGEMENT_VIDEO":  # new to Python
                targets += [["Activity on the Facebook Family", "video"]] # https://www.facebook.com/business/help/221146184973131?id=2469097953376494
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_STORE_VISITS":  # new to Python
                targets += [["Offline data", "store visits"]] 
            elif elem["waist_ui_type"] == "CUSTOM_AUDIENCES_OFFLINE":  # new to Python
                targets += [["Offline data", "other"]] 

            else:
                print("UNKNOWN waist UI type: {}".format(elem["waist_ui_type"]))
                print(elem)
                # haven't seen these yet. # unimplemented
                # CUSTOM_AUDIENCES_OFFLINE
                # CUSTOM_AUDIENCES_UNRESOLVED

            if "dfca_data" in elem: 
                targets += [["Audience Owner", elem["dfca_data"]["ca_owner_name"]]]
                targets += [["Custom Audience Match Key", ', '.join(sorted(elem["dfca_data"]["match_keys"]))]]
            if "mobile_ca_data" in elem: 
                targets += [["Mobile App", elem["mobile_ca_data"]["app_name"]]]
            if "website_ca_data" in elem and elem["website_ca_data"].get("website_url", None): 
                targets += [["Mobile App", elem["website_ca_data"]["website_url"]]]


        elif elem["__typename"] ==  "WAISTUIAgeGenderType":
            # {"__typename"=>"WAISTUIAgeGenderType", "waist_ui_type"=>"AGE_GENDER", "age_min"=>23, "age_max"=>53, "gender"=>"ANY",  "id"=>"V0FJU1RVSUFnZUdlbmRlclR5cGU6MjM1Mw==", "serialized_data"=>"{\"age_min\":23,\"age_max\":53,\"gender\":null}",}
            targets += [
                ["MinAge", elem["age_min"] + AGE_OFFSET], 
                ["MaxAge", elem["age_max"] + AGE_OFFSET] if elem["age_max"] != 53 else None, 
                ["Age", ("{} and older".format(elem["age_min"] + AGE_OFFSET) if elem["age_max"] == 53 else ("{} and younger".format(elem["age_max"] + AGE_OFFSET) if elem["age_min"] == 0  else "{} to {}".format(elem["age_min"] + AGE_OFFSET, elem["age_max"] + AGE_OFFSET)))], # TODO. 30 to 45, 45 and older,  35 and younger
                ["Gender", GENDER_CROSSWALK[elem["gender"]]] if elem["gender"] != "ANY" else None
            ]
        elif elem["__typename"] ==  "WAISTUILocationType":
            # {"__typename"=>"WAISTUILocationType", "id"=>"V0FJU1RVSUxvY2F0aW9uVHlwZTpjaXR5LmhvbWUuMjQzMDUzNg==", "serialized_data"=>"{\"location_granularity\":\"city\",\"location_geo_type\":\"home\",\"location_code\":\"2430536\"}", "waist_ui_type"=>"LOCATION", "location_name"=>"Atlanta, Georgia", "location_type"=>"HOME"}
            # {"__typename"=>"WAISTUILocationType", "id"=>"V0FJU1RVSUxvY2F0aW9uVHlwZTpjb3VudHJ5LmhvbWUuVVM=", "serialized_data"=>"{\"location_granularity\":\"country\",\"location_geo_type\":\"home\",\"location_code\":\"US\"}", "waist_ui_type"=>"LOCATION", "location_name"=>"the United States", "location_type"=>"HOME"}
            # City... Occasionally has a comma in it. 
            # State # only in city state pairs
            # Region # e.g. United States, California
            granularity = json.loads(elem["serialized_data"])["location_granularity"] 
            if granularity == "city":
                *city, state = elem["location_name"].split(",")
                locs = [["City", ','.join(city)], ["State", state]]
            elif granularity == "region":
                locs = [["Region", elem["location_name"]]]
            elif granularity == "country":
                locs = [["Country", elem["location_name"]]]
            else:
                print("UNKNOWN location_granularity: {}".format(json.loads(elem["serialized_data"])["location_granularity"] ))
                locs = [[]]
            targets += locs + [["Location Granularity", json.loads(elem["serialized_data"])["location_granularity"] ], ["Location Type", elem["location_type"]]]
        elif elem["__typename"] ==  "WAISTUILocaleType":
            targets += [["Language", l] for l in elem["locales"]]
        elif elem["__typename"] ==  "WAISTUIInterestsType":
            targets += [["Interest", i["name"]] for i in elem["interests"]]
        elif elem["__typename"] ==  "WAISTUIBCTType": # thus far, likely engagement with conservative content
            if 'multicultural affinity' in elem["desc"]:
                elem["name"] = "Multicultural affinity: " + elem["name"] + "." # hotfix for multicultural affinity not showing up in the BCT name.
            targets += [["Segment", elem["name"]]]
        elif elem["__typename"] ==  "WAISTUIEduStatusType":
            targets += [["Education", elem["edu_status"]], ["Segment", "Bachelor's degree" if elem["edu_status"] == "EDU_COLLEGE_ALUMNUS" else elem["edu_status"] ]]
        elif elem["__typename"] ==  "WAISTUIConnectionType": # new to Python
            targets += [["Like", "Likes Page"]]
        elif elem["__typename"] ==  "WAISTUIEduSchoolsType": # new to Python
        # {'__typename': 'WAISTUIEduSchoolsType', 'id': 'V0FJU1RVSUVkdVNjaG9vbHNUeXBlOjg3ODczNjkzMTQx', 'serialized_data': '{"school_ids":[87873693141]}', 'waist_ui_type': 'EDU_SCHOOLS', 'school_names': ['Boston College']}
            targets += [["Education", school] for school in elem["school_names"]]
        elif elem["__typename"] ==  "WAISTUIFriendsOfConnectionType": # new to Python
            # print(elem)
            targets += [["Like", "Friend Likes Page"]]
            pass
        elif elem["__typename"] ==  "WAISTUIWorkEmployerType":
            targets += [["Employer", elem["employer_name"]]]
        elif elem["__typename"] ==  "WAISTUIRelationshipType":
            targets += [["Relationship Status", elem["relationship_status"]]]
        elif elem["__typename"] ==  "WAISTUIJobTitleType":
            targets += [["Job Title", elem["job_title"]]]
        elif elem["__typename"] ==  "WAISTUIActionableInsightsType": # appears to be stuff like "people who might be switching phone plans"
            targets += [["Segment", "Actionable Insights: " +  elem["name"]]]
        elif elem["__typename"] ==  "WAISTUIDPAType":
            # audience_type {1, 2}
            # audience_id ... could be anything
            # matched_data: USUALLY
            #    website_url {websites, None}
            #    app_name {None}
            #    event_time_string: {date, localized}
            if elem["matched_data"] and elem["matched_data"]["website_url"]:
                targets += [["Website", "Dynamic Product Ad"], ["Website Visit", elem["matched_data"]["website_url"]]]
            elif elem["matched_data"] and elem["matched_data"]["app_name"]:
                targets += [["Website", "Dynamic Product Ad"], ["App Usage", elem["matched_data"]["app_name"]]]
            else:
                targets += [["Offline Data", "Dynamic Product Ad"]]
        elif elem["__typename"] ==  "WAISTUILocalReachType":
            targets += [["Local Reach"]]

        else:
            print("Unknown WAIST type {}".format(elem["__typename"]))

            # no examples of these yet #unimplemented
            # WAISTUIBrandedContentWithPageType
            # WAISTUICollaborativeAdType
            # WAISTUIRelationshipType
            # WAISTUIJobTitleType

    return [{"target": t[0], "segment": t[1]} for t in targets if t]
