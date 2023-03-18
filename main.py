from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from google.cloud import bigquery
from datetime import datetime, date, timedelta
import time
import base64
import json
import jsons
# import logging
# from pprint import pprint
import uuid
# from credential import event
# logger = logging.getLogger()


def schema():
    with open('schema.json', 'r') as f:
        schema = json.load(f)
    return schema

def load_table_from_json(bigquery_client, row_to_insert_df, project_id, dataset_id, table_id):
    job_config = bigquery.LoadJobConfig(
        autodetect=False,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema(),
        write_disposition='WRITE_APPEND',
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        # add column to table if this columt do not exist in table but need add this column to schema
        time_partitioning=bigquery.TimePartitioning(field="date_start"),
        clustering_fields=["campaign_id", "campaign_name"]
        # ignore_unknown_values=True
    )
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)

    load_job = bigquery_client.load_table_from_json(destination=table_ref, json_rows=row_to_insert_df,
                                                    job_config=job_config)
    try:
        result = load_job.result()
        print("Loaded job {}".format(result))

    except:
        err = 0
        for error in load_job.errors:
            err += 1
            print("Error load job {}: {}".format(err, error))

    return "ok"


def daterange(date_since, date_until):
    date_since = (date.today() - timedelta(3)) if date_since is None else datetime.strptime(date_since, "%Y-%m-%d").date()
    date_until = (date.today() - timedelta(2)) if date_until is None else datetime.strptime(date_until, "%Y-%m-%d").date()
    for n in range(int((date_until - date_since).days)):
        date_range = (date_since + timedelta(n))
        print("Fatch start date {}".format(date_range))
        yield date_range.strftime('%Y-%m-%d')

def facebook_data(app_id, app_secret, access_token, api_version, account_id, date_range):
    try:
        FacebookAdsApi.init(app_id, app_secret, access_token, api_version=api_version)
        account = AdAccount('act_' + account_id)

        insights_item = account.get_insights(
            fields=[
                AdsInsights.Field.date_start,
                AdsInsights.Field.date_stop,
                # account
                AdsInsights.Field.account_id,
                AdsInsights.Field.account_name,
                AdsInsights.Field.account_currency,
                # campaign
                AdsInsights.Field.campaign_id,
                AdsInsights.Field.campaign_name,
                AdsInsights.Field.buying_type,
                AdsInsights.Field.objective,
                # adset
                AdsInsights.Field.adset_id,
                # ad
                AdsInsights.Field.ad_id,
                AdsInsights.Field.ad_name,
                AdsInsights.Field.created_time,
                # metriks
                AdsInsights.Field.clicks,
                AdsInsights.Field.impressions,
                AdsInsights.Field.reach,
                AdsInsights.Field.spend,
                AdsInsights.Field.inline_link_clicks,
                AdsInsights.Field.inline_link_click_ctr,
                AdsInsights.Field.inline_post_engagement,
                AdsInsights.Field.social_spend,
                AdsInsights.Field.unique_clicks,
                AdsInsights.Field.unique_ctr,
                AdsInsights.Field.unique_inline_link_clicks,
                AdsInsights.Field.unique_inline_link_click_ctr,
                AdsInsights.Field.unique_link_clicks_ctr,
                # actions
                AdsInsights.Field.quality_ranking,
                AdsInsights.Field.engagement_rate_ranking,
                AdsInsights.Field.actions,
                AdsInsights.Field.action_values,
                AdsInsights.Field.video_thruplay_watched_actions,
                AdsInsights.Field.conversion_rate_ranking,
                AdsInsights.Field.conversions,
                AdsInsights.Field.conversion_values,
            ],
            params={
                'level': 'ad',
                'time_range': {'since': date_range,
                               'until': date_range},
                'time_increment': 1}, is_async=True)

        async_job = insights_item.api_get()
        while async_job[AdReportRun.Field.async_status] != 'Job Completed' or async_job[AdReportRun.Field.async_percent_completion] < 100:
            print(async_job[AdReportRun.Field.async_status])
            time.sleep(1)
            insights_item.api_get()
        time.sleep(1)
        print(async_job[AdReportRun.Field.async_status])
        insights_item_list = insights_item.get_result()

    except Exception as e:
        print(e)
        raise

    insights_item_result = []
    for item in insights_item_list:
        adset_id = item.pop("adset_id")
        ad_id = item.get("ad_id")
        adset = AdSet(fbid=adset_id)
        adset_item = adset.api_get([
            AdSet.Field.id,
            AdSet.Field.name,
            AdSet.Field.daily_budget,
            AdSet.Field.optimization_goal,
            AdSet.Field.destination_type,
            AdSet.Field.billing_event,
            AdSet.Field.created_time,
            AdSet.Field.bid_strategy,
            AdSet.Field.bid_amount,
            AdSet.Field.start_time,
            AdSet.Field.end_time,
        ],
            params={
                'level': 'ad',
                'time_range': {'since': date_range,
                               'until': date_range},
                'time_increment': 1})
        item.update({
            'adset': adset_item
        })

        ad = Ad(fbid=ad_id)
        ad_item = ad.api_get(fields=[
            Ad.Field.id,
            Ad.Field.name,
            Ad.Field.created_time,
            Ad.Field.bid_amount,
            Ad.Field.bid_type,
            Ad.Field.adlabels,
            Ad.Field.status,
            Ad.Field.targeting,
            Ad.Field.creative
        ],
            params={
                'level': 'ad',
                'time_range': {'since': date_range,
                               'until': date_range},
                'time_increment': 1})
        item.update({
            'ad': ad_item
        })
        creative_id = item.get('ad').pop('creative').get('id')
        creative = AdCreative(fbid=creative_id)
        creative_item = creative.api_get(fields=[
            AdCreative.Field.id,
            AdCreative.Field.name,
            AdCreative.Field.applink_treatment,
            AdCreative.Field.body,
            AdCreative.Field.call_to_action_type,
            AdCreative.Field.instagram_actor_id,
            AdCreative.Field.instagram_permalink_url,
            AdCreative.Field.instagram_story_id,
            AdCreative.Field.link_og_id,
            AdCreative.Field.link_url,
            AdCreative.Field.object_id,
            AdCreative.Field.object_story_id,
            AdCreative.Field.object_url,
            AdCreative.Field.object_type,
            AdCreative.Field.effective_object_story_id,
            AdCreative.Field.product_set_id,
            AdCreative.Field.template_url,
            AdCreative.Field.object_story_spec,
            AdCreative.Field.url_tags,
            AdCreative.Field.link_deep_link_url,
            AdCreative.Field.object_store_url
        ],
            params={
                'level': 'ad',
                'time_range': {'since': date_range,
                               'until': date_range},
                'time_increment': 1})
        item.update({
            'creative': creative_item
        })

        if item.get('creative').get('object_story_spec') is not None:
            if item.get('creative').get('object_story_spec').get('link_data') is not None:
                if item.get('creative').get('object_story_spec').get('link_data').get('image_crops') is not None:
                    item.get('creative').get('object_story_spec').get('link_data').pop('image_crops')

        insights_item_result.append(item)
    # print(jsons.dump(insights_item_result))
    yield jsons.dump(insights_item_result)


def get_data(app_id, app_secret, access_token, api_version, account_id, date_since, date_until):
    for date_range in daterange(date_since, date_until):
        events = facebook_data(app_id, app_secret, access_token, api_version, account_id, date_range)
        if events:
            for item in events:
                yield (item)

def add_data(app_id, app_secret, access_token, api_version, account_id, date_since, date_until):
    for events in get_data(app_id, app_secret, access_token, api_version, account_id, date_since, date_until):
        result = []
        for value in events:
            row_ids = str(uuid.uuid4())
            inserted_at_utc = str(datetime.utcnow())
            result.append(value | {'_row_ids': row_ids} | {'_inserted_at_utc': inserted_at_utc})
        yield result

def insert_data(event, context):
    pubsub_massage = base64.b64decode(event['data']).decode('utf-8')
    # pubsub_massage = event.get('data')
    if pubsub_massage == 'facebook_data':
        access_token = event.get('attributes').get('access_token')
        project_id = event.get('attributes').get('project_id')
        dataset_id = event.get('attributes').get('dataset_id')
        table_id = event.get('attributes').get('table_id')
        app_id = event.get('attributes').get('app_id')
        app_secret = event.get('attributes').get('app_secret')
        access_token = event.get('attributes').get('access_token')
        api_version = event.get('attributes').get('api_version')
        account_id = event.get('attributes').get('account_id')
        date_since = event.get('attributes').get('date_since')
        date_until = event.get('attributes').get('date_until')
        # GOOGLE_APPLICATION_CREDENTIALS = '/Projects/connectors/credentials/or2-msq-epm-plx1-t1iylu-01927efe0aef.json'
        # bigquery_client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
        bigquery_client = bigquery.Client()

        for row_to_insert in add_data(app_id, app_secret, access_token, api_version, account_id, date_since, date_until):
            # pprint(row_to_insert)
            load_table_from_json(bigquery_client, row_to_insert, project_id, dataset_id, table_id)
    return "ok"

# insert_data(event, '1')
