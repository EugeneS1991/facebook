from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adreportrun import AdReportRun
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
# from bson.json_util import dumps, loads
from datetime import datetime, date, timedelta
from credential import event
import time
import base64
import logging

logger = logging.getLogger()
schema_facebook_table = [
    bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("date_start", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("date_stop", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("account_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("account_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("account_currency", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("campaign_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("campaign_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("buying_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("objective", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ad_id", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ad_name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("ad_created_time", "DATE", mode="REQUIRED"),
    bigquery.SchemaField("clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("impressions", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("reach", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("spend", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("inline_link_clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("inline_link_click_ctr", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("inline_post_engagement", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("social_spend", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("unique_clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("unique_ctr", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("unique_inline_link_clicks", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("unique_inline_link_click_ctr", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("unique_link_clicks_ctr", "FLOAT", mode="REQUIRED"),
    bigquery.SchemaField("quality_ranking", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("engagement_rate_ranking", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("video_thruplay_watched_actions", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("conversion_rate_ranking", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("conversion_values", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("adset", "RECORD", mode="REPEATED",
                         fields=[
                             bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
                             bigquery.SchemaField("value", "STRING", mode="NULLABLE")
                         ]),
    bigquery.SchemaField("ad", "RECORD", mode="REPEATED",
                         fields=[
                             bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
                             bigquery.SchemaField("value", "STRING", mode="NULLABLE")
                         ]),
    bigquery.SchemaField("creative", "RECORD", mode="REPEATED",
                         fields=[
                             bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
                             bigquery.SchemaField("value", "STRING", mode="NULLABLE")
                         ]),
    bigquery.SchemaField("actions", "RECORD", mode="REPEATED",
                         fields=[
                             bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
                             bigquery.SchemaField("value", "STRING", mode="NULLABLE")
                         ]),
    bigquery.SchemaField("conversions", "RECORD", mode="REPEATED",
                         fields=[
                             bigquery.SchemaField("key", "STRING", mode="NULLABLE"),
                             bigquery.SchemaField("value", "STRING", mode="NULLABLE")
                         ]),
    bigquery.SchemaField("append_data_date", "TIMESTAMP", mode="REQUIRED")
]

clustering_field = ["campaign_id", "campaign_name"]


def exist_dataset_table(client, project_id, dataset_id, table_id, schema_facebook_table,
                        clustering_field=None):
    try:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        client.get_dataset(dataset_ref)

    except NotFound:
        dataset_ref = "{}.{}".format(project_id, dataset_id)
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        dataset = client.create_dataset(dataset)
        logger.info("Created dataset {}.{}".format(client.project, dataset.dataset_id))
        print("Created dataset {}.{}".format(client.project, dataset.dataset_id))

    try:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        client.get_table(table_ref)

    except NotFound:
        table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
        print(table_ref)
        table = bigquery.Table(table_ref, schema=schema_facebook_table)

        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date")

        if clustering_field is not None:
            table.clustering_fields = clustering_field

        table = client.create_table(table)
        logger.info("Created dataset {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        print("Created dataset {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
    return "ok"

def insert_rows_json(bigquery_client, project_id, dataset_id, table_id, result):
    table_ref = "{}.{}.{}".format(project_id, dataset_id, table_id)
    table = bigquery_client.get_table(table_ref)
    errors = bigquery_client.insert_rows_json(json_rows=result, table=table_ref)
    if errors == []:
        print("New rows have been added to table: {}".format(table.table_id))
        logger.info("New rows have been added to table: {}".format(table.table_id))
    else:
        print("Encountered errors while inserting rows: {}".format(errors))
        logger.info("Encountered errors while inserting rows: {}".format(errors))


def facebook_data(event, context):
    pubsub_massage = base64.b64decode(event['data']).decode('utf-8')  # переделать на base64, чтоба забрать потом data pub sub https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    bigquery_client = bigquery.Client()
    # GOOGLE_APPLICATION_CREDENTIALS = '/Projects/facebook_correct/or2-msq-epm-plx1-t1iylu-01927efe0aef.json'
    # bigquery_client = bigquery.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)

    if pubsub_massage == 'facebook_data':
        access_token = event.get('attributes').get('access_token')
        account_id = event.get('attributes').get('account_id')
        app_id = event.get('attributes').get('app_id')
        app_secret = event.get('attributes').get('app_secret')
        api_version = event.get('attributes').get('api_version')
        project_id = event.get('attributes').get('project_id')
        dataset_id = event.get('attributes').get('dataset_id')
        table_id = event.get('attributes').get('table_id')
        date_since = event.get('attributes',{}).get('date_since')
        date_until = event.get('attributes',{}).get('date_until')
        append_data_date = str(datetime.utcnow())
        try:
            FacebookAdsApi.init(app_id, app_secret, access_token, api_version=api_version)

            account = AdAccount('act_' + account_id)
            date_since = date_since if date_since is not None else date.today() - timedelta(3)
            date_until = date_until if date_until is not None else date.today() - timedelta(4)




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
                    # 'breakdowns': ['country'],
                    'time_range': {'since': date_since.strftime("%Y-%m-%d"),
                                   'until': date_until.strftime("%Y-%m-%d")},
                    'time_increment': 1}, is_async=True)

            async_job = insights_item.api_get()
            while async_job[AdReportRun.Field.async_percent_completion] < 100:
                print(async_job[AdReportRun.Field.async_status])
                time.sleep(1)
                insights_item.api_get()
            print(async_job[AdReportRun.Field.async_status])
            insights_item_list = insights_item.get_result()


        except Exception as e:
            logger.info(e)
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
                AdSet.Field.promoted_object,
                AdSet.Field.destination_type,
                AdSet.Field.billing_event,
                # AdSet.Field.attribution_spec,
                AdSet.Field.created_time,
                AdSet.Field.bid_strategy,
                AdSet.Field.bid_amount,
                AdSet.Field.start_time,
                AdSet.Field.end_time,
                # AdSet.Field.targeting
            ],
                params={
                    'level': 'ad',
                    # 'breakdowns': ['country'],
                    'time_range': {'since': date_since.strftime("%Y-%m-%d"),
                                   'until': date_until.strftime("%Y-%m-%d")},
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
                # Ad.Field.targeting,
                Ad.Field.creative
            ],
                params={
                    'level': 'ad',
                    # 'breakdowns': ['country'],
                    'time_range': {'since': date_since.strftime("%Y-%m-%d"),
                                   'until': date_until.strftime("%Y-%m-%d")},
                    'time_increment': 1})
            item.update({
                'ad': ad_item
            })
            #
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
                    # 'breakdowns': ['country'],
                    'time_range': {'since': date_since.strftime("%Y-%m-%d"),
                                   'until': date_until.strftime("%Y-%m-%d")},
                    'time_increment': 1})
            #
            item.update({
                'creative': creative_item
            })
            insights_item_result.append(item)

        result = []
        for item in insights_item_result:
            adset = []
            ad = []
            creative = []
            actions = []
            conversions = []

            if 'adset' in item:
                for key, value in item.pop('adset').items():
                    adset.append({'key': key, 'value': value})

            if 'ad' in item:
                for key, value in item.pop('ad').items():
                    ad.append({'key': key, 'value': value})

            if 'creative' in item:
                for key, value in item.pop('creative').items():
                    creative.append({'key': key, 'value': value})
            #
            if 'actions' in item:
                for value in item.pop('actions'):
                    actions.append({'key': value['action_type'], 'value': value['value']})

            if 'conversions' in item:
                for value in item.pop('conversions'):
                    conversions.append({'key': value['action_type'], 'value': value['value']})

            result.append({
                'date': item.get('date_start', None),
                'date_start': item.get('date_start', None),
                'date_stop': item.get('date_stop', None),
                'account_id': item.get('account_id', None),
                'account_name': item.get('account_name', None),
                'account_currency': item.get('account_currency', None),
                'campaign_id': item.get('campaign_id', None),
                'campaign_name': item.get('campaign_name', None),
                'buying_type': item.get('buying_type', None),
                'objective': item.get('objective', None),
                'ad_id': item.get('ad_id', None),
                'ad_name': item.get('ad_name', None),
                'ad_created_time': item.get('created_time', None),
                'clicks': item.get('clicks', None),
                'impressions': item.get('impressions', None),
                'reach': item.get('reach', None),
                'spend': item.get('spend', None),
                'inline_link_clicks': item.get('inline_link_clicks', None),
                'inline_link_click_ctr': item.get('inline_link_click_ctr', None),
                'inline_post_engagement': item.get('inline_post_engagement', None),
                'social_spend': item.get('social_spend', None),
                'unique_clicks': item.get('unique_clicks', None),
                'unique_ctr': item.get('unique_ctr', None),
                'unique_inline_link_clicks': item.get('unique_inline_link_clicks', None),
                'unique_inline_link_click_ctr': item.get('unique_inline_link_click_ctr', None),
                'unique_link_clicks_ctr': item.get('unique_link_clicks_ctr', None),
                'quality_ranking': item.get('quality_ranking', None),
                'engagement_rate_ranking': item.get('engagement_rate_ranking', None),
                'video_thruplay_watched_actions': item.get('video_thruplay_watched_actions', None),
                'conversion_rate_ranking': item.get('conversion_rate_ranking', None),
                'conversion_values': item.get('conversion_values', None),
                'adset': adset,
                'ad': ad,
                'creative': creative,
                'actions': actions,
                'conversions': conversions,
                'append_data_date': append_data_date
            })

            if exist_dataset_table(bigquery_client, project_id, dataset_id, table_id, schema_facebook_table,
                                   clustering_field) == "ok":
                insert_rows_json(bigquery_client, project_id, dataset_id, table_id, result)

            return "ok", result

