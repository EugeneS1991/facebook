from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from facebook_business.adobjects.ad import Ad
from facebook_business.adobjects.adcreative import AdCreative
from credential import event

def get_facebook_data(event):
    pubsub_massage = event[
        'data']  # переделать на base64, чтоба забрать потом data pub sub https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
    if pubsub_massage == 'facebook_cost':
        access_token = event['attributes']['access_token']
        app_secret = event['attributes']['app_secret']
        app_id = event['attributes']['app_id']
        account_id = event['attributes']['account_id']
        api_version = event['attributes']['api_version']
        # try:
        FacebookAdsApi.init(app_id, app_secret, access_token, api_version=api_version)
        # ad = Ad('23849464192990354')
        account = AdAccount('act_' + account_id)
        # adcreative has url_tags  23849464394760354
        # creative = AdCreative('23849464394760354')
        insights = account.get_ads(fields=[
            # AdCreative.Field.name,
            # AdCreative.Field.object_type,
            # AdCreative.Field.object_story_spec,
            # AdCreative.Field.url_tags,
            # AdCreative.Field.object_url,
            # AdCreative.Field.object_store_url,
            Ad.Field.id,
            Ad.Field.adset_id,
            Ad.Field.campaign_id,
            Ad.Field.creative,
        ],
                params={
                    'level': 'ad',
                    # 'breakdowns': ['country'],
                    'time_range': {'since': '2022-08-10',
                                   'until': '2022-08-10'},
                    'time_increment': 1})
        print(insights)
        #
        # account = AdAccount('act_' + account_id)
        #
        # insights = account.get_insights(
        #     fields=[
        #         AdsInsights.Field.date_start,
        #         # account
        #         AdsInsights.Field.account_id,
        #         AdsInsights.Field.account_name,
        #         AdsInsights.Field.account_currency,
        #         # campaign
        #         AdsInsights.Field.campaign_id,
        #         AdsInsights.Field.campaign_name,
        #         AdsInsights.Field.buying_type,
        #         AdsInsights.Field.objective,
        #         # adset
        #         AdsInsights.Field.adset_id,
        #         # AdsInsights.Field.adset_name,
        #         # AdsInsights.Field.adset_start,
        #         # ad
        #         AdsInsights.Field.ad_id,
        #         # AdsInsights.Field.ad_name,
        #         # AdsInsights.Field.created_time,
        #         # metriks
        #         AdsInsights.Field.clicks,
        #         AdsInsights.Field.impressions,
        #         AdsInsights.Field.reach,
        #         AdsInsights.Field.spend,
        #         AdsInsights.Field.actions,
        #         AdsInsights.Field.action_values,
        #         AdsInsights.Field.conversions,
        #         AdsInsights.Field.conversion_values
        #     ],
        #     params={
        #         'level': 'ad',
        #         # 'breakdowns': ['country'],
        #         'time_range': {'since': '2022-08-10',
        #                        'until': '2022-08-10'},
        #         'time_increment': 1})

        # adset_item = account.get_ads(fields={
        #     Ad.Field.id,
        #     Ad.Field.name,
        #     Ad.Field.created_time,
        #     # AdSet.Field.daily_budget,
        #     # AdSet.Field.billing_event,
        #     Ad.Field.bid_info,
        #     Ad.Field.bid_amount,
        #     Ad.Field.campaign_id,
        #     Ad.Field.date_format
        #     # AdSet.Field.bid_strategy,
        #     # AdSet.Field.bid_adjustments,
        #     # AdSet.Field.bid_constraints,
        #     # AdSet.Field.end_time
        # },
        #     params={
        #         'level': 'ad',
        #         # 'breakdowns': ['country'],
        #         'time_range': {'since': '2022-08-10',
        #                        'until': '2022-08-10'},
        #         'time_increment': 1})
        # print(adset_insights)
        # # full_list = []
        #
        # for item in adset_insights:
        #     for i in item:
        #         full_list.append({i: item.get(i)})

        # except:

        # data = [ x for x in chain(insights,adset_insights)]
        # print(data)
        pass
        return full_list


print(get_facebook_data(event))
