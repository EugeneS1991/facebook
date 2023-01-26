from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.adsinsights import AdsInsights
from facebook_business.adobjects.adcreative import AdCreative
from facebook_business.adobjects.ad import Ad
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
        # # creative_id 23852852815460354
        # ad_insights = ad.api_get(fields=[
        # Ad.Field.creative,
        # Ad.Field.name])

        # ad_insights = creative.api_get(fields=[
        #     AdCreative.Field.name,
        #     AdCreative.Field.image_url
        # ])
        #
        # account = AdAccount('act_' + account_id)
        # ad_insights = account.get_ad_creatives(fields=[
        #
        #     AdCreative.Field.id,
        #     AdCreative.Field.object_story_spec
        # ])
        #
        #adcreative has url_tags  23849464394760354
        creative = AdCreative('23849464394760354')

        ad_insights = creative.api_get(fields=[
            AdCreative.Field.name,
            AdCreative.Field.object_type,
            AdCreative.Field.object_story_spec,
            AdCreative.Field.url_tags,
            AdCreative.Field.object_url,
            AdCreative.Field.object_store_url
        ])




        # # insights = account.get_insights(
        # #     fields=[
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
        #         AdsInsights.Field.adset_name,
        #         AdsInsights.Field.adset_start,
        #         # ad
        #         AdsInsights.Field.ad_id,
        #         AdsInsights.Field.ad_name,
        #         # metriks
        #         AdsInsights.Field.created_time,
        #         AdsInsights.Field.clicks,
        #         AdsInsights.Field.impressions,
        #         AdsInsights.Field.reach,
        #         AdsInsights.Field.spend,
        #         AdsInsights.Field.actions,
        #         AdsInsights.Field.action_values,
        #         AdsInsights.Field.conversions,
        #         AdsInsights.Field.conversion_values
        #     ],
        #
        #     params={
        #         'level': 'ad',
        #         # 'breakdowns': {'country'},
        #         'time_range': {'since': '2022-08-10',
        #                        'until': '2022-08-10'},
        #         'time_increment': 1})
        #
        # # except:
        # pass
        return ad_insights


# get_insights


print(get_facebook_data(event))
