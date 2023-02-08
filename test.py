# from facebook_business.api import FacebookAdsApi
# from facebook_business.adobjects.adaccount import AdAccount
# from facebook_business.adobjects.adsinsights import AdsInsights
# from facebook_business.adobjects.campaign import Campaign
# from facebook_business.adobjects.adset import AdSet
# from facebook_business.adobjects.ad import Ad
# from facebook_business.adobjects.adcreative import AdCreative
#
# from credential import event
# from itertools import chain
#
# def get_facebook_data(event):
#     pubsub_massage = event[
#         'data']  # переделать на base64, чтоба забрать потом data pub sub https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
#     if pubsub_massage == 'facebook_cost':
#         access_token = event['attributes']['access_token']
#         app_secret = event['attributes']['app_secret']
#         app_id = event['attributes']['app_id']
#         account_id = event['attributes']['account_id']
#         api_version = event['attributes']['api_version']
#         # try:
#         FacebookAdsApi.init(app_id, app_secret, access_token, api_version=api_version)
#
#         account = AdAccount('act_' + account_id)
#
#         insights = account.get_insights(
#             fields=[
#                 AdsInsights.Field.date_start,
#                 AdsInsights.Field.account_id,
#                 AdsInsights.Field.campaign_id,
#                 AdsInsights.Field.adset_id,
#
#             ],
#             params={
#                 'level': 'ad',
#                 # 'breakdowns': ['country'],
#                 'time_range': {'since': '2022-08-10',
#                                'until': '2022-08-10'},
#                 'time_increment': 1})
#         #
#         # print(insights)
#
#         api_one_dict = {item.pop("adset_id"): item for item in insights}
#         # print(api_one_dict)
#
#
#         adset_insights = account.get_ad_sets(fields=[
#             AdSet.Field.id,
#             AdSet.Field.daily_budget
#         ],
#             params={
#                 'level': 'ad',
#                 # 'breakdowns': ['country'],
#                 'time_range': {'since': '2022-08-10',
#                                'until': '2022-08-10'},
#                 'time_increment': 1})
#
#         for item in adset_insights:
#             # print(item)
#             if item.get("id") not in api_one_dict:
#                 continue
#             item_id = item.get("id")
#
#             api_one_dict[item_id].update({
#                 'adset': item
#             })
#             print(api_one_dict[item_id])
#         #
#         # print(full_list)
#
#
#         #
#         # for item in adset_insights:
#         #
#         #     if item["id"] not in api_one_dict:
#         #         continue
#         #     item_id = item.pop("id")
#         #     api_one_dict.update( {'item_id': item})
#         #     print(api_one_dict[item_id])
#         #
#         # 23851292342730354
#         # 23851284870740354
#         # 23851284842660354
#         # 23851284832070354
#         # 23851284821440354
#
#         # api_two_dict = {item.pop("id"): item for item in adset_insights}
#         # print(api_two_dict.items())
#         # print([ api_one_dict[item.pop('id').update(item)] for item in adset_insights if item.get('id') in api_one_dict])
#
#         # [api_one_dict[item.pop("id")].update(item)] for items in adset_insights if items.get("id") in api_one_dict]
#
#         # for item in adset_insights:
#         #     if item["id"] not in api_one_dict:
#         #         continue
#         #     item_id = item.pop("id")
#         #     api_one_dict[item_id].update(item)
#         #
#         # print(api_one_dict)
#
#
#         # [api_one_dict[item.pop("id")].update(item)]
#         # for item in api_two_dict if item.get("id") in api_one_dict]
#
#         # full_list = []
#         #
#         # for item in adset_insights:
#         #     for i in item:
#         #         full_list.append({i: item.get(i)})
#
#         # except:
#
#         # data = [ x for x in chain(insights,adset_insights)]
#         # print(data)
#         pass
#         return full_list
#
#
# print(get_facebook_data(event))


# a = {'23852293779910354' '23852293780030354': {
#     "campaign_id": "23852293779860354",
#     "campaign_name": "New Engagement campaign",
#     "date_start": "2023-01-01",
#     "date_stop": "2023-01-01"
# }}
# if '23852293779910354' in a:
#     print(a)
    # print(a.keys())


a = [('23852293779910354', '23852293780030354')]
print(type(a))
for i, c in enumerate(a.pop()):
    print(i, c)

