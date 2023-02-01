import sys
import requests
# import json
from credential import event
from  urllib.parse import unquote


def get_facebook_data(event):
    pubsub_massage = event['data']
    if pubsub_massage == 'facebook_cost':
        try:
            hostname = "https://graph.facebook.com"
            api_version = str(event['attributes']['api_version'])
            account_id = str(event['attributes']['account_id'])
            access_token = str(event['attributes']['access_token'])
            # app_secret = event['attributes']['app_secret']
            # app_id = event['attributes']['app_id']


            since = '2022-08-10'
            until = '2022-08-10'
            time_range = f'since:{since},until:{until}'
            insights = 'insights{date_start,date_stop,account_id,account_name,account_currency,campaign_id,campaign_name,'
                       # 'buying_type,objective,attribution_setting,adset_id,adset_name,ad_id,ad_name,clicks,impressions,reach,' \
                       # 'spend,inline_link_clicks,inline_link_click_ctr,inline_post_engagement,social_spend,outbound_clicks,' \
                       # 'outbound_clicks_ctr,optimization_goal,engagement_rate_ranking,cpm,cpc,actions,action_values,conversions,' \
                       # 'conversion_values,conversion_rate_ranking}'
            adcreatives = 'adcreatives{id,applink_treatment,body,call_to_action_type,instagram_actor_id,instagram_permalink_url,' \
                          'instagram_story_id,link_og_id,link_url,name,object_id,object_story_id,object_url,object_type,' \
                          'effective_object_story_id,product_set_id,template_url,object_story_spec,url_tags,' \
                          'link_destination_display_url}'
            targetingsentencelines = 'targetingsentencelines{id, params, targetingsentencelines}'

            ad = 'created_time,name,id'
            params_dict = {'field': {'time_range' : time_range}}

            # url = hostname + '/' + api_version + '/' + 'act_' + account_id + '/ads?fields=' + '{time_range=' + time_range +'}'
                  # + ',' + ad + ',' + insights + ',' + adcreatives + ',' + targetingsentencelines + '&' + 'access_token=' + access_token
            # print(url)
            # url = "https://graph.facebook.com/" + api_version + "/act_" + account_id + "/ads?fields=insights&time_range={since:2012-08-16,until:2012-08-16}&access_token=" + access_token

        #
        #     # url = "https://graph.facebook.com/" + api_version + "/act_" + account_id +"/ads?fields=name,account_id,currency,timezone_id&access_token=" + access_token
        #     url = "https://graph.facebook.com/" + api_version + "/act_" + account_id +"/ads?fields=insights&{time_range={since:2022-08-10,until:2022-08-10}}&access_token=" + access_token
            url = "https://graph.facebook.com/" + api_version + "/act_" + account_id + "/ads?fields=insights&level=ad&time_increment=1&time_range={since:'2023-01-01',until:'2023-01-02'}&access_token=" + access_token
        #     # act_417625609928355/ads?fields=insights{account_id,actions,ad_name,adset_name,date_start,date_stop}&{time_range=since:2022-08-10,until:2022-08-10}
            headers = {}



            # рабочее API 23852815641340354/insights?fields=ad_id,impressions&time_range={since:'2023-01-01',until:'2023-01-31'}&time_increment=1&level=ad










            r = requests.get(url=url, headers=headers)
            fb_ads_account_data = r.json()
            fb_ads_accounts = fb_ads_account_data['data']

            for fb_ads_account in fb_ads_accounts:
                print("\nfb_ads_account -> ", fb_ads_account)

        except:
            print("\nFunction (get_facebook_ads_account) Failed", sys.exc_info())


        # if __name__ == '__main__':
        #     # try:
        #     print("Facebook Ads Account extraction process Started")
        #     # reading client_id json file
        #     cred_file = "./facebook_cred.json"
        #     facebook_cred = open(cred_file, 'r')
        #     cred_json = json.load(facebook_cred)
        #
        #     access_token = cred_json["access_token"]
        #     app_id = cred_json["app_id"]
        #     app_secret = cred_json["app_secret"]
        #     api_version = cred_json["api_version"]
        #
        #     accounts_details_df = get_facebook_ads_account(access_token, api_version)

        # except:
        #     print("\nFacebook Ads Account extraction process Failed", sys.exc_info())

print(get_facebook_data(event))